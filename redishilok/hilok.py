import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from redis import asyncio as aioredis

from redishilok.rwctx import RedisRWLockCtx


class RedisHiLokError(RuntimeError):
    pass


class RedisHiLok:
    def __init__(
        self,
        redis: str | aioredis.Redis,
        *,
        ttl: int = 5000,
        refresh_interval: float = 2000,
        separator: str = "/",
        cancel_on_lock_failure: bool = True,
    ):
        if isinstance(redis, str):
            self.redis = aioredis.from_url(redis)
        else:
            self.redis = redis
        self.ttl = ttl
        self.refresh_interval = refresh_interval
        self.separator = separator
        self.cancel_on_lock_failure = cancel_on_lock_failure

    async def __aenter__(self) -> "RedisHiLok":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    async def close(self) -> None:
        await self.redis.aclose()

    def _build_lock(
        self, path: str, uuid: str, restore: bool, refresh: bool
    ) -> RedisRWLockCtx:
        return RedisRWLockCtx(
            self.redis,
            path,
            ttl=self.ttl,
            refresh_interval=self.refresh_interval if refresh else 0,
            cancel_on_lock_failure=self.cancel_on_lock_failure,
            uuid=uuid,
            restore=restore,
        )

    def _path_split(self, path: str) -> list[str]:
        return list(filter(lambda x: x, path.split(self.separator)))

    async def _acquire_hierarchy(
        self,
        path: str,
        shared_last: bool,
        block: bool,
        timeout: float | None,
        uuid: str,
        restore: bool,
        refresh: bool,
    ) -> list[RedisRWLockCtx]:
        nodes = self._path_split(path)
        locks: list[RedisRWLockCtx] = []
        try:
            for i, node in enumerate(nodes):
                lock_path = self.separator.join(nodes[: i + 1])
                lock = self._build_lock(
                    lock_path, uuid, restore=restore, refresh=refresh
                )
                if i < len(nodes) - 1:  # Ancestors: always shared
                    ok = await lock.acquire_read(block=block, timeout=timeout)
                else:  # Target node: mode depends on `shared_last`
                    if shared_last:
                        ok = await lock.acquire_read(block=block, timeout=timeout)
                    else:
                        ok = await lock.acquire_write(block=block, timeout=timeout)
                if not ok:
                    raise RedisHiLokError(f"Failed to acquire lock at {lock_path}")
                locks.append(lock)
            return locks
        except Exception:
            await self._release_hierarchy(locks, shared_last=shared_last)
            raise

    @staticmethod
    async def _release_hierarchy(
        locks: list[RedisRWLockCtx], shared_last: bool
    ) -> None:
        for i, lock in enumerate(reversed(locks)):
            try:
                if i != 0:
                    await lock.release_read()
                else:
                    if shared_last:
                        await lock.release_read()
                    else:
                        await lock.release_write()
                try:
                    # this can fail because we're stopping the refresh task
                    await lock.close()
                except asyncio.CancelledError:
                    pass
            except Exception:
                # this isn't catastrophic, but we should log it
                logging.exception("Failed to release hilok")

    async def acquire_read(
        self,
        path: str,
        *,
        block: bool = True,
        timeout: float | None = None,
        uuid: str | None = None,
    ) -> str:
        """Acquire a read lock on the given path.

        Args:
            path (str): The path to lock.
            block (bool, optional): Whether to block until the lock is acquired. Defaults to True.
            timeout (float, optional): The maximum time to wait for the lock. Defaults to None.
            uuid (str, optional): The UUID to use for the lock. Defaults to None.

        Returns:
            str: The UUID used for the lock.

        If a uuid is passed, the lock will be only acquired if the uuid matches the
        current lock holder, type and path.
        """
        restore = bool(uuid)
        uuid = uuid or os.urandom(16).hex()
        await self._acquire_hierarchy(
            path,
            shared_last=True,
            block=block,
            timeout=timeout,
            uuid=uuid,
            restore=restore,
            refresh=False,
        )
        return uuid

    async def acquire_write(
        self,
        path: str,
        *,
        block: bool = True,
        timeout: float | None = None,
        uuid: str | None = None,
    ) -> str:
        """Acquire a write lock on the given path."""
        restore = bool(uuid)
        uuid = uuid or os.urandom(16).hex()
        await self._acquire_hierarchy(
            path,
            shared_last=False,
            block=block,
            timeout=timeout,
            uuid=uuid,
            restore=restore,
            refresh=False,
        )
        return uuid

    async def release_read(self, path: str, uuid: str) -> None:
        """Release a read lock on the given path + uuid."""
        assert uuid, "UUID is required"
        # passing the uuid in means the lock is restored and then released
        async with self.read(path, uuid=uuid):
            pass

    async def release_write(self, path: str, uuid: str) -> None:
        """Release a write lock on the given path + uuid."""
        assert uuid, "UUID is required"
        # passing the uuid in means the lock is restored and then released
        async with self.write(path, uuid=uuid):
            pass

    @asynccontextmanager
    async def read(
        self,
        path: str,
        *,
        block: bool = True,
        timeout: float | None = None,
        uuid: str | None = None,
    ) -> AsyncIterator[None]:
        """Context manager for acquiring a read lock."""
        locks = await self._acquire_hierarchy(
            path,
            shared_last=True,
            block=block,
            timeout=timeout,
            uuid=uuid or os.urandom(16).hex(),
            restore=bool(uuid),
            refresh=True,
        )
        try:
            yield locks[-1].lock.uuid
        finally:
            await self._release_hierarchy(locks, True)

    @asynccontextmanager
    async def write(
        self,
        path: str,
        *,
        block: bool = True,
        timeout: float | None = None,
        uuid: str | None = None,
    ) -> AsyncIterator[None]:
        """Context manager for acquiring a write lock."""
        locks = await self._acquire_hierarchy(
            path,
            shared_last=False,
            block=block,
            timeout=timeout,
            uuid=uuid or os.urandom(16).hex(),
            restore=bool(uuid),
            refresh=True,
        )
        try:
            yield locks[-1].lock.uuid
        finally:
            await self._release_hierarchy(locks, False)

    async def status(self, path: str, uuid: str):
        """Get the status of the lock at the given path."""
        nodes = self._path_split(path)
        status = []
        for i, node in enumerate(nodes):
            lock_path = self.separator.join(nodes[: i + 1])
            lock = self._build_lock(lock_path, uuid=uuid, restore=True, refresh=False)
            status.append(await lock.lock.status())
        return status

import asyncio
from asyncio import Task
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from redis import asyncio as aioredis

from redishilok.rwlock import RedisRWLock
from redishilok.types import RedisHiLokError, RedisRWLockStatus


class RedisRWLockCtx:
    def __init__(
        self,
        redis: str | aioredis.Redis,
        path: str,
        ttl: int = 10000,
        refresh_interval: float = 3000,
        cancel_on_lock_failure: bool = True,
        uuid: str | None = None,
        restore: bool = False,
    ) -> None:
        """Context manager for acquiring Redis read/write locks.

        Args:
            redis (str): Redis connection URL or aioredis.Redis connection.
            path (str): Key for the lock.
            ttl (int, optional): Lock TTL in milliseconds. Defaults to 10000.
            refresh_interval (int, optional): Lock refresh interval in milliseconds. Defaults to 3000.
            cancel_on_lock_failure (bool, optional): Cancel the current task if lock refresh fails. Defaults to True.

        Raises:
            RuntimeError: If lock refresh fails.
        """
        self._context_task: Task[Any] | None = None
        self.cancel_on_lock_failure = cancel_on_lock_failure
        self.refresh_interval = refresh_interval
        self.lock = RedisRWLock(redis, path, ttl, uuid=uuid, restore=restore)
        self._refresh_task: Task[Any] | None = None
        self._stop_event = asyncio.Event()

    @property
    def path(self) -> str:
        return self.lock.path

    @property
    def ttl(self) -> int:
        return self.lock.ttl

    async def close(self) -> None:
        """Close the redis connection."""
        try:
            await self._stop_refresh()
        except Exception:
            pass
        await self.lock.close()

    async def _start_refresh(self, shared: bool) -> None:
        self._context_task = asyncio.current_task()

        async def refresh_loop() -> None:
            try:
                while not self._stop_event.is_set():
                    try:
                        await self.lock.refresh_lock(shared=shared)
                    except RuntimeError as e:
                        # Lock lost; raise an exception to terminate the task
                        raise RedisHiLokError(f"Refresh failed: {str(e)}") from e
                    await asyncio.sleep(self.refresh_interval / 1000)
            except Exception:
                self._stop_event.set()
                if self._context_task and self.cancel_on_lock_failure:
                    self._context_task.cancel()
                    self._context_task = None
                raise

        self._refresh_task = asyncio.create_task(refresh_loop())

    async def _stop_refresh(self) -> None:
        if self._refresh_task:
            self._stop_event.set()
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            self._context_task = None
            self._refresh_task = None
            self._stop_event.clear()

    async def acquire_read(
        self, block: bool = True, timeout: float | None = None
    ) -> bool:
        """Acquire a read lock."""
        acquired = await self.lock.acquire_read_lock(block=block, timeout=timeout)
        if acquired and self.refresh_interval > 0:
            await self._start_refresh(shared=True)
        return acquired

    async def release_read(self) -> None:
        """Release a read lock."""
        try:
            if self.refresh_interval > 0:
                await self._stop_refresh()
        except Exception:
            pass
        finally:
            await self.lock.release_read_lock()

    async def acquire_write(
        self, block: bool = True, timeout: float | None = None
    ) -> bool:
        """Acquire a write lock."""
        acquired = await self.lock.acquire_write_lock(block=block, timeout=timeout)
        if acquired and self.refresh_interval > 0:
            await self._start_refresh(shared=False)
        return acquired

    async def release_write(self) -> None:
        """Release a write lock."""
        try:
            if self.refresh_interval > 0:
                await self._stop_refresh()
        except Exception:
            pass
        finally:
            await self.lock.release_write_lock()

    @asynccontextmanager
    async def read(
        self, block: bool = True, timeout: float | None = None
    ) -> AsyncIterator[None]:
        """Context manager for acquiring a read lock."""
        try:
            got = await self.acquire_read(block=block, timeout=timeout)
            if not got:
                raise RedisHiLokError(f"Failed to acquire read lock for {self.path}")
            yield
        finally:
            await self.release_read()

    @asynccontextmanager
    async def write(
        self, block: bool = True, timeout: float | None = None
    ) -> AsyncIterator[None]:
        """Context manager for acquiring a write lock."""
        try:
            got = await self.acquire_write(block=block, timeout=timeout)
            if not got:
                raise RedisHiLokError(f"Failed to acquire write lock for {self.path}")
            yield
        finally:
            await self.release_write()

    async def status(self) -> RedisRWLockStatus:
        return await self.lock.status()

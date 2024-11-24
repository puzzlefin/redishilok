import logging
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any, AsyncIterator

from redis import asyncio as aioredis

from redishilok.rwctx import RedisRWLockCtx


class RedisHiLok:
    def __init__(
        self,
        redis: str | aioredis.Redis,
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

    def _build_lock(self, path: str) -> RedisRWLockCtx:
        return RedisRWLockCtx(
            self.redis,
            path,
            ttl=self.ttl,
            refresh_interval=self.refresh_interval,
            cancel_on_lock_failure=self.cancel_on_lock_failure,
        )

    async def _acquire_hierarchy(
        self, path: str, shared_last: bool, block: bool, timeout: float | None
    ) -> list[tuple[RedisRWLockCtx, AbstractAsyncContextManager[None]]]:
        nodes = list(filter(lambda x: x, path.split(self.separator)))
        locks: list[tuple[RedisRWLockCtx, AbstractAsyncContextManager[None]]] = []
        try:
            for i, node in enumerate(nodes):
                lock_path = self.separator.join(nodes[: i + 1])
                lock = self._build_lock(lock_path)
                if i < len(nodes) - 1:  # Ancestors: always shared
                    lock_ctx = lock.read(block=block, timeout=timeout)
                else:  # Target node: mode depends on `shared_last`
                    if shared_last:
                        lock_ctx = lock.read(block=block, timeout=timeout)
                    else:
                        lock_ctx = lock.write(block=block, timeout=timeout)
                await lock_ctx.__aenter__()
                locks.append((lock, lock_ctx))
            return locks
        except Exception:
            await self._release_hierarchy(locks)
            raise

    @staticmethod
    async def _release_hierarchy(
        locks: list[tuple[RedisRWLockCtx, AbstractAsyncContextManager[None]]],
    ) -> None:
        for i, (lock, ctx) in enumerate(reversed(locks)):
            try:
                await ctx.__aexit__(None, None, None)
                await lock.close()
            except Exception:
                # this isn't catastrophic, but we should log it
                logging.exception("Failed to release hilok")

    @asynccontextmanager
    async def read(
        self, path: str, block: bool = True, timeout: float | None = None
    ) -> AsyncIterator[None]:
        locks = await self._acquire_hierarchy(
            path, shared_last=True, block=block, timeout=timeout
        )
        try:
            yield
        finally:
            await self._release_hierarchy(locks)

    @asynccontextmanager
    async def write(
        self, path: str, block: bool = True, timeout: float | None = None
    ) -> AsyncIterator[None]:
        locks = await self._acquire_hierarchy(
            path, shared_last=False, block=block, timeout=timeout
        )
        try:
            yield
        finally:
            await self._release_hierarchy(locks)

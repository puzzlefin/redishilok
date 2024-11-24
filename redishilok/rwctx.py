import asyncio
from contextlib import AbstractAsyncContextManager, asynccontextmanager

from redis import asyncio as aioredis

from redishilok.rwlock import RedisRWLock


class RedisRWLockCtx:
    def __init__(
        self,
        redis: str | aioredis.Redis,
        lock_key,
        ttl=10000,
        refresh_interval=3000,
        cancel_on_lock_failure=True,
    ):
        """Context manager for acquiring Redis read/write locks.

        Args:
            redis (str): Redis connection URL or aioredis.Redis connection.
            lock_key (str): Key for the lock.
            ttl (int, optional): Lock TTL in milliseconds. Defaults to 10000.
            refresh_interval (int, optional): Lock refresh interval in milliseconds. Defaults to 3000.
            cancel_on_lock_failure (bool, optional): Cancel the current task if lock refresh fails. Defaults to True.

        Raises:
            RuntimeError: If lock refresh fails.
        """
        self._context_task = None
        self.cancel_on_lock_failure = cancel_on_lock_failure
        self.refresh_interval = refresh_interval
        self.lock = RedisRWLock(redis, lock_key, ttl)
        self._refresh_task = None
        self._stop_event = asyncio.Event()

    @property
    def path(self):
        return self.lock.path

    @property
    def ttl(self):
        return self.lock.ttl

    async def close(self):
        """Close the redis connection."""
        await self._stop_refresh()
        await self.lock.close()

    async def _start_refresh(self, shared: bool):
        async def refresh_loop():
            try:
                while not self._stop_event.is_set():
                    try:
                        await self.lock.refresh_lock(shared=shared)
                    except RuntimeError as e:
                        # Lock lost; raise an exception to terminate the task
                        raise RuntimeError(f"Refresh failed: {str(e)}") from e
                    await asyncio.sleep(self.refresh_interval / 1000)
            except Exception:
                self._stop_event.set()
                if self._context_task and self.cancel_on_lock_failure:
                    self._context_task.cancel()
                raise

        self._refresh_task = asyncio.create_task(refresh_loop())

    async def _stop_refresh(self):
        if self._refresh_task:
            self._stop_event.set()
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            self._refresh_task = None
            self._stop_event.clear()

    @asynccontextmanager
    async def read(
        self, block=True, timeout: int | None = None
    ) -> AbstractAsyncContextManager[None]:
        """Context manager for acquiring a read lock."""
        acquired = await self.lock.acquire_read_lock(block=block, timeout=timeout)
        if not acquired:
            raise RuntimeError(f"Failed to acquire read lock for {self.path}")
        try:
            await self._start_refresh(shared=True)
            yield
        finally:
            try:
                await self._stop_refresh()
            finally:
                await self.lock.release_read_lock()

    @asynccontextmanager
    async def write(
        self, block=True, timeout=None
    ) -> AbstractAsyncContextManager[None]:
        """Context manager for acquiring a write lock."""
        acquired = await self.lock.acquire_write_lock(block=block, timeout=timeout)
        if not acquired:
            raise RuntimeError(f"Failed to acquire write lock for {self.path}")
        try:
            self._context_task = asyncio.current_task()
            await self._start_refresh(shared=False)
            yield
        finally:
            try:
                await self._stop_refresh()
            finally:
                await self.lock.release_write_lock()

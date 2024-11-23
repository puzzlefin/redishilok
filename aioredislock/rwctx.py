import asyncio
from contextlib import asynccontextmanager

from aioredislock.rwlock import RedisRWLock


class RedisRWLockCtx:
    def __init__(
        self,
        redis_url,
        lock_key,
        ttl=10000,
        refresh_interval=3000,
        cancel_on_lock_failure=True,
    ):
        self._context_task = None
        self.redis_url = redis_url
        self.lock_key = lock_key
        self.ttl = ttl
        self.cancel_on_lock_failure = cancel_on_lock_failure
        self.refresh_interval = refresh_interval
        self.lock = RedisRWLock(redis_url, lock_key, ttl)
        self._refresh_task = None
        self._stop_event = asyncio.Event()

    async def _start_refresh(self, shared):
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
    async def read(self):
        """Context manager for acquiring a read lock."""
        acquired = await self.lock.acquire_read_lock(block=True)
        if not acquired:
            raise RuntimeError(f"Failed to acquire read lock for {self.lock_key}")
        try:
            await self._start_refresh(shared=True)
            yield
        except RuntimeError:
            # Cleanup if refresh fails
            await self._stop_refresh()
            await self.lock.release_read_lock()
            raise
        finally:
            await self._stop_refresh()
            await self.lock.release_read_lock()

    @asynccontextmanager
    async def write(self):
        """Context manager for acquiring a write lock."""
        acquired = await self.lock.acquire_write_lock(block=True)
        if not acquired:
            raise RuntimeError(f"Failed to acquire write lock for {self.lock_key}")
        try:
            self._context_task = asyncio.current_task()
            await self._start_refresh(shared=False)
            yield
        except RuntimeError:
            # Cleanup if refresh fails
            await self._stop_refresh()
            await self.lock.release_write_lock()
            raise
        finally:
            await self._stop_refresh()
            await self.lock.release_write_lock()

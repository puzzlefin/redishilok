from contextlib import asynccontextmanager

from redishilok.rwctx import RedisRWLockCtx


class RedisHiLok:
    def __init__(
        self,
        redis_url: str,
        ttl=5000,
        refresh_interval=2000,
        separator="/",
        cancel_on_lock_failure=True,
    ):
        self.redis_url = redis_url
        self.ttl = ttl
        self.refresh_interval = refresh_interval
        self.separator = separator
        self.cancel_on_lock_failure = cancel_on_lock_failure

    def _build_lock(self, path):
        return RedisRWLockCtx(
            self.redis_url,
            path,
            ttl=self.ttl,
            refresh_interval=self.refresh_interval,
            cancel_on_lock_failure=self.cancel_on_lock_failure,
        )

    async def _acquire_hierarchy(self, path, shared_last, block, timeout):
        nodes = path.split(self.separator)
        locks = []
        try:
            for i, node in enumerate(nodes):
                lock_path = self.separator.join(nodes[: i + 1])
                lock = self._build_lock(lock_path)
                if i < len(nodes) - 1:  # Ancestors: always shared
                    await lock.read(block=block, timeout=timeout).__aenter__()
                else:  # Target node: mode depends on `shared_last`
                    if shared_last:
                        await lock.read(block=block, timeout=timeout).__aenter__()
                    else:
                        await lock.write(block=block, timeout=timeout).__aenter__()
                locks.append(lock)
            return locks
        except:
            # Release locks if acquisition fails
            await self._release_hierarchy(locks, shared_last)
            raise

    async def _release_hierarchy(self, locks, shared_last=True):
        for i, lock in enumerate(reversed(locks)):
            if i == 0 and not shared_last:  # Last node: release according to its mode
                await lock.write().__aexit__(None, None, None)
            else:  # Ancestors: always shared
                await lock.read().__aexit__(None, None, None)

    @asynccontextmanager
    async def read(self, path, block=True, timeout=None):
        locks = await self._acquire_hierarchy(
            path, shared_last=True, block=block, timeout=timeout
        )
        try:
            yield
        finally:
            await self._release_hierarchy(locks, shared_last=True)

    @asynccontextmanager
    async def write(self, path, block=True, timeout=None):
        locks = await self._acquire_hierarchy(
            path, shared_last=False, block=block, timeout=timeout
        )
        try:
            yield
        finally:
            await self._release_hierarchy(locks, shared_last=False)

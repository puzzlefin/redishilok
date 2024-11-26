import asyncio
import dataclasses
import os

from redis import asyncio as aioredis


@dataclasses.dataclass
class RedisRWLockStatus:
    held: bool
    type: str | None
    owned: bool
    ttl: int | None


class RedisRWLockError(RuntimeError):
    pass


class RedisRWLock:
    _redis: aioredis.Redis

    def __init__(
        self,
        redis_url_or_redis_conn: str | aioredis.Redis,
        path: str,
        ttl: int,
        uuid: str | None = None,
        restore=False,
    ):
        self._redis_param = redis_url_or_redis_conn
        self.path = path
        self.ttl = ttl
        assert not uuid or isinstance(uuid, str)
        self.uuid = uuid or os.urandom(16).hex()
        self.restore = restore
        self.held = False

    @property
    def redis(self) -> aioredis.Redis:
        if not hasattr(self, "_redis"):
            if isinstance(self._redis_param, str):
                self._redis = aioredis.from_url(self._redis_param)
            elif isinstance(self._redis_param, aioredis.Redis):
                self._redis = self._redis_param
            else:  # pragma: no cover
                raise ValueError(
                    "redis_url_or_redis_conn must be a string or aioredis.Redis instance."
                )
        return self._redis

    async def close(self) -> None:
        if self._redis is not None:
            await self.redis.aclose()

    async def acquire_read_lock(
        self, block: bool = True, timeout: float | None = None
    ) -> bool:
        if self.restore:
            try:
                await self.refresh_lock(True)
                self.held = True
                return True
            except RedisRWLockError:
                return False

        script = """
        if redis.call("HGET", KEYS[1], "writer") ~= false then
            return false
        end
        redis.call("LPUSH", KEYS[2], ARGV[1])
        redis.call("PEXPIRE", KEYS[2], ARGV[2])
        return true
        """
        readers_key = f"{self.path}:readers"
        while True:
            acquired = await self.redis.eval(  # type: ignore[misc]
                script, 2, self.path, readers_key, self.uuid, self.ttl
            )
            if acquired or not block:
                self.held = acquired
                return bool(acquired)
            if timeout is not None:
                timeout -= 0.1
                if timeout <= 0:
                    return False
            await asyncio.sleep(0.05)

    async def acquire_write_lock(
        self, block: bool = True, timeout: float | None = None
    ) -> bool:
        if self.restore:
            try:
                await self.refresh_lock(False)
                if not self.held:
                    raise RedisRWLockError("Restore failed")
                return True
            except RedisRWLockError:
                return False

        script = """
        if redis.call("LLEN", KEYS[2]) > 0 then
            return false
        end
        if redis.call("HGET", KEYS[1], "writer") ~= false then
            return false
        end
        redis.call("HSET", KEYS[1], "writer", ARGV[1])
        redis.call("PEXPIRE", KEYS[1], ARGV[2])
        return true
        """
        readers_key = f"{self.path}:readers"
        while True:
            acquired = await self.redis.eval(  # type: ignore[misc]
                script, 2, self.path, readers_key, self.uuid, self.ttl
            )
            if acquired or not block:
                self.held = acquired
                return bool(acquired)
            if timeout is not None:
                timeout -= 0.1
                if timeout <= 0:
                    return False
            await asyncio.sleep(0.4)

    async def refresh_lock(self, shared: bool = True) -> None:
        script = """
        if ARGV[1] == "shared" then
            if redis.call("LPOS", KEYS[2], ARGV[2]) then
                redis.call("PEXPIRE", KEYS[1], ARGV[3])
                redis.call("PEXPIRE", KEYS[2], ARGV[3])
            else
                return false
            end
        else
            if redis.call("HGET", KEYS[1], "writer") ~= ARGV[2] then
                return false
            end
            redis.call("PEXPIRE", KEYS[1], ARGV[3])
        end
        return true
        """

        readers_key = f"{self.path}:readers"
        lock_type = "shared" if shared else "exclusive"
        refreshed = await self.redis.eval(  # type: ignore[misc]
            script, 2, self.path, readers_key, lock_type, self.uuid, str(self.ttl)
        )
        self.held = bool(refreshed)
        if not refreshed:
            raise RedisRWLockError(
                "Failed to refresh lock: Lock does not exist or is not held."
            )

    async def release_read_lock(self) -> None:
        script = """
        redis.call("LREM", KEYS[1], 1, ARGV[1])
        """
        # get human readable stack trace
        readers_key = f"{self.path}:readers"
        try:
            return await self.redis.eval(script, 1, readers_key, self.uuid)  # type: ignore[misc]
        finally:
            self.restore = False
            self.held = False

    async def release_write_lock(self) -> None:
        script = """
        redis.call("HDEL", KEYS[1], "writer")
        """
        try:
            return await self.redis.eval(script, 1, self.path, self.uuid)  # type: ignore[misc]
        finally:
            self.restore = False
            self.held = False

    async def status(self) -> RedisRWLockStatus:
        """Query the status of the lock."""
        readers_key = f"{self.path}:readers"

        script = """
        local writer = redis.call("HGET", KEYS[1], "writer")
        local readers = redis.call("LPOS", KEYS[2], ARGV[1])
        local all = redis.call("LRANGE", KEYS[2], 0, -1)
        local ttl = redis.call("PTTL", KEYS[1])
        if writer then
            if writer == ARGV[1] then
                return {"write", true, ttl, all}
            else
                return {"write", false, ttl, all}
            end
        elseif readers then
            return {"read", true, readers, all}
        elseif redis.call("LLEN", KEYS[2]) > 0 then
            return {"read", false, ttl, all}
        else
            return {"", false, -1, all}
        end
        """
        result = await self.redis.eval(script, 2, self.path, readers_key, self.uuid)  # type: ignore[misc]

        lock_type, owned, ttl, rkeys = result
        return RedisRWLockStatus(
            held=bool(lock_type),
            type=lock_type if lock_type else None,
            owned=bool(owned),
            ttl=max(ttl, 0) if ttl and ttl >= 0 else None,
        )

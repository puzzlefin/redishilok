import asyncio
import os

from redis import asyncio as aioredis


class RedisRWLock:
    _redis: aioredis.Redis

    def __init__(
        self, redis_url_or_redis_conn: str | aioredis.Redis, path: str, ttl: int
    ):
        self._redis_param = redis_url_or_redis_conn
        self.path = path
        self.ttl = ttl
        self.uuid = os.urandom(16).hex()

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
        script = """
        if redis.call("HGET", KEYS[1], "writer") ~= false then
            return false
        end
        redis.call("LPUSH", KEYS[2], ARGV[1])
        redis.call("PEXPIRE", KEYS[1], ARGV[2])
        redis.call("PEXPIRE", KEYS[2], ARGV[2])
        return true
        """
        readers_key = f"{self.path}:readers"
        while True:
            acquired = await self.redis.eval(  # type: ignore[misc]
                script, 2, self.path, readers_key, self.uuid, str(self.ttl)
            )
            if acquired or not block:
                return bool(acquired)
            if timeout is not None:
                timeout -= 0.1
                if timeout <= 0:
                    return False
            await asyncio.sleep(0.05)

    async def acquire_write_lock(
        self, block: bool = True, timeout: float | None = None
    ) -> bool:
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
                script, 2, self.path, readers_key, self.uuid, str(self.ttl)
            )
            if acquired or not block:
                return bool(acquired)
            if timeout is not None:
                timeout -= 0.1
                if timeout <= 0:
                    return False
            await asyncio.sleep(0.1)

    async def refresh_lock(self, shared: bool = True) -> None:
        script = """
        if ARGV[1] == "shared" then
            if redis.call("LPOS", KEYS[2], ARGV[2]) == false then
                return false
            end
            redis.call("PEXPIRE", KEYS[1], ARGV[3])
            redis.call("PEXPIRE", KEYS[2], ARGV[3])
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
        if not refreshed:
            raise RuntimeError(
                "Failed to refresh lock: Lock does not exist or is not held."
            )

    async def release_read_lock(self) -> bool:
        script = """
        local pos = redis.call("LPOS", KEYS[1], ARGV[1])
        if pos ~= false then
            redis.call("LREM", KEYS[1], 1, ARGV[1])
        end
        return true
        """
        readers_key = f"{self.path}:readers"
        return bool(await self.redis.eval(script, 1, readers_key, self.uuid))  # type: ignore[misc]

    async def release_write_lock(self) -> bool:
        script = """
        if redis.call("HGET", KEYS[1], "writer") == ARGV[1] then
            redis.call("HDEL", KEYS[1], "writer")
            return true
        end
        return false
        """
        return bool(await self.redis.eval(script, 1, self.path, self.uuid))  # type: ignore[misc]

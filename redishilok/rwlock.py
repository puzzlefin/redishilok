import asyncio
import os

from redis import asyncio as aioredis


class RedisRWLock:
    _redis: aioredis.Redis | None

    def __init__(
        self, redis_url_or_redis_conn: str | aioredis.Redis, path: str, ttl: int
    ):
        self._redis_param = redis_url_or_redis_conn
        self.path = path
        self.ttl = ttl
        self.uuid = os.urandom(16).hex()

    @property
    def redis(self):
        if not hasattr(self, "_redis"):
            if isinstance(self._redis_param, str):
                self._redis = aioredis.from_url(self._redis_param)
            else:
                self._redis = self._redis_param
        return self._redis

    async def close(self):
        if self._redis is not None:
            await self.redis.aclose()

    async def acquire_read_lock(self, block=True, timeout: int | None = None):
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
            acquired = await self.redis.eval(
                script, 2, self.path, readers_key, self.uuid, self.ttl
            )
            if acquired or not block:
                return acquired
            if timeout is not None:
                timeout -= 0.1
                if timeout <= 0:
                    return False
            await asyncio.sleep(0.05)

    async def acquire_write_lock(self, block=True, timeout=None):
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
            acquired = await self.redis.eval(
                script, 2, self.path, readers_key, self.uuid, self.ttl
            )
            if acquired or not block:
                return acquired
            if timeout is not None:
                timeout -= 0.1
                if timeout <= 0:
                    return False
            await asyncio.sleep(0.1)

    async def refresh_lock(self, shared=True):
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
        refreshed = await self.redis.eval(
            script, 2, self.path, readers_key, lock_type, self.uuid, self.ttl
        )
        if not refreshed:
            raise RuntimeError(
                "Failed to refresh lock: Lock does not exist or is not held."
            )

    async def release_read_lock(self):
        script = """
        local pos = redis.call("LPOS", KEYS[1], ARGV[1])
        if pos ~= false then
            redis.call("LREM", KEYS[1], 1, ARGV[1])
        end
        return true
        """
        readers_key = f"{self.path}:readers"
        return await self.redis.eval(script, 1, readers_key, self.uuid)

    async def release_write_lock(self):
        script = """
        if redis.call("HGET", KEYS[1], "writer") == ARGV[1] then
            redis.call("HDEL", KEYS[1], "writer")
            return true
        end
        return false
        """
        return await self.redis.eval(script, 1, self.path, self.uuid)

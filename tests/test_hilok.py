import asyncio

import pytest
from redis import asyncio as aioredis

from redishilok.hilok import RedisHiLok


@pytest.fixture
async def redis_client():
    redis_url = "redis://localhost:6379/0"
    redis = aioredis.from_url(redis_url)
    await redis.flushdb()  # Ensure a clean slate
    yield redis
    await redis.flushdb()
    await redis.aclose()


async def test_write_lock_same_level(redis_client):
    h = RedisHiLok("redis://localhost")

    async with h.write("/a/b"):
        pass

    async with h.write("/a/b"):
        with pytest.raises(RuntimeError):
            async with h.write("/a/b", block=False):
                pass

    await h.close()


async def test_nested_write_locks(redis_client):
    h = RedisHiLok("redis://localhost")

    async with h.write("/a/b"):
        with pytest.raises(RuntimeError):
            async with h.write("/a/b/c", block=False):
                pass

    # all is forgiven
    async with h.write("/a/b", block=False):
        pass

    await h.close()


@pytest.mark.asyncio
async def test_write_lock_on_ancestor_blocks_descendent(redis_client):
    async with RedisHiLok("redis://localhost") as h:
        async with h.read("/a/b/c/d/e"):
            with pytest.raises(RuntimeError):
                async with h.write("/a/b", timeout=0.1):
                    pass


async def test_nested_read_and_write(redis_client):
    async with RedisHiLok("redis://localhost") as h:
        async with h.read("/a/b"):
            # ok to read again
            async with h.read("/a/b", block=False):
                pass
            with pytest.raises(RuntimeError):
                async with h.write("/a/b", block=False):
                    pass
        async with h.write("/a/b", block=False):
            pass


async def test_using_other_sep(redis_client):
    h = RedisHiLok("redis://localhost", separator=":")
    async with h.read("a:b"):
        with pytest.raises(RuntimeError):
            async with h.write("a", block=False):
                pass


async def test_read_lock_allows_write_descendant(redis_client):
    async with RedisHiLok("redis://localhost") as h:
        async with h.read("/a"):
            async with h.write("/a/b/c"):
                pass


async def test_write_lock_blocks_descendant(redis_client):
    async with RedisHiLok("redis://localhost") as h:
        async with h.write("/a"):
            with pytest.raises(RuntimeError):
                async with h.read("/a/b/c", timeout=0.1):
                    pass


async def test_concurrent_reads(redis_client):
    async with RedisHiLok("redis://localhost") as h:

        async def read_task():
            async with h.read("/a/b/c"):
                await asyncio.sleep(0.1)

        # Run multiple readers concurrently
        tasks = [asyncio.create_task(read_task()) for _ in range(5)]
        await asyncio.gather(*tasks)


async def test_hierarchical_write_conflicts(redis_client):
    async with RedisHiLok("redis://localhost") as h:
        async with h.write("/a/b"):
            with pytest.raises(RuntimeError):
                async with h.write("/a/b/c", timeout=0.1):
                    pass


async def test_lock_timeout(redis_client):
    async with RedisHiLok("redis://localhost") as h:
        async with h.write("/a/b/c"):
            with pytest.raises(RuntimeError):
                async with h.write("/a/b/c", timeout=0.1):
                    pass


async def test_lock_refresh(redis_client):
    async with RedisHiLok("redis://localhost", ttl=500, refresh_interval=200) as h:
        async with h.write("/a/b/c"):
            await asyncio.sleep(1)  # exceeds the original TTL, but refresh catches it!
            with pytest.raises(RuntimeError):
                async with h.read("/a/b/c", block=False):
                    pass

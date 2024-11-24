import asyncio
import os

import pytest

from redishilok.hilok import RedisHiLok

redis_host = os.environ.get("REDIS_URL", "redis://localhost")


async def test_write_lock_same_level():
    h = RedisHiLok(redis_host)

    async with h.write("/a/b"):
        pass

    async with h.write("/a/b"):
        with pytest.raises(RuntimeError):
            async with h.write("/a/b", block=False):
                pass

    await h.close()


async def test_nested_write_locks():
    h = RedisHiLok(redis_host)

    async with h.write("/a/b"):
        with pytest.raises(RuntimeError):
            async with h.write("/a/b/c", block=False):
                pass

    # all is forgiven
    async with h.write("/a/b", block=False):
        pass

    await h.close()


@pytest.mark.asyncio
async def test_write_lock_on_ancestor_blocks_descendent():
    async with RedisHiLok(redis_host) as h:
        async with h.read("/mmm/b/c/d/e"):
            with pytest.raises(RuntimeError):
                async with h.write("/mmm/b", timeout=0.1):
                    pass


async def test_nested_read_and_write():
    async with RedisHiLok(redis_host) as h:
        async with h.read("/rrr/b"):
            # ok to read again
            async with h.read("/rrr/b", block=False):
                pass
            with pytest.raises(RuntimeError):
                async with h.write("/rrr/b", block=False):
                    pass
        async with h.write("/rrr/b", block=False):
            pass


async def test_using_other_sep():
    h = RedisHiLok(redis_host, separator=":")
    async with h.read("qq:b"):
        with pytest.raises(RuntimeError):
            async with h.write("qq", block=False):
                pass


async def test_read_lock_allows_write_descendant():
    async with RedisHiLok(redis_host) as h:
        async with h.read("/ll"):
            async with h.write("/ll/b/c"):
                pass


async def test_write_lock_blocks_descendant():
    async with RedisHiLok(redis_host) as h:
        async with h.write("/mm"):
            with pytest.raises(RuntimeError):
                async with h.read("/mm/b/c", timeout=0.1):
                    pass


async def test_concurrent_reads():
    async with RedisHiLok(redis_host) as h:

        async def read_task():
            async with h.read("/ww/b/c"):
                await asyncio.sleep(0.1)

        # Run multiple readers concurrently
        tasks = [asyncio.create_task(read_task()) for _ in range(5)]
        await asyncio.gather(*tasks)


async def test_hierarchical_write_conflicts():
    async with RedisHiLok(redis_host) as h:
        async with h.write("/tt/b"):
            with pytest.raises(RuntimeError):
                async with h.write("/tt/b/c", timeout=0.1):
                    pass


async def test_lock_timeout():
    async with RedisHiLok(redis_host) as h:
        async with h.write("/a/b/c"):
            with pytest.raises(RuntimeError):
                async with h.write("/a/b/c", timeout=0.1):
                    pass


async def test_lock_refresh():
    async with RedisHiLok(redis_host, ttl=500, refresh_interval=200) as h:
        async with h.write("/zzz/b/c"):
            await asyncio.sleep(1)  # exceeds the original TTL, but refresh catches it!
            with pytest.raises(RuntimeError):
                async with h.read("/zzz/b/c", block=False):
                    pass

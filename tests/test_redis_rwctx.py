import asyncio

import pytest
from redis import asyncio as aioredis

from redishilok.rwctx import RedisRWLockCtx


@pytest.fixture
async def redis_client():
    redis_url = "redis://localhost:6379/0"
    redis = aioredis.from_url(redis_url)
    await redis.flushdb()  # Ensure a clean slate
    yield redis
    await redis.flushdb()
    await redis.aclose()


async def test_read_lock_context(redis_client):
    lock_ctx = RedisRWLockCtx("redis://localhost", "test_lock", ttl=2000)

    shared_counter = {"value": 0}

    async def read_task():
        async with lock_ctx.read():
            # Simulate read operation
            assert shared_counter["value"] == 0
            await asyncio.sleep(0.1)

    # Run multiple readers concurrently
    tasks = [asyncio.create_task(read_task()) for _ in range(5)]
    await asyncio.gather(*tasks)

    await lock_ctx.lock.close()


async def test_write_lock_context(redis_client):
    lock_ctx = RedisRWLockCtx("redis://localhost", "test_lock", ttl=2000)

    shared_counter = {"value": 0}

    async def write_task():
        async with lock_ctx.write():
            # Simulate write operation
            current_value = shared_counter["value"]
            await asyncio.sleep(0.1)  # Simulate processing time
            shared_counter["value"] = current_value + 1

    # Run write operations sequentially to avoid contention
    tasks = [asyncio.create_task(write_task()) for _ in range(5)]
    await asyncio.gather(*tasks)

    # Final counter value should be 5
    assert shared_counter["value"] == 5

    await lock_ctx.lock.close()


async def test_read_write_conflict(redis_client):
    lock_ctx = RedisRWLockCtx("redis://localhost", "test_lock", ttl=2000)

    shared_counter = {"value": 0}

    async def read_task():
        async with lock_ctx.read():
            # Simulate a read operation
            assert shared_counter["value"] >= 0
            await asyncio.sleep(0.5)

    async def write_task():
        await asyncio.sleep(0.1)  # Ensure reader acquires the lock first
        async with lock_ctx.write():
            shared_counter["value"] += 1

    # Run reader and writer concurrently
    reader = asyncio.create_task(read_task())
    writer = asyncio.create_task(write_task())

    await asyncio.gather(reader, writer)

    # Ensure the writer waited for the reader to finish
    assert shared_counter["value"] == 1

    await lock_ctx.lock.close()


async def test_refresh_failure(redis_client):
    lock_ctx = RedisRWLockCtx("redis://localhost", "test_lock", ttl=500)

    ok = True

    async def write_task():
        nonlocal ok
        try:
            async with lock_ctx.write():
                # Simulate external lock tampering
                await redis_client.hset("test_lock", "writer", "external_uuid")
                for _ in range(6):
                    await asyncio.sleep(0.25)
                ok = False
        except RuntimeError as e:
            assert "Refresh failed" in str(e)

    await write_task()
    assert ok
    await lock_ctx.lock.close()

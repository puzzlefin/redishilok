import asyncio
import threading

import pytest
from redis import asyncio as aioredis

from redishilok.rwlock import RedisRWLock


@pytest.fixture
async def redis_client():
    redis_url = "redis://localhost:6379/0"
    redis = aioredis.from_url(redis_url)
    await redis.flushdb()  # Ensure a clean slate
    yield redis
    await redis.flushdb()
    await redis.aclose()


async def test_read_lock(redis_client):
    lock = RedisRWLock("redis://localhost", "test_lock", ttl=2000)

    assert await lock.acquire_read_lock()

    readers = await redis_client.lrange("test_lock:readers", 0, -1)
    assert lock.uuid.encode() in readers

    await lock.refresh_lock(shared=True)

    readers = await redis_client.lrange("test_lock:readers", 0, -1)
    assert lock.uuid.encode() in readers

    assert await lock.release_read_lock()

    readers = await redis_client.lrange("test_lock:readers", 0, -1)
    assert lock.uuid.encode() not in readers
    await lock.close()


async def test_write_lock(redis_client):
    lock = RedisRWLock("redis://localhost", "test_lock", ttl=2000)

    assert await lock.acquire_write_lock()

    writer = await redis_client.hget("test_lock", "writer")
    assert writer.decode() == lock.uuid

    await lock.refresh_lock(shared=False)

    assert await lock.release_write_lock()

    writer = await redis_client.hget("test_lock", "writer")
    assert writer is None

    await lock.close()


async def test_read_write_conflict(redis_client):
    lock1 = RedisRWLock("redis://localhost", "test_lock", ttl=2000)
    lock2 = RedisRWLock("redis://localhost", "test_lock", ttl=2000)

    assert await lock1.acquire_read_lock()

    assert not await lock2.acquire_write_lock(block=False)

    assert await lock1.release_read_lock()

    assert await lock2.acquire_write_lock()

    await lock1.close()
    await lock2.close()


async def test_refresh_failure(redis_client):
    lock = RedisRWLock("redis://localhost", "test_lock", ttl=2000)

    assert await lock.acquire_write_lock()

    await redis_client.hset("test_lock", "writer", "external_uuid")

    with pytest.raises(RuntimeError, match="Lock does not exist or is not held"):
        await lock.refresh_lock(shared=False)
    await lock.close()


async def test_lock_expiry(redis_client):
    lock = RedisRWLock("redis://localhost", "test_lock", ttl=500)

    assert await lock.acquire_write_lock()

    await asyncio.sleep(0.6)

    writer = await redis_client.hget("test_lock", "writer")
    assert writer is None
    await lock.close()


async def test_threaded_increment(redis_client):
    shared_counter = {"value": 0}  # Shared resource

    def increment():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        lock = RedisRWLock(
            "redis://localhost", "test_lock", ttl=2000
        )  # Independent lock per thread
        loop.run_until_complete(increment_task(lock, shared_counter))
        loop.run_until_complete(lock.close())
        loop.close()

    async def increment_task(lock, counter):
        for _ in range(50):
            assert await lock.acquire_write_lock()
            try:
                counter["value"] = counter["value"] + 1
            finally:
                assert await lock.release_write_lock()

    threads = [threading.Thread(target=increment) for _ in range(10)]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    assert shared_counter["value"] == 500

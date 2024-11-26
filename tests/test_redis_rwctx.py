import asyncio
import os

import pytest

from redishilok import RedisRWLock
from redishilok.rwctx import RedisRWLockCtx

redis_host = os.environ.get("REDIS_URL", "redis://localhost")


async def test_read_lock_context():
    lock_ctx = RedisRWLockCtx(redis_host, "test_lock", ttl=2000)

    shared_counter = {"value": 0}

    async def read_task():
        async with lock_ctx.read():
            # Simulate read operation
            assert shared_counter["value"] == 0
            await asyncio.sleep(0.1)

    # Run multiple readers concurrently
    tasks = [asyncio.create_task(read_task()) for _ in range(5)]
    await asyncio.gather(*tasks)

    await lock_ctx.close()


async def test_write_lock_context():
    lock_ctx = RedisRWLockCtx(redis_host, "test_lock", ttl=2000)

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

    await lock_ctx.close()


async def test_read_write_conflict():
    lock_ctx = RedisRWLockCtx(redis_host, "test_lock", ttl=2000)

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

    await lock_ctx.close()


async def test_refresh_failure():
    lock_ctx = RedisRWLockCtx(redis_host, "test_lock", ttl=500)

    ok = True

    async def write_task():
        nonlocal ok
        try:
            async with lock_ctx.write():
                # Simulate external lock tampering
                await lock_ctx.lock.redis.hset("test_lock", "writer", "external_uuid")
                for _ in range(6):
                    await asyncio.sleep(0.25)
                ok = False
        except RuntimeError as e:
            assert "Refresh failed" in str(e)

    await write_task()
    assert ok
    await lock_ctx.close()


async def test_read_lock_restore_ctx():
    lock = RedisRWLock(redis_host, "test_restore_read_ctx", ttl=2000)
    await lock.acquire_read_lock()
    uuid = lock.uuid

    lock_ctx = RedisRWLockCtx(
        redis_host,
        "test_restore_read_ctx",
        ttl=200,
        uuid=uuid,
        restore=True,
        refresh_interval=100,
    )
    write_ctx = RedisRWLockCtx(redis_host, "test_restore_read_ctx")

    # locked
    with pytest.raises(RuntimeError):
        async with write_ctx.write(block=False):
            pass

    async with lock_ctx.read():
        # still locked
        with pytest.raises(RuntimeError):
            async with write_ctx.write(block=False):
                pass
        # ttl is 200, so it should be expired, except if it is refreshed
        await asyncio.sleep(0.25)
        # refresh works after restore
        with pytest.raises(RuntimeError):
            async with write_ctx.write(block=False):
                pass

    # dropping out of restored read lock releases it
    async with write_ctx.write(block=False):
        pass

    await lock.close()
    await lock_ctx.close()
    await write_ctx.close()


async def test_get_status():
    h = RedisRWLockCtx(redis_host, "zzzgg", ttl=9999, refresh_interval=0)

    async with h.write():
        status = await h.status()
        assert status.owned
        assert status.owned

    async with h.write():
        status = await RedisRWLock(h.lock.redis, "zzzgg", 9999).status()
        assert status.held
        assert status.held
        assert not status.owned
        assert not status.owned

    await h.close()

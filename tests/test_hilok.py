import asyncio
import os

import pytest

from redishilok.hilok import RedisHiLok

redis_host = os.environ.get("REDIS_URL", "redis://localhost")


async def test_write_lock_same_level():
    h = RedisHiLok(redis_host, ttl=500000, refresh_interval=0)
    path = "/" + os.urandom(8).hex() + "a/b"

    async with h.write(path) as uuid:
        status = await h.status(path, uuid)
        assert status[0].owned
        assert status[1].owned
        assert isinstance(uuid, str)
        pass

    async with h.write(path):
        stats = await h.status(path, "mismatch")
        assert stats[0].held
        assert stats[1].held
        with pytest.raises(RuntimeError):
            async with h.write(path, block=False):
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


async def test_hierarchical_read_lock_restore():
    lock_system = RedisHiLok(redis_host, ttl=20000, refresh_interval=0)

    path = os.urandom(8).hex() + "_hierarchical/restore/read/"
    uuid = await lock_system.acquire_read(path)

    status = await lock_system.status(path, uuid)

    assert status[0].owned
    assert status[-1].owned

    restored_uuid = await lock_system.acquire_read(path, uuid=uuid)

    assert uuid == restored_uuid

    with pytest.raises(RuntimeError):
        await lock_system.acquire_write(path, block=False)

    # Release the read lock using the restored UUID
    await lock_system.release_read(path, uuid)

    # Now the write lock should succeed
    write_uuid = await lock_system.acquire_write(path)
    assert write_uuid

    await lock_system.release_write(path, write_uuid)
    await lock_system.close()


async def test_hierarchical_write_lock_restore():
    lock_system = RedisHiLok(redis_host, ttl=2000, refresh_interval=500)

    path = "hierarchical/restore/write"
    uuid = await lock_system.acquire_write(path)

    restored_uuid = await lock_system.acquire_write(path, uuid=uuid)

    assert uuid == restored_uuid

    with pytest.raises(RuntimeError):
        await lock_system.acquire_read(path, block=False)

    await lock_system.release_write(path, uuid)

    read_uuid = await lock_system.acquire_read(path)
    assert read_uuid

    await lock_system.release_read(path, read_uuid)
    await lock_system.close()


async def test_hierarchical_read_write_conflict_restore():
    lock_system = RedisHiLok(redis_host, ttl=2000, refresh_interval=500)

    path = "hierarchical/conflict/restore"
    read_uuid = await lock_system.acquire_read(path)

    with pytest.raises(RuntimeError):
        await lock_system.acquire_write(path, block=False)

    restored_read_uuid = await lock_system.acquire_read(path, uuid=read_uuid)
    assert read_uuid == restored_read_uuid

    await lock_system.release_read(path, restored_read_uuid)

    write_uuid = await lock_system.acquire_write(path)
    assert write_uuid

    await lock_system.release_write(path, write_uuid)
    await lock_system.close()


async def test_hierarchical_lock_expiry_and_restore():
    lock_system = RedisHiLok(redis_host, ttl=300, refresh_interval=200)

    path = "hierarchical/expiry/restore"
    read_uuid = await lock_system.acquire_read(path)

    # Sleep beyond TTL to let the lock expire
    await asyncio.sleep(0.6)

    # Attempt to restore the expired lock, should fail
    with pytest.raises(RuntimeError):
        await lock_system.acquire_read(path, uuid=read_uuid)

    # Acquire a new read lock
    new_uuid = await lock_system.acquire_read(path)
    assert new_uuid != read_uuid

    await lock_system.release_read(path, new_uuid)
    await lock_system.close()

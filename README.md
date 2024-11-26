# Redis Hierarchical Distributed Read-Write Locking

This is a lightweight implementation of a hierarchical distributed read-write lock using Redis.

It supports concurrent readers and exclusive writers in a tree-like hierarchy, ensuring locks on ancestors affect descendants.

## Features
- Hierarchical locking with customizable path separators (e.g., `/`, `:`).
- Concurrent read locks on the same path or ancestors.
- Exclusive write locks on paths or descendants.
- Timeout and non-blocking lock options.
- Automatic lock refreshing for long-running operations.

## Installation

```bash
pip install redishilok
```

## Usage

```python
import asyncio
from redishilok import RedisHiLok

async def main():
    hilok = RedisHiLok('redis://localhost:6379/0')

    # Acquire a read lock
    async with hilok.read('a/b'):
        print("Read lock acquired on 'a/b'")

        # Acquire a write lock with non-blocking mode
        try:
            async with hilok.write('a', block=False):
                print("Write lock acquired on 'a'")
            print("Never gets here")
        except RuntimeError:
            print("Failed to acquire write lock on 'a'")

asyncio.run(main())
```

## Examples

### Basic Hierarchical Locking

```python
import asyncio
from redishilok import RedisHiLok

async def main():
    hilok = RedisHiLok('redis://localhost:6379/0')

    # Concurrent readers
    async with hilok.read('a'):
        async with hilok.read('a/b/c'):
            print("Both read locks succeed")

    # Write lock blocks descendants
    async with hilok.write('a'):
        try:
            async with hilok.read('a/b/c', timeout=0.1):
                pass
        except RuntimeError:
            print("Failed to acquire read lock on 'a/b/c' due to write lock on 'a'")

asyncio.run(main())
```

### Custom Separator

```python
import asyncio
from redishilok import RedisHiLok

async def main():
    hilok = RedisHiLok('redis://localhost', separator=':')
    async with hilok.write('a:b:c'):
        print("Write lock acquired on 'a:b:c'")

asyncio.run(main())
```

### Long-Running Operations with Refresh

```python
import asyncio
from redishilok import RedisHiLok

async def main():
    hilok = RedisHiLok('redis://localhost', ttl=2000, refresh_interval=1000)
    async with hilok.write('a/b'):
        print("Long operation starts")
        await asyncio.sleep(5)  # Lock is automatically refreshed

asyncio.run(main())
```



### Manually-managed Operations with Handles

```python
import asyncio
from redishilok import RedisHiLok

async def main():
    hilok = RedisHiLok('redis://localhost')
    uuid = await hilok.acquire_write('a/b')
    await asyncio.sleep(1)  # Lock is not automatically refreshed, caller must "restore" the lock

    # refresh by uuid, manually
    manual = RedisHiLok('redis://localhost')
    await manual.acquire_write("a/b", uuid=uuid)

    # refresh automatically
    other = RedisHiLok('redis://localhost', refresh_interval=1000)
    async with other.write('a/b', uuid=uuid):
        # lock is auto-refreshing in the background
        pass

    # free resources
    await hilok.close()
    await manual.close()
    await other.close()

asyncio.run(main())
```

## Limitations
- Requires a running Redis instance.
- Not suitable for high-frequency locking scenarios (due to Redis round-trips).
- Lock fairness is not guaranteed (e.g., no queue for blocked writers).

## License
MIT

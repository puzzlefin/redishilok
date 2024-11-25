import asyncio
import os
from multiprocessing import Manager, Process

from redishilok import RedisHiLok


def writer_process(path, loops, counters, redis_host):
    async def writer_task():
        async with RedisHiLok(redis_host) as h:
            for _ in range(loops):
                async with h.write(path):
                    tmp = counters[path]
                    await asyncio.sleep(0.01)
                    counters[path] = tmp + 1

    asyncio.run(writer_task())


def reader_process(path, loops, counters, redis_host):
    async def reader_task():
        async with RedisHiLok(redis_host) as h:
            for _ in range(loops):
                async with h.read(path):
                    _ = counters[path]
                    await asyncio.sleep(0.01)

    asyncio.run(reader_task())


def test_adversarial_locks_and_counters_multiprocessing():
    redis_host = os.environ.get("REDIS_URL", "redis://localhost")

    # Paths for hierarchical locking
    paths = [
        "/r",
        "/r/a",
        "/r/a/a",
        "/r/a/b",
        "/r/b",
        "/r/b/a",
        "/r/b/b",
    ]

    loops = 50

    # Use a managed dictionary for shared state across processes
    with Manager() as manager:
        counters = manager.dict({path: 0 for path in paths})
        processes = []

        # Create writer and reader processes for each path
        for path in paths:
            processes.append(
                Process(target=writer_process, args=(path, loops, counters, redis_host))
            )
            processes.append(
                Process(target=reader_process, args=(path, loops, counters, redis_host))
            )

        # Start all processes
        for p in processes:
            p.start()

        # Wait for all processes to complete
        for p in processes:
            p.join()

        for p in processes:
            assert p.exitcode == 0, f"Process {p.name} exited with code {p.exitcode}"

        for path in paths:
            assert (
                counters[path] == loops
            ), f"Mismatch at {path}: counters={counters[path]} vs expected={loops}"

# Redis hierarchical distributed read-write locking

This is a simple implementation of a hierarchical distributed read-write lock
using Redis.

It is based on the algorithm described in the paper [Distributed Locks in Redis](http://redis.io/topics/distlock).

## Usage

```python
from redishilok import RedisHiLok

hilok = RedisHiLok('redis://localhost:6379/0')
with hilok.read('a/b'):
    # Do something

with hilok.write('a', block=False):
    # Do something

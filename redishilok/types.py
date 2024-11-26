import dataclasses


@dataclasses.dataclass
class RedisRWLockStatus:
    held: bool
    type: str | None
    owned: bool
    ttl: int | None


class RedisHiLokError(RuntimeError):
    pass

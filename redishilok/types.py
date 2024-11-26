import dataclasses


@dataclasses.dataclass
class RedisHiLokStatus:
    held: bool
    type: str | None
    owned: bool
    ttl: int | None


class RedisHiLokError(RuntimeError):
    pass

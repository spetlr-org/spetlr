import re
from datetime import datetime, timedelta, timezone
from typing import Union

from atc import dbg


class PartitionSpec:
    y: Union[None, int]
    m: Union[None, int]
    d: Union[None, int]
    h: Union[None, int]

    def __init__(self, y: int = None, m: int = None, d: int = None, h: int = None):
        self.y = None if y is None else int(y)
        self.m = None if m is None else int(m)
        self.d = None if d is None else int(d)
        self.h = None if h is None else int(h)

    @property
    def ys(self):
        return f"{self.y:04}"

    @property
    def ms(self):
        return f"{self.m:02}"

    @property
    def ds(self):
        return f"{self.d:02}"

    @property
    def hs(self):
        if self.h is None:
            raise ValueError("Hour partition not used")
        return f"{self.h:02}"

    @classmethod
    def from_path(cls, s: str):
        # we got the partition in the form y=2022/m=04/d=04
        # extract the parts
        pattern = r"y=(?P<y>\d+)/m=(?P<m>\d+)/d=(?P<d>\d+)(/h=(?P<h>\d+))?"
        match = re.match(pattern, s)
        return cls(
            y=match.group("y"),
            m=match.group("m"),
            d=match.group("d"),
            h=match.group("h"),  # can be None
        )

    def as_sql_spec(self) -> str:
        s = f"y='{self.ys}', m='{self.ms}', d='{self.ds}'"
        if self.h is not None:
            s += f", h='{self.hs}'"
        return s

    def as_path(self) -> str:
        # returns the partition in the form y=2022/m=04/d=04
        # if some parts are None, they will not appear in the path
        # the partial definition feature assists in discovery
        s = ""
        if self.y is not None:
            s = f"y={self.ys}"
        if self.m is not None:
            s += f"/m={self.ms}"
        if self.d is not None:
            s += f"/d={self.ds}"
        if self.h is not None:
            s += f"/h={self.hs}"
        return s

    def as_datetime(self) -> datetime:
        dbg(
            "returning datetime for "
            f"{repr(self.y)}, {repr(self.m)}, {repr(self.d)}, {repr(self.h)}"
        )
        return datetime(
            year=self.y, month=self.m, day=self.d, hour=self.h or 0, tzinfo=timezone.utc
        )

    def is_earlier_than_dt(self, dt: datetime) -> bool:
        dt = dt.astimezone(tz=timezone.utc)
        dt = dt.replace(minute=0, second=0, microsecond=0)
        if self.h is None:
            dt.replace(hour=0)
        return self.as_datetime() < dt

    def next(self):
        dt = self.as_datetime()
        if self.h is None:
            delta = timedelta(days=1)
        else:
            delta = timedelta(hours=1)

        next_dt = dt + delta

        return self.__class__(
            y=next_dt.year,
            m=next_dt.month,
            d=next_dt.day,
            h=next_dt.hour if self.h is not None else None,
        )

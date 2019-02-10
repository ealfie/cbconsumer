import json
import numbers
import logging
import datetime

from operator import itemgetter
from functools import reduce

from iso8601 import parse_date
from aioitertools import groupby as agroupby


logger = logging.getLogger(__name__)


class SizeSplitter:
    """A Key generator intended to be used with groupby()
    SizeSplitter generates increasing numeric keys (0, 1, 2...)
    changing the key each time the cumulative value
    of `field_taker(item)` reaches max_size"""

    def __init__(self, *, field_taker, max_size):
        self.max_size = max_size
        self.field_taker = field_taker
        self.acc = 0.
        self.slotnum = 0

    def __call__(self, v):
        size = float(self.field_taker(v))

        assert isinstance(size, numbers.Real)
        assert size > 0

        emit_slot = self.slotnum

        self.acc += size
        if self.acc >= self.max_size:
            self.acc = 0
            self.slotnum += 1

        return emit_slot


async def select_keys(it, include=None, exclude=None):
    """Consume dicts from `it` and emits them selecting/filtering keys"""
    exclude = set(exclude) if exclude else {}
    include = set(include) if include else None
    async for e in it:
        o = {k: v for k, v in e.items() if (include is None or k in include) and k not in exclude}
        yield o


async def trade_formatter(it):
    """Raw trade matches have str types for datetime, price and size, this function
    converts those strings to the appropiate numeric types"""

    utc = datetime.timezone.utc

    async for e in it:
        o = dict(e)

        # XXX: consider using Decimal for price & size or int with 10**n multiplier
        # beware of json serialization

        o['size'] = float(e['size'])
        o['price'] = float(e['price'])
        o['time'] = parse_date(e['time']).astimezone(utc).timestamp()  # seconds from unix epoch, UTC

        # o['time'] = int(1e3*parse_date(e['time']).astimezone(utc).timestamp()  # integral milliseconds from epoch

        yield o


async def compute_volume(it):
    """Compute trade dollar volume, adding it to the dictionary"""
    async for e in it:
        e['volume'] = e['price'] * e['size']
        yield e


async def groupby_volume(it, *, volume_slot):
    """Consumes trades and groups consecutive trades,
    such that the aggregated volume is at least `volume_slot`"""
    keyfunc = SizeSplitter(field_taker=itemgetter('volume'),
                           max_size=volume_slot)

    async for k, groups in agroupby(it, keyfunc):  # @UnusedVariable
        batch = list(groups)
        logger.debug('groupby_volume: yielding batch of %d trades', len(batch))
        yield batch


async def apply_aggregators(it, *, aggregators_defs):
    """Apply the aggregators defined in `aggregator_defs`, which
    should be a dictionary-like with each key being the aggregator name
    and each value the function to apply to the whole trades batch
    """

    def unzip(e):
        D = {k: [v[k] for v in e] for k in e[0].keys()}
        return D

    # we may do the same as above (unzip) using pandas DataFrame constructor
    # def unzip_with_pandas(e):
    #     return pd.DataFrame(e)

    async for e in it:
        df = unzip(e)
        assert len(df) > 0
        y = {k: v(df) for k, v in aggregators_defs.items()}
        yield y


async def json_file_sink(aiterator, *, filename, flush):
    """Async iterator consumer which writes each item repr() to `filename`"""
    with open(filename, 'a') as f:
        async for e in aiterator:
            f.write(json.dumps(e) + "\n")
            if flush:
                f.flush()


async def queue_consumer(q):
    """Consume from an asyncio.Queue, making it an async iterable"""
    while True:
        e = await q.get()
        yield e


def compose(*functions):
    """Function composition of `functions`: as in f(g(x))"""
    return reduce(lambda f, g: lambda x: f(g(x)), functions, lambda x: x)


def pipeline_compose(*stages):
    """Compose the pipeline as p1 | p2 | p3 | ..."""
    return compose(*reversed(stages))

# TODO: create an instrumented pipeline compose which measures time used by each stage

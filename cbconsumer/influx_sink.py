import time
import numbers
import asyncio
import logging
import datetime

import iso8601
import aioinflux


logger = logging.getLogger(__name__)


class BaseInfluxSink:

    DEFAULT_INFLUX_OPTIONS = {
        'timeout': 10,
        'retries': 3,
        'database': 'sink'
    }

    def __init__(self, in_queue, measurement,
                 influx_options=None,
                 tags=None,
                 batch_size=10,
                 max_flush_interval=15,
                 loop=None):

        self.queue = in_queue
        self.measurement = measurement
        influx_options = influx_options or {}
        self.tags = tags or {}
        self.influx_options = dict(**self.DEFAULT_INFLUX_OPTIONS).update(influx_options)
        self.loop = loop or asyncio.get_event_loop()
        self.batch_size = batch_size
        self.max_flush_interval = max_flush_interval

    def make_influx_point(self, s):
        raise NotImplementedError

    @property
    def _classname(self):
        return type(self).__qualname__

    async def run(self):
        local_queue = []
        last_time = time.monotonic()

        # FIXME: explicitly handle reconnections with queue flushing to avoid
        # infinite in_queue size
        async with aioinflux.InfluxDBClient(**self.influx_options) as client:
            await client.create_database(database=self.influx_db, **self.influx_options)
            async for s in self.metric_generator():
                point = self._make_influx_point(s)
                if len(local_queue) > self.batch_size or time.monotonic() - last_time > self.max_flush_interval:
                    logger.info('%s: [%s] writing batch of %d points',
                                self._classname,
                                self.measurement,
                                len(local_queue))

                    await client.write(local_queue)
                    last_time = time.monotonic()
                    local_queue[:] = []
                else:
                    local_queue.append(point)


def _format_time(t):
    """Tries to interpret datetime `t` and format with isoformat() in UTC timezone
    Rules:
       If given,
       - tz-aware datetime: use it
       - naive datetime: use it as UTC
       - float value: interpret it as seconds since UTC unix epoch
       - integral value: interpret it as milliseconds since UTC unix epoch
       - None: give the current UTC datetime
    """
    utc = datetime.timezone.utc

    if isinstance(t, str):
        dt = iso8601.parse_date(t, utc).astimezone(utc)
    elif isinstance(t, datetime.datetime):
        if t.tzinfo is None:
            dt = t.tzinfo.replace(tzinfo=utc)
        else:
            dt = t.astimezone(utc)
    elif isinstance(t, numbers.Integral):
        dt = datetime.datetime.fromtimestamp(t / 1e3, utc)
    elif isinstance(t, numbers.Real):
        dt = datetime.datetime.fromtimestamp(t, utc)

    elif t is None:
        dt = datetime.datetime.utcnow().replace(tzinfo=utc)
    else:
        raise ValueError

    return dt.isoformat()


class AllFieldsInfluxSink(BaseInfluxSink):

    def _make_influx_point(self, sample):

        # parse and reformat date

        time_ = _format_time(sample.get('time', None))

        tags = dict(self.tags)

        # XXX: filter fields?
        fields = {k: v for k, v in sample.items() if isinstance(v, numbers.Real)}

        point = {
            'time': time_.isoformat(),
            'measurement': self.measurement,
            'tags': tags,
            'fields': fields,
        }
        return point


class MatchesInfluxSink(BaseInfluxSink):

    def _make_influx_point(self, match):

        # parse and reformat date
        time_ = _format_time(match.get('time', None))

        tags = dict(self.tags)
        tags.update({'side': match['side']})

        point = {
            'time': time_.isoformat(),
            'measurement': self.measurement,
            'tags': tags,
            'fields': {
                'trade_id': match['trade_id'],
                'size': match['size'],
                'price': match['price'],
            }
        }
        return point

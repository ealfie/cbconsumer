import sys
import asyncio
import logging
import argparse

from functools import partial

from aionursery import Nursery
import coloredlogs

from .utils import json_dumps
from .async_utils import AsyncApp, queue_monitor
from .coinbase import CoinbaseFeed
from .pipeline import (pipeline_compose,
                       queue_consumer,
                       select_keys, trade_formatter, compute_volume,
                       groupby_volume, apply_aggregators)
from .user_aggregators import aggregators_defs


logger = logging.getLogger(__name__)


class CBConsumerApp(AsyncApp):

    def __init__(self, *, product_id, dollar_volume_slot):
        super().__init__()
        self.product_id = product_id
        self.volume_slot = dollar_volume_slot

        self.trade_queue = asyncio.Queue(loop=self.loop)
        self.cb_feed = CoinbaseFeed(self.product_id, self.trade_queue)

        # NB: cb_feed pushes, the pipeline pulls, the queue contends between the two

        self.queue_monitor_task = queue_monitor(self.trade_queue,
                                                name='trades_queue',
                                                poll_time=1,
                                                alert_on_size=2,
                                                raise_on_size=10)

        self.build_pipeline()

        # TODO: connect split queue and connect influx sink to trade queue
        # TODO: also duplicate sink to output to influx after aggregation

    def build_pipeline(self):

        # NB: if the mapper/sampler/aggregator were very-CPU intensive tasks
        # then it would be better to wrap the pipeline into a separate process
        # using aioprocessing.AioProcess

        # NB: another possibility is using a push-type pipeline, like aiostream's
        src = queue_consumer(self.trade_queue)
        keysel = partial(select_keys, include=('time', 'size', 'price'))

        mapper = pipeline_compose(keysel, trade_formatter, compute_volume)
        sampler = partial(groupby_volume, volume_slot=self.volume_slot)
        aggregator = partial(apply_aggregators, aggregators_defs=aggregators_defs)

        pipeline = pipeline_compose(mapper, sampler, aggregator)

        self.sink = pipeline(src)

    async def pull_sink(self):
        async for e in self.sink:
            print(json_dumps(e))

    async def main(self):
        async with Nursery() as ns:
            ns.start_soon(self.queue_monitor_task)
            ns.start_soon(self.cb_feed.run())
            ns.start_soon(self.pull_sink())


def setup_logger(loglevel=logging.INFO):
    pkg_name = 'cbconsumer'
    pkg_logger = logging.getLogger(pkg_name)
    pkg_logger.setLevel(loglevel)
    coloredlogs.install(level=loglevel, logger=pkg_logger, milliseconds=True)


def parse_arguments(cmdline_args):
    parser = argparse.ArgumentParser()
    parser.add_argument('--loglevel', '-l', type=str, choices=('error', 'warning', 'info', 'debug'), default='info')
    parser.add_argument('--product-id', type=str, default='BTC-USD')
    parser.add_argument('--dollar-volume-slot', type=float, default=100.)
    args = parser.parse_args(cmdline_args)
    return args


def main(cmdline_args=None):
    args = parse_arguments(cmdline_args)
    loglevel = getattr(logging, args.loglevel.upper())
    setup_logger(loglevel)
    app = CBConsumerApp(product_id=args.product_id, dollar_volume_slot=args.dollar_volume_slot)
    app.run()


# provide also an entry point here in addition to the bin/cbconsumer script
if __name__ == "__main__":
    main(sys.argv[1:])

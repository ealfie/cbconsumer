import json
import asyncio
import logging

from contextlib import suppress

import aiohttp
import websockets
import copra.rest

from aionursery import Nursery

# CoPrA is only used for the missing trades filler,
# which in turns uses aiohttp
# websocket message consumption is done using websockets and not CoPrA
# (NB: CoPrA websocket handling is a bit bizarre,
#  and wont allow some low level stuff needed here)

logger = logging.getLogger(__name__)


COINBASE_FEED_URL = 'wss://ws-feed.pro.coinbase.com'
COINBASE_REST_API_URL = 'https://api.pro.coinbase.com'


class Channel:
    """
    A Coinbase WebSocket channel.

    (adapted from the CoPrA package https://github.com/tpodlaski/copra)

    A Channel object encapsulates the Coinbase Pro WebSocket channel name
    *and* one or more Coinbase Pro product ids.

    To read about Coinbase Pro channels and the data they return, visit:
    https://docs.gdax.com/#channels

    :ivar str name: The name of the WebSocket channel.
    :ivar product_ids: Product ids for the channel.
    :vartype product_ids: set of str
    """

    def __init__(self, name, product_ids):
        """
        :param str name: The name of the WebSocket channel. Possible values are
            heatbeat, ticker, level2, full, matches, or user

        :param product_ids: A single product id (eg., 'BTC-USD') or list of
            product ids (eg., ['BTC-USD', 'ETH-EUR', 'LTC-BTC'])
        :type product_ids: str or list of str

        :raises ValueError: If name not valid or product ids is empty.
        """
        self.name = name.lower()
        if self.name not in ('heartbeat', 'ticker', 'level2', 'full', 'matches', 'user'):
            raise ValueError('invalid name {}'.format(name))
        if not product_ids:
            raise ValueError('must include at least one product id')
        if not isinstance(product_ids, list):
            product_ids = [product_ids]
        self.product_ids = set(product_ids)

    def __repr__(self):
        return '<{}: {}>'.format(type(self).__qualname__, str(self._as_dict()))

    def _as_dict(self):
        """Returns the Channel as a dictionary.

        :returns dict: The Channel as a dict with keys name & value list
            of product_ids.
        """
        return {'name': self.name, 'product_ids': list(self.product_ids)}

    def __eq__(self, other):
        if self.name != other.name:
            raise TypeError('Channels need the same name to be compared.')
        return self.name == other.name and self.product_ids == other.product_ids

    def __sub__(self, other):
        if self.name != other.name:
            raise TypeError('Channels need the same name to be subtracted.')
        product_ids = self.product_ids - other.product_ids
        if not product_ids:
            return
            return Channel(self.name, list(product_ids))

    @classmethod
    def get_subscribe_message(cls, channels, unsubscribe=False):
        """Create and return the subscription message for the provided channels.

        :param channels: List of channels to be subscribed to.
        :type channels: list of Channel

        :param bool unsubscribe:  If True, returns an unsubscribe message
            instead of a subscribe method. The default is False.

        :returns: JSON-formatted, UTF-8 encoded bytes object representing the
            subscription message for the provided channels.
        """
        msg_type = 'unsubscribe' if unsubscribe else 'subscribe'
        msg = {'type': msg_type, 'channels': [channel._as_dict() for channel in channels]}
        return json.dumps(msg)


class BareCoinbaseFeed:
    """Coinbase Pro Websocket feed consumer"""

    class BrokenHeartBeat(IOError):
        """Heartbeat stream interrupted"""
        pass

    def __init__(self,
                 product_id,
                 queue,
                 uri=COINBASE_FEED_URL,
                 max_heartbeat_wait_seconds=5,
                 max_reconnections=None,
                 ping_interval=10,
                 ping_timeout=10,
                 loop=None):

        self.uri = uri
        self.product_id = product_id
        self.queue = queue

        self.max_heartbeat_wait_seconds = max_heartbeat_wait_seconds
        self.max_reconnections = max_reconnections
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout

        self.loop = loop or asyncio.get_event_loop()
        self._broken_heart = asyncio.Event(loop=self.loop)
        self._broken_heart.clear()
        self._heartbeat_timer = None

        self._subscribed = asyncio.Event(loop=self.loop)

    @property
    def _classname(self):
        return type(self).__qualname__

    def _heartbeat_alarm(self):
        self._broken_heart.set()

    def _start_heartbeat_check(self):
        assert self._heartbeat_timer is None
        if self.max_heartbeat_wait_seconds is not None:
            self._heartbeat_timer = self.loop.call_later(self.max_heartbeat_wait_seconds,
                                                         self._heartbeat_alarm)

    def _stop_heartbeat_check(self):
        if self._heartbeat_timer is not None:
            self._heartbeat_timer.cancel()
            self._heartbeat_timer = None

    async def _wait_msg_or_alarm(self, client):

        recv_task = self.loop.create_task(client.recv())
        heart_task = self.loop.create_task(self._broken_heart.wait())

        fs = [recv_task, heart_task]

        done = None  # @UnusedVariable
        pending = None

        try:
            done, pending = await asyncio.wait(fs, return_when=asyncio.FIRST_COMPLETED)  # @UnusedVariable
        except asyncio.CancelledError:
            for f in fs:
                f.cancel()
                with suppress(asyncio.CancelledError):
                    await f

        exc = None

        # order of checking done status is important in corner cases
        if recv_task.done():
            if recv_task.exception() is not None:
                exc = recv_task.exception()
            else:
                msg = recv_task.result()

        if heart_task.done():
            exc = self.BrokenHeartBeat()
            self._broken_heart.clear()

        if pending:
            for p in set(pending):
                p.cancel()
                with suppress(asyncio.CancelledError):
                    await p
        if exc:
            raise exc

        return msg

    async def run(self):

        connection_tries = 0
        while self.max_reconnections is None or connection_tries <= self.max_reconnections:
            connection_tries += 1

            def reconn_str():
                mr = str(self.max_reconnections) if self.max_reconnections else 'infinity'
                return '(reconnection={:d}/{:s})'.format(connection_tries, mr)

            self.subscribed = False
            try:
                logger.debug('%s: websocket: trying to connect', self._classname)
                async with websockets.client.connect(self.uri,
                                                     ping_interval=self.ping_interval,
                                                     ping_timeout=self.ping_timeout) as client:

                    logger.info('%s: websocket client connected %s',
                                self._classname, reconn_str())

                    await self._subscribe(client)

                    # TODO: raise if subscription not received in some timeframe
                    # TODO: heartbeat check should start after receiving subscription message

                    self._start_heartbeat_check()

                    while True:
                        msg = await self._wait_msg_or_alarm(client)
                        element = self._msg_handler(msg)
                        if element is not None:
                            logger.info('%s: inserting match msg into queue', self._classname)
                            await self.queue.put(element)

            # no heartbeat received
            except self.BrokenHeartBeat:
                logger.error('%s: websocket, broken heartbeat %s',
                             self._classname,
                             reconn_str)
                continue

            # this handles hard TCP errors
            except ConnectionError as e:
                logger.error('%s: websocket %s %s',
                             self._classname, type(e).__name__,
                             reconn_str())
                continue

            # this handles websocket disconnections at ws protocol level
            except websockets.exceptions.ConnectionClosed:
                logger.error('%s: websocket, detected disconnection %s',
                             self._classname,
                             reconn_str())
                continue

            # task cancellation (sigint, etc)
            except asyncio.CancelledError:
                logger.info('%s: run task cancelled, not reconnecting',
                            self._classname)
                break

            finally:
                self._stop_heartbeat_check()
        else:
            logger.error('%s: websocket: max reconnections (%r) achieved, exiting',
                         self._classname, self.max_reconnections)

    def _handle_heartbeat(self, data):  # @UnusedVariable
        if self.max_heartbeat_wait_seconds is not None:
            logger.debug('%s: heartbeat received, resetting timer',
                         self._classname)
            self._stop_heartbeat_check()
            self._start_heartbeat_check()
        else:
            logger.debug('%s: heartbeat received: ignoring', self._classname)

    async def _subscribe(self, client):

        channels = [
            Channel('matches', self.product_id),
            Channel('heartbeat', self.product_id),
        ]

        subscribe_msg = Channel.get_subscribe_message(channels)

        logger.debug('%s, subscribing to channels', self._classname)

        await client.send(subscribe_msg)
        logger.debug('%s: done subscribing to channeld', self._classname)

    def _msg_handler(self, msg):

        # returns None if no further processing, else return the match message
        # to be enqueued or yielded

        try:
            data = json.loads(msg)
        except json.JSONDecodeError:
            logger.error('%s: invalid json received in ws msg', self._classname)
            return

        if 'type' not in data:
            logger.error('%s: json message has no type field', self._classname)
            return

        elif data['type'] == 'match':
            logger.debug('%s: match msg received: %r', self._classname, data)
            return data

        if data['type'] == 'heartbeat':
            self._handle_heartbeat(data)

        elif data['type'] == 'error':
            logger.error('%s: error message received: %r', self._classname, data)

        elif data['type'] == 'last_match':
            logger.debug('%s: ignoring expected msgtype=last_match',
                         self._classname)

        elif data['type'] == 'subscriptions':
            logger.info('%s: subscription message received', self._classname)
            self._subscribed.set()

        else:
            logger.warning('%s: ignoring msgtype=%s', self._classname, data['type'])


class TradesGapsFiller:
    """Takes trade matches as input and produces matches with no gaps,
    filling the missing matches calling the Coinbase Pro REST API"""

    def __init__(self, product_id, in_queue, out_queue,
                 uri=COINBASE_REST_API_URL,
                 max_tries=10,
                 last_trade_id=None,
                 loop=None):

        self.product_id = product_id
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.max_tries = max_tries

        self.uri = uri
        self.last_tid = last_trade_id
        self.loop = loop or asyncio.get_event_loop()

    @property
    def _classname(self):
        return type(self).__qualname__

    async def run(self):

        while True:
            this_trade = await self.in_queue.get()
            tid = this_trade['trade_id']

            if self.last_tid is not None and tid <= self.last_tid:
                exc_msg = '{}: non increasing trade id: tid={}, last_tid={}'.format(tid, self.last_tid)
                raise AssertionError(exc_msg)

            num_missing_trades = tid - self.last_tid - 1 if self.last_tid is not None else None
            assert num_missing_trades is None or num_missing_trades >= 0

            is_contiguous = self.last_tid is None or (num_missing_trades == 0)

            logger.debug("%s: ltid=%r, tid=%r, cont=%r",
                         self._classname,
                         self.last_tid, tid, is_contiguous)

            if not is_contiguous:

                logger.warning('%s: trade_id discontinuity: ltid=%r, tid=%r: fetching %d missing trades',
                               self._classname,
                               self.last_tid,
                               tid,
                               num_missing_trades)

                missing_trades = await self.get_missing_trades(tid,
                                                               num=num_missing_trades)

                for t in missing_trades:
                    mtid = t['trade_id']
                    logger.info('%s: sending missed trade %d', self._classname, mtid)

                    if mtid != self.last_tid + 1:
                        raise AssertionError

                    await self.out_queue.put(t)

                    self.last_tid = mtid

            logger.info('%s: sending trade, id=%d', self._classname, tid)

            await self.out_queue.put(this_trade)
            self.last_tid = tid

    async def get_missing_trades(self, tid, num):
        """Fetch the `num` trades with trade_id less than `tid`"""

        # here I prefer to have the Client resource explicitly open/closed on
        # each fetch: cleanup is easier and since this function should
        # be seldomly called, there is no disadvantage in doing so

        # FIXME: handle num > max permitted by Coibase API (around ~100?)
        tries = 0
        while tries < self.max_tries:
            tries += 1
            try:
                async with copra.rest.Client(self.loop, self.uri) as client:
                    missing_trades, first, last = await client.trades(self.product_id,  # @UnusedVariable
                                                                      after=tid,
                                                                      limit=num)

                    missing_trades = list(reversed(missing_trades))
                return missing_trades
            except aiohttp.ClientConnectionError:
                logger.warning('%s: get_missing_trades: connection error, will retry')
                continue
        else:
            logger.error('%s: get_missing_trades: no more tries (max=%d)',
                         self._classname, self.max_tries)
            raise ConnectionError('{}: get_missing_trades (max connection trials reached)'.format(self._classname))


class CoinbaseFeed:
    """Coinbase Pro Websocket feed consumer with trades gap filler"""

    def __init__(self,
                 product_id,
                 queue,
                 ws_uri=COINBASE_FEED_URL,
                 rest_uri=COINBASE_REST_API_URL,
                 max_heartbeat_wait_seconds=5,
                 max_reconnections=None,
                 ping_interval=10,
                 ping_timeout=10,
                 loop=None):

        self.loop = loop
        out_queue = queue

        # intermediate queue between WS feed and gap filler
        bare_queue = asyncio.Queue(loop=self.loop)

        self.cb_ws_feed = BareCoinbaseFeed(product_id, bare_queue,
                                           uri=ws_uri,
                                           max_heartbeat_wait_seconds=max_heartbeat_wait_seconds,
                                           max_reconnections=max_reconnections,
                                           ping_interval=ping_interval,
                                           ping_timeout=ping_timeout,
                                           loop=loop)

        self.gap_filler = TradesGapsFiller(product_id,
                                           bare_queue,
                                           out_queue,
                                           uri=rest_uri)

    async def run(self):
        async with Nursery() as ns:
            ns.start_soon(self.cb_ws_feed.run())
            ns.start_soon(self.gap_filler.run())

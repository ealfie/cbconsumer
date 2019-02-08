import os
import signal
import asyncio
import logging
import functools
import threading


logger = logging.getLogger(__name__)


try:
    import uvloop
except ImportError:
    pass
else:
    if not bool(os.environ.get('CBCONSUMER_UVLOOP_DISABLE', False)):
        uvloop.install()


class AsyncApp:

    def __init__(self):

        # expecting to be the main thread here
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.loop = loop

        logger.debug('%s: using event loop: %r', self._classname, self.loop)

        self._stopping = False
        self._main_task = None

        # expecting to be main thread here to handle signals correctly
        if not isinstance(threading.current_thread(), threading._MainThread):
            raise RuntimeError('AsyncApp class is meant to be used from the main thread')

        for signame in ('SIGINT', 'SIGTERM'):
            handler = functools.partial(self.sighandler, signame)
            sig = getattr(signal, signame)
            self.loop.add_signal_handler(sig, handler)

    def __repr__(self):
        return '<{}: {}>'.format(self._classname, id(self))

    @property
    def _classname(self):
        return type(self).__qualname__

    async def main(self):
        """To be overriden in subclasses, here I write an example"""

        logger.debug('%s: main task starting', self._classname)
        ev = asyncio.Event(loop=self.loop)
        try:
            await ev.wait()
        except asyncio.CancelledError:
            logger.debug('%s: main task cancelling', self._classname)
        finally:
            logger.debug('%s: main task closed', self._classname)

    def sighandler(self, signame):
        if not self._stopping:
            self._stopping = True
            logger.warning('%s: received signal %r, cancelling main task', self._classname, signame)
            self.loop.call_soon_threadsafe(self._main_task.cancel)
        else:
            logger.warning('%s: already exiting: ignoring signal %r', self._classname, signame)

    def run(self):
        self._main_task = self.loop.create_task(self.main())
        logger.info('%s: starting main task', self._classname)
        try:
            self.loop.run_until_complete(self._main_task)
        except asyncio.CancelledError:
            logger.info('%s: cancellation finished', self._classname)
        else:
            logger.info('%s: main task exited by itself', self._classname)
        finally:
            logger.debug('%s: shutting down pending async generators', self._classname)
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
            logger.debug('%s: closing event loop', self._classname)
            self.loop.close()
            logger.info('%s: finished', self._classname)


class QueueSplitter:

    def __init__(self, in_queue, *out_queues, blocking=False):
        self.in_queue = in_queue
        self.out_queues = out_queues
        self.blocking = blocking

    async def run(self):
        while True:
            item = await self.in_queue.get()
            for q in self.out_queues:
                if self.blocking:
                    await q.put(item)
                else:
                    q.put_nowait(item)


async def queue_monitor(queue, *, name, poll_time, alert_on_size, raise_on_size):

    # NB: Yes, we could have used max_sized queues, but I'd want a way to instrument
    # and measure queue sizes

    assert raise_on_size >= alert_on_size

    watermark = alert_on_size
    while True:
        await asyncio.sleep(poll_time)
        size = queue.qsize()
        if size >= watermark:
            msg = 'QueueMonitor: {} has reached a size greater than {:d}'.format(name, size)
            logger.warning(msg)
            watermark = size + 1

        if size >= raise_on_size:
            msg = 'QueueMonitor: {} has reached a size {:d} greater than {:d}'.format(
                name, size, raise_on_size)

            raise OverflowError(msg)

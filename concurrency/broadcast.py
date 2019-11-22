"""
This module implements message queue multiplexing, or broadcasting.
An instance of the Broadcast class is used to publish messages to a number of mailboxes,
implemented using asyncio.Queue.
"""

import asyncio
from concurrency.utils import race
from typing import (
    Mapping
)


class MailboxClosed(Exception): pass

class Mailbox:
    def __init__(self, queue):
        self.queue = queue
        self.closed = asyncio.Event()
        
    async def receive(self, ack=True):
        if self.closed.is_set():
            raise MailboxClosed
        queue_task = asyncio.create_task(self.queue.get())
        closed_task = asyncio.create_task(self.closed.wait())
        winners = await race(queue_task, closed_task)
        if queue_task in winners:
            value = await queue_task
            if ack:
                self.queue.task_done()
            return value
        else:
            assert closed_task in winners
            raise MailboxClosed

    async def __aiter__(self):
        while True:
            try:
                yield await self.receive()
            except MailboxClosed:
                break

    def close(self):
        self.closed.set()
            
            
class Broadcast:
    subscribers: Mapping[str, asyncio.Queue]

    def __init__(self, subscribers=()):
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__qualname__}")
        self.subscribers = dict(subscribers)

    def publish(self, value):
        for name, s in self.subscribers.items():
            if s.full():
                # discard oldest item from queue
                # TODO: log this
                self.logger.info("Queue for subscriber %s is full. Discarding oldest message.")
                s.get_nowait()
            s.put_nowait(value)

    def subscribe(self, name, buffer_size=0):
        """Add a new subscription, returning a new mailbox"""
        self.subscribers[name] = new_queue = asyncio.Queue(buffer_size)
        return Mailbox(new_queue)

    def unsubscribe(self, name):
        self.subscribers.pop(name, None)

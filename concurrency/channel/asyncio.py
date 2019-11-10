import asyncio
from concurrency import utils
import typing


class ChannelClosed(Exception):
    def __init__(self, channel_id):
        super().__init__(channel_id)
        self.channel_id = channel_id        

        
T = typing.TypeVar("T")


class _ChannelCommon(typing.Generic[T]):
    __slots__ = ["_channel_id", "_queue", "_closed"]
    def __init__(self, channel_id, queue: asyncio.Queue, close_event: asyncio.Event):
        self._channel_id = channel_id
        self._queue = queue
        self._closed = close_event

    async def raise_closed(self):
        await self._closed.wait()
        raise ChannelClosed(self.channel_id)

    @property
    def channel_id(self):
        return self._channel_id

    def close(self):
        self._closed.set()
        
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_val is None:
            # on normal exit, close and wait for messages to be received before closing
            self.close()
            await self._queue.join()
        elif exc_type is ChannelClosed and exc_val.channel_id == self.channel_id:
            # silence ChannelClosed exception
            return True
        else:
            # on unexpected error, close channel to unblock readers
            self.close()
    
        
class ReadChannel(_ChannelCommon[T]):
    async def receive(self) -> T:
        if not self._queue.empty():
            # if a value is already queued, return it
            value = await self._queue.get()
            self._queue.task_done()
            return value
        elif self._closed.is_set():
            await self.raise_closed()
        else:
            # if channel is not yet closed and no value is already queued,
            # wait for a value or a close, whichever comes first.
            check_closed, get_value = (
                asyncio.create_task(self.raise_closed()),
                asyncio.create_task(self._queue.get())
            )
            # this ensures that if one finishes first, the other is cancelled.
            # both may finish together, so we still need to check each task to know what's up
            winners = await utils.race(
                [check_closed, get_value]
            )
            if get_value in winners:
                # no matter what, if we manage to get a value before getting closed, we return it
                value = await get_value
                self._queue.task_done()
                return value
            elif check_closed in winners:
                assert self._closed.is_set()
                # channel got closed before getting a value
                await check_closed

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return await self.receive()
        except ChannelClosed:
            raise StopAsyncIteration

        
class WriteChannel(_ChannelCommon[T]):
    async def send(self, m: T, sync=False):
        if self._closed.is_set():
            await self.raise_closed()
        else:
            check_closed, put_value = (
                asyncio.create_task(self.raise_closed()),
                asyncio.create_task(self._queue.put(m))
            )
            winners = await utils.race([
                check_closed, put_value
            ])
            if put_value in winners:
                await put_value # just to make sure exceptions are reraised
                if sync:
                    # wait for message to be received
                    await self._queue.join()
            elif check_closed in winners:
                assert self._closed.is_set()
                await check_closed



class FullChannel(ReadChannel[T], WriteChannel[T]):
    def split(self) -> typing.Tuple[ReadChannel[T], WriteChannel[T]]:
        write_channel = WriteChannel(
            channel_id=self._channel_id,
            queue=self._queue,
            close_event=self._closed
        )
        read_channel = ReadChannel(
            channel_id=self._channel_id,
            queue=self._queue,
            close_event=self._closed
        )
        return read_channel, write_channel
                

def channel(size=1) -> FullChannel[T]:
    queue = asyncio.Queue(maxsize=size)
    closed = asyncio.Event()
    channel_id = id(queue)
    # read_end = ReadChannel(channel_id, queue, closed)
    # write_end = WriteChannel(channel_id, queue, closed)
    full_channel = FullChannel(channel_id=channel_id, queue=queue, close_event=closed)
    return full_channel



async def monitor(tasks, channel: WriteChannel[dict]):
    """
    A monitoring task that sends an event to a channel
    whenever a watched task is done,
    and a final event when all tasks are done.
    """
    async with channel:
        active_tasks = set(tasks)
        while active_tasks:
            done, pending = await asyncio.wait(active_tasks, return_when=asyncio.FIRST_COMPLETED)
            active_tasks.difference_update(done)
            for t in done:
                # three possibilities: task was cancelled, task ended with an exception, task ended successfully
                if t.cancelled():
                    await channel.send({
                        "type": "cancelled",
                        "task": t,
                        "remaining": len(active_tasks),
                        "time": time.time()
                    })
                elif t.exception():
                    await channel.send({
                        "type": "exception",
                        "task": t,
                        "remaining": len(active_tasks),
                        "time": time.time()
                    })
                else:
                    await channel.send({
                        "type": "success",
                        "task": t,
                        "remaining": len(active_tasks),
                        "time": time.time()
                    })
        else:
            await channel.send({
                "type": "all_done",
                "time": time.time()
            })

            
async def monitor_poll(tasks, channel: WriteChannel[dict]):
    """
    A monitoring task that polls a set of tasks, 
    notifying a channel on each poll cycle,
    whenever a watched task is done,
    and a final event when all tasks are done.
    """
    async with channel:
        active_tasks = set(tasks)
        while active_tasks:
            finished = [t for t in active_tasks if t.done()]
            active_tasks.difference_update(finished)
            await channel.send({
                "type": "heartbeat",
                "remaining": len(active_tasks),
                "time": time.time()
            })
            for t in finished:
                if t.cancelled():
                    await channel.send({
                        "type": "cancelled",
                        "task": t,
                        "remaining": len(active_tasks),
                        "time": time.time()
                    })
                elif t.exception():
                    await channel.send({
                        "type": "exception",
                        "task": t,
                        "remaining": len(active_tasks),
                        "time": time.time()
                    })
                else:
                    await channel.send({
                        "type": "success",
                        "task": t,
                        "remaining": len(active_tasks),
                        "time": time.time()
                    })
        else:
            await channel.send({
                "type": "all_done",
                "time": time.time()
            })
            
    
if __name__ == "__main__":
    import time

    async def produce(c: WriteChannel[int]):
        async with c:
            for i in range(10):
                print("Producing {}! ".format(i), time.time())
                await c.send(i)
                await asyncio.sleep(0)
                if i > 3:
                    c.close()
            else:
                print("Done!")

    async def consume(c: ReadChannel[int]):
        async with c:
            async for x in c:
                print("Consuming {}! ".format(x), time.time())
                await asyncio.sleep(0)
            else:
                print("Channel closed. Goodbye.")

    async def main():
        r, w = channel().split()
        tasks = [
            asyncio.create_task(produce(w)),
            asyncio.create_task(consume(r)),
        ]
        mr, mw = channel(1).split()
        monitor_task = asyncio.create_task(monitor(tasks, mw))
        
        async for event in mr:
            print("monitor event: ", event)
        else:
            await asyncio.gather(*tasks, monitor_task)

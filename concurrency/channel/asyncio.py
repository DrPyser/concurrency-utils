import asyncio
from concurrency import utils


class ChannelClosed(Exception):
    def __init__(self, channel_id):
        super().__init__(channel_id)
        self.channel_id = channel_id        


class _ChannelCommon:
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
            # on normal exit, wait for messages to be received before closing
            await self._queue.join()
            self.close()
        elif exc_type is ChannelClosed and exc_val.channel_id == self.channel_id:
            # silence ChannelClosed exception
            return True
    
        
class ReadChannel(_ChannelCommon):
    async def receive(self):
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
            await utils.race(
                [check_closed, get_value]
            )
            if not get_value.cancelled():
                # no matter what, if we manage to get a value before getting closed, we return it
                value = await get_value
                self._queue.task_done()
                return value
            if not check_closed.cancelled():
                # channel got closed before getting a value
                await check_closed

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return await self.receive()
        except ChannelClosed:
            raise StopAsyncIteration

        
class WriteChannel(_ChannelCommon):
    async def send(self, m, sync=True):
        if self._closed.is_set():
            await self.raise_closed()
        else:
            check_closed, put_value = (
                asyncio.create_task(self.raise_closed()),
                asyncio.create_task(self._queue.put(m))
            )
            await utils.race([
                check_closed, put_value
            ])
            if not put_value.cancelled():
                await put_value # just to make sure exceptions are reraised
                if sync:
                    # wait for message to be received
                    await self._queue.join()
            if not check_closed.cancelled():
                await check_closed

                
def channel(size=1):
    queue = asyncio.Queue(maxsize=size)
    closed = asyncio.Event()
    channel_id = id(queue)
    read_end = ReadChannel(channel_id, queue, closed)
    write_end = WriteChannel(channel_id, queue, closed)
    return read_end, write_end


if __name__ == "__main__":
    import time
    async def produce(c):
        async with c:
            for i in range(10):
                print("Producing {}! ".format(i), time.time())
                await c.send(i)
                await asyncio.sleep(0)
                if i > 3:
                    c.close()
            else:
                print("Done!")

    async def monitor(tasks):
        active_tasks = set(tasks)
        while active_tasks:
            print("Checking on tasks. ", time.time())
            finished = [t for t in active_tasks if t.done()]
            exceptionals = [t for t in finished if not t.cancelled() and t.exception()]
            for t in exceptionals:
                print(f"task {t} raised an exception: {t.exception()}")                
            active_tasks.difference_update(finished)
            print(f"{len(active_tasks)} remaining active tasks")
            await asyncio.sleep(0)
        print("all tasks finished.", time.time())
                
    async def consume(c):
        async with c:
            async for x in c:
                print("Consuming {}! ".format(x), time.time())
                await asyncio.sleep(0)
            else:
                print("Channel closed. Goodbye.")

    async def main():
        r, w = channel()
        tasks = [
            asyncio.create_task(produce(w)),
            asyncio.create_task(consume(r))
        ]
        done, pending = await asyncio.wait([            
            asyncio.create_task(monitor(tasks)),
            *tasks
        ])
    
    #linear_schedule = schedule.Schedule.from_delay_stream(itertools.repeat(1))

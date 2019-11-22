import concurrent.futures
import asyncio
from typing import (
    Callable, Union,
    Iterable, Awaitable,
    AsyncIterator, AsyncIterable,
    TypeVar, NamedTuple, Generic, Any,
    List
)
import typing
import functools
import itertools
import time
import datetime
import types

T = TypeVar("T")

spawn = asyncio.create_task
"""An alias for asyncio.create_task"""


class DefaultAsynchronousSettings:
    @property
    def loop(self):
        return asyncio.get_event_loop()

    @property
    def executor(self):
        return None


def asynchronous(settings: object=DefaultAsynchronousSettings()):
    """
    Decorator factory to transform
    synchronous functions into asynchronous ones
    by running them in separate threads
    """
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            loop = settings.loop
            executor = settings.executor
            return loop.run_in_executor(
                executor,
                functools.partial(
                    f, *args, **kwargs
                ),
            )
        return wrapper
    return decorator


async def race(*aws: Awaitable):
    tasks = [asyncio.ensure_future(aw) for aw in aws]
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    if pending:
        for t in pending:
            t.cancel()
        else:
            # wait for all tasks to finish, i.e. for cancellation to propagate
            await asyncio.wait(pending, return_when=asyncio.ALL_COMPLETED)
    assert all(t.done() for t in pending), f"Cancelled tasks still pending"
    assert all(t.done() for t in done)
    return done


async def select(cases: dict):
    tasks = {
        key: asyncio.ensure_future(t)
        for key, t in cases.items()
    }
    tasks_keys = {
        f: key
        for key, f in tasks.items()
    }
    try:
        winners = await race(tasks.values())
    except asyncio.TimeoutError:
        if asyncio.TimeoutError in cases:
            return cases[asyncio.TimeoutError]()
        else:
            raise
    else:
        assert winners <= tasks_keys.keys()
        return {
            tasks_keys[t]: t
            for t in winners
        }


async def select_handlers(cases: dict):
    task_handlers = {
        asyncio.ensure_future(aw): handler
        for aw, handler in cases.items()
        if not isinstance(aw, asyncio.TimeoutError) or aw is not None
    }
    try:
        winners = await race(tasks.values(), timeout=timeout)
    except asyncio.TimeoutError:
        if asyncio.TimeoutError in cases:
            return cases[asyncio.TimeoutError]()
        else:
            raise
    else:
        winner = winners.pop()
        return task_handlers[winner](await winner)


Default = object()
    
async def select_now(cases: dict):
    futures = {
        key: asyncio.ensure_future(t)
        for key, t in cases.items()
    }
    completed = next(((k, f) for k, f in futures.items() if f.done()), (None, None))
    return completed

if False:
    key, task = select_now({
        "r1": make_call(...),
        "r2": make_call(...)
    })
    if key is None:
        # default
        ...
    elif key == "r1":
        result = await r1
    elif key == "r2":
        result = await r2
    
    

def dataclass(klass):
    annotations = typing.get_type_hints(klass)
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        attrs = ", ".join("{k}={v}".format(k=k, v=getattr(self, k)) for k in self.__fields__)
        return f"{self.__class__.__name__}({attrs})"

    def extend_ns(ns):
        ns.update(
            __init__=__init__,
            __slots__=list(annotations.keys()),
            __fields__=list(annotations.keys()),
            __repr__=__repr__
        )
    new_klass = types.new_class(klass.__name__, bases=(klass,), exec_body=extend_ns)
    return new_klass

E = TypeVar("E")

class Outcome(Generic[E, T]):
    @classmethod
    def safely(cls, exc_types, f, *args, **kwargs):
        try:
            return Success(f(*args, **kwargs))
        except exc_types as ex:
            return Failure(ex)
        
    @classmethod
    def safe_iter(cls, dangerous_iter):
        it = iter(dangerous_iter)
        while True:
            try:
                yield Success(next(it))
            except StopIteration:
                break
            except Exception as ex:
                yield Failure(ex)
                

@dataclass
class Success(Outcome[E, T]):
    value: T

@dataclass
class Failure(Outcome[E, T]):
    value: E


async def join(aws: Iterable[Awaitable[T]]) -> List[T]:
    return [await f for f in asyncio.as_completed(aws)]

async def safe_join(aws: Iterable[Awaitable[T]]) -> List[Outcome[Exception, T]]:
    return list(Outcome.safe_iter(await f for f in asyncio.as_completed(aws)))


def link_tasks(t1: Union[asyncio.Task, asyncio.Future], t2: Union[asyncio.Task, asyncio.Future]):
    def propagate_failure(other: asyncio.Task, t: asyncio.Task):
        if t.cancelled():
            other.cancel()
        elif t.exception() is not None:
            other.cancel()

    t1.add_done_callback(functools.partial(propagate_failure(t2)))
    t2.add_done_callback(functools.partial(propagate_failure(t1)))


async def anext(ait: AsyncIterator[T]) -> T:
    return await ait.__anext__()
    

def aiter(ait: AsyncIterable[T]) -> AsyncIterator[T]:
    return ait.__aiter__()


class AsyncIteratorWrapper(AsyncIterator[T]):
    def __init__(self, it: Iterable[T], buffer_size=0):
        self._source = iter(it)
        self._tasks = set()
        self._cancelled = asyncio.Event()
        self._other_loop = None
        self._buffer = asyncio.Queue(buffer_size)

    @staticmethod
    async def _iterate(source, cancelled):
        try:
            while not cancelled.is_set():
                yield next(source)
                await asyncio.sleep(0)
        except StopIteration:
            return

    def _start(self):
        async def fill_buffer(buffer, source, cancelled, iterate):
            iterator = iterate(source, cancelled)
            try:
                while True:
                    i = await anext(iterator)
                    await buffer.put(i)
            except StopAsyncIteration:
                return
        def do_work(loop, buffer, source, cancelled, iterate):
            asyncio.set_event_loop(loop)
            loop.run_until_complete(fill_buffer(buffer, source, cancelled, iterate))

        self._other_loop = asyncio.new_event_loop()
        iterate_task = asyncio.run_in_executor(
                self._executor,
            functools.partial(
                do_work,
                loop,
                self._buffer,
                self._source,
                self._cancelled,
                self._iterate
            )
        )
        self._tasks.add(
            iterate_task
        )
        self._iterate_task = iterate_task
        
    async def __anext__(self):
        if self._other_loop is None:
            self._start()

        if self._cancelled.is_set():
            raise asyncio.CancelledError
        elif not self._other_loop.is_running():
            raise StopAsyncIteration
        elif ...:
            ...
            
        queue_task, cancel_task = (
            asyncio.create_task(c)
            for c in (
                    self._buffer.get(),
                    self._cancelled.wait()
            )
        )
        winners = await race([queue_task, cancel_task])
        if queue_task in winners:
            return await queue_task
        else:
            assert self._cancelled.is_set()
            assert not self._other_loop.is_running()
            assert all(t.done() for t in self._tasks)
            raise asyncio.CancelledError

    def __aiter__(self):
        return self
    

def primed(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        c = f(*args, **kwargs)
        next(c)
        return c
    return wrapped


class Ticker:
    __slots__ = ["_delay_stream"]
    
    def __init__(self, delay_stream):
        self._delay_stream = enumerate(delay_stream)

    async def __anext__(self):
        try:
            i, delay = next(self._delay_stream)
        except StopIteration:
            raise StopAsyncIteration
        else:
            await asyncio.sleep(delay)
            return i

    def __aiter__(self):
        return self

    @classmethod
    def constant(cls, t, limit=None):
        repeater = itertools.repeat(t, limit) if limit is not None\
            else itertools.repeat(t)
        return cls(repeater)


import logging


#async def propagator(t1: asyncio.Task, t2: asyncio.Task, callback: Callable=None):



class RestartPolicy:
    ...          


class TaskSpec(NamedTuple):
    factory: Callable[..., Awaitable[T]]  # allow to create task instances
    restart_policy: RestartPolicy  # when to restart this task
    result_handler: Callable[[T], Any]  # something to do on normal termination
    exception_handler: Callable[[Exception], Any]  # something to do on exceptional termination
    
    ...


class Supervisor:
    """
    Must be able to 
    * spawn tasks
    * restart failed tasks
    """
    def __init__(self):
        self._task_specs
        self._task_manager = TaskGroup()
        self._active_tasks = {}
        
    async def supervise(self):
        while True:
            # TODO: allow non-exceptional termination?
            done, pending = await asyncio.wait(self._task_manager.tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                # TODO: keep archive of tasks?
                spec = self._active_tasks.pop(t)
                # TODO: remove task from task manager?
                self._task_manager.remove_tasks(t)
                try:
                    result = await t
                except Exception as ex:
                    # cancellation can also be handled here
                    spec.exception_handler(ex)
                else:
                    spec.result_handler(result)
                    
                # restart task
                self.restart(spec)

    def restart(self, spec):
        # TODO: maintain restart counter, throttling, allow different restart policies
        self._spawn(spec)

    def _spawn(self, spec):
        task = self._task_manager.spawn(spec.factory())
        self._active_tasks[task] = spec

    async def start(self):
        async with self._task_manager as manager:
            for spec in self._task_specs:
                self._spawn(spec)
            else:
                # run supervision task
                await self.supervise()

                
async def after(n: float, f, *args, **kwargs):
    """call async function after n seconds"""
    await asyncio.sleep(n)
    return await f(*args, **kwargs)


# if False:
#     async def foo(i):
#         print("Hello: ", i)
#         return i    
#     result = await after(10, foo, 1)

                
LOGGER = logging.getLogger(__name__)




# async def main():
#     # settings = DefaultAsynchronousSettings()
#     # asynchronous_ = asynchronous(settings)

#     # @asynchronous_
#     # def test(i):
#     #     print("See you in {} seconds".format(i))
#     #     time.sleep(i)
#     #     print("Done!")
#     #     return i

#     # now = datetime.datetime.utcnow()
#     # print(now)
#     # results = await asyncio.gather(
#     #     test(2),
#     #     test(5),
#     #     test(3),
#     #     test(1),
#     #     test(5),
#     #     test(5),
#     #     test(5),
#     #     test(5),
#     #     test(5),
#     #     test(5),
#     #     test(5),
#     # )
#     # then = datetime.datetime.utcnow()
#     # print(then)
#     # print("duration {}".format(then-now))
#     # print(results)
#     async def faulty_routine():
#         print("starting faulty_routine")
#         while True:
#             await asyncio.sleep(1)
#             raise Exception("faulty_routine failed!")

#     async def other_routine():
#         print("starting other_routine")
#         while True:
#             await asyncio.sleep(1)
#             print("Doing things")

#     try:
#         async with TaskGroup() as g:
#             t1 = g.spawn(faulty_routine())
#             t2 = g.spawn(other_routine())
#     except:
#         LOGGER.exception("Error")

#     tasks = map(asyncio.create_task, (faulty_routine(), other_routine()))
#     await TaskGroup(tasks=tasks)
        
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    #asyncio.run(main())

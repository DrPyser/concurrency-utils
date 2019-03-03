import concurrent.futures
import asyncio
from typing import Callable, Union, Iterable, Awaitable
import functools
import time
import datetime


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


async def race(aws: Iterable[Awaitable], timeout=None):
    done, pending = await asyncio.wait(aws, timeout=timeout)
    for t in pending:
        t.cancel()
    return done.pop()


def link_tasks(t1: Union[asyncio.Task, asyncio.Future], t2: Union[asyncio.Task, asyncio.Future]):
    def propagate_failure(other: asyncio.Task, t: asyncio.Task):
        if t.cancelled():
            other.cancel()
        elif t.exception() is not None:
            other.cancel()

    t1.add_done_callback(functools.partial(propagate_failure(t2)))
    t2.add_done_callback(functools.partial(propagate_failure(t1)))

    
import logging


#async def propagator(t1: asyncio.Task, t2: asyncio.Task, callback: Callable=None):

import abc
import types
from typing import FrozenSet


class TaskManager(abc.ABC):
    tasks: FrozenSet[asyncio.Task]

    @abc.abstractmethod
    def spawn(self, coro: types.CoroutineType) -> asyncio.Task:
        """
        spawn a task managed by this TaskManager
        """
        pass

    @abc.abstractmethod
    def cancel(self):
        """
        Cancel all tasks managed by this TaskManager
        """
        pass

    
    
async def entangle(tasks, propagate=True, logger=None):
    """
    Entangle the fates of a group of tasks,
    ensuring all are cancelled if one fails or is cancelled.
    """
    logger = logger or logging.getLogger(__name__)
    try:
        return await asyncio.gather(*tasks)
    except Exception:
        for t in tasks:
            logger.debug("Cancelling task: {}".format(t))
            t.cancel()
        if propagate:
            raise


async def link(task1, task2, propagate=True, logger=None):
    """
    Create a unidirectional link between two tasks,
    such that an exception raised in task2 
    guarantees cancellation of task1.
    """
    logger = logger or logging.getLogger(__name__)
    try:
        await task2
    except:
        logger.info("link triggered on task {} following exception from task {}".format(task1, task2))
        task1.cancel()
        if propagate:
            raise

        
class MultiError(Exception):
    def __init__(self, exceptions):
        self._exceptions = exceptions


import enum


class TaskGroupState(enum.Enum):
    OPEN = enum.auto()
    CLOSED = enum.auto()


class TaskGroupClosedError(Exception):
    def __init__(self, task_group):
        super().__init__("Invalid operation on closed TaskGroup")
        self.task_group = task_group

    
class TaskGroup(TaskManager):
    """An object that keeps track of spawned tasks"""

    def __init__(self, tasks=(), timeout=None, logger=None, loop=None, name=None):
        self.name = name or str(id(self))
        self._tasks = set(tasks)
        self.logger = logger or logging.getLogger(
            __name__ + "." + type(self).__name__ + "." + self.name
        )
        self.loop = loop or asyncio.get_event_loop()
        self._state = TaskGroupState.OPEN
        self._timeout = timeout

    @property
    def tasks(self):
        return frozenset(self._tasks)

    def __contains__(self, t: asyncio.Task):
        return t in self._tasks

    def add_tasks(self, *tasks):
        """Add existing tasks to this TaskGroup."""
        if self._state is TaskGroupState.CLOSED:
            raise TaskGroupClosedError(self)
        self._tasks.update(tasks)

    def remove_tasks(self, *tasks):
        """
        Remove tasks from this task group.
        This does not cancel those tasks.
        """
        if self._state is TaskGroupState.CLOSED:
            raise TaskGroupClosedError(self)
        self._tasks.difference_update(tasks)

    def spawn(self, coro):
        """
        Spawn a task from a coroutine, into this task group.
        """
        if self._state is TaskGroupState.CLOSED:
            raise TaskGroupClosedError(self)
        task = self.loop.create_task(coro)
        self._tasks.add(task)
        return task

    def __await__(self):
        # wait for all tasks to complete
        # yield from asyncio.wait(self._tasks, return_when=asyncio.ALL_COMPLETED).__await__()
        # # identify tasks that failed(or were cancelled)
        # exceptions = [
        #     asyncio.CancelledError if t.cancelled() else t.exception()
        #     for t in self._tasks
        #     if t.cancelled() or t.exception() is not None
        # ]
        # if len(exceptions):
        #     raise MultiError(exceptions)
        # else:
        # just raise on first task error, else return task results
        if self._timeout is None:
            yield from asyncio.gather(*self._tasks).__await__()
        else:
            yield from asyncio.wait_for(asyncio.gather(*self._tasks), self._timeout).__await__()

    async def __aiter__(self):
        for t in self._tasks:
            yield (await t)

    def __iter__(self):
        for t in self._tasks:
            yield t.result()

    def cancel(self):
        """
        Cancel all tasks in this task group
        """
        for t in self._tasks:
            t.cancel()
            
    async def __aenter__(self):
        if self._state is TaskGroupState.CLOSED:
            raise TaskGroupClosedError(self)
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        self._state = TaskGroupState.CLOSED
        if exc_value is not None:
            self.cancel()
        else:
            try:
                await self
            except:
                self.cancel()
                raise

    def __len__(self):
        return len(self._tasks)

    @property
    def status(self):
        return self._state

    def __repr__(self):
        return f"{type(self).__name__}(name={self.name}, size={len(self._tasks)}, status={self._state})"



class RestartPolicy:
    ...          


from typing import NamedTuple, TypeVar, Any

T = TypeVar("T")


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
            

LOGGER = logging.getLogger(__name__)

async def main():
    # settings = DefaultAsynchronousSettings()
    # asynchronous_ = asynchronous(settings)

    # @asynchronous_
    # def test(i):
    #     print("See you in {} seconds".format(i))
    #     time.sleep(i)
    #     print("Done!")
    #     return i

    # now = datetime.datetime.utcnow()
    # print(now)
    # results = await asyncio.gather(
    #     test(2),
    #     test(5),
    #     test(3),
    #     test(1),
    #     test(5),
    #     test(5),
    #     test(5),
    #     test(5),
    #     test(5),
    #     test(5),
    #     test(5),
    # )
    # then = datetime.datetime.utcnow()
    # print(then)
    # print("duration {}".format(then-now))
    # print(results)
    async def faulty_routine():
        print("starting faulty_routine")
        while True:
            await asyncio.sleep(1)
            raise Exception("faulty_routine failed!")

    async def other_routine():
        print("starting other_routine")
        while True:
            await asyncio.sleep(1)
            print("Doing things")

    try:
        async with TaskGroup() as g:
            t1 = g.spawn(faulty_routine())
            t2 = g.spawn(other_routine())
    except:
        LOGGER.exception("Error")

    tasks = map(asyncio.create_task, (faulty_routine(), other_routine()))
    await TaskGroup(tasks=tasks)
        
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    #asyncio.run(main())

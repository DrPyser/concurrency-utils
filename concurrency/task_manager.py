import abc
import types
from typing import FrozenSet, Callable, Union, Iterable, Awaitable
import concurrent.futures
import asyncio
import functools
import time
import datetime
import logging
import enum


class TaskManager(abc.ABC):
    """
    Basic interface for objects that represent a group of tasks
    that are managed together.
    """
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
    
    
async def entangle(tasks, propagate=False, logger=None):
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


async def link(task1, task2, propagate=False, logger=None):
    """
    Create a unidirectional link between two tasks,
    such that an exception raised in task2 
    result in cancellation of task1.
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

    def __iter__(self):
        return iter(self._exceptions)
    
    # TODO: add appropriate __repr__ and __str__

class TaskGroupState(enum.Enum):
    OPEN = enum.auto()
    CLOSED = enum.auto()


class TaskGroupClosedError(Exception):
    def __init__(self, task_group):
        super().__init__("Invalid operation on closed TaskGroup")
        self.task_group = task_group

        
async def preempt(tasks, timeout=None):
    waiter = asyncio.gather(*tasks)
    if timeout is not None:
        return await asyncio.wait_for(waiter, timeout)
    else:
        return await waiter

    
async def wait_for_all(tasks, timeout=None, on_cancellation=None, on_exception=None):
    waiter = asyncio.wait(tasks, timeout=timeout)
    done, pending = await waiter
    for t in pending:
        t.cancel()
    cancellations = [
        t for t in done if t.cancelled()
    ]
    # TODO: 
    exceptions = [
        (t, t.exception())
        for t in done
        if not t.cancelled() and t.exception() is not None
    ]
        


class TaskGroup(TaskManager):
    def __init__(self, tasks=(), join_policy=preempt, logger=None, loop=None, name=None):
        self.name = name or str(id(self))
        self._tasks = set(tasks)
        self.logger = logger or logging.getLogger(
            __name__ + "." + type(self).__name__ + "." + self.name
        )
        self.loop = loop or asyncio.get_event_loop()
        self._state = TaskGroupState.OPEN
        self._join_policy = join_policy

    @property
    def tasks(self):
        return frozenset(self._tasks)

    def __contains__(self, t: asyncio.Task):
        return t in self._tasks

    def add_tasks(self, *tasks):
        """Add existing tasks to this TaskGroup"""
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
        Spawn a task into this task group.
        """
        if self._state is TaskGroupState.CLOSED:
            raise TaskGroupClosedError(self)
        task = self.loop.create_task(coro)
        self._tasks.add(task)
        return task

    def __await__(self):
        yield from self._join_policy(self.tasks).__await__()

    async def __aiter__(self):
        for t in self._tasks:
            yield (await t)

    def __iter__(self):
        for t in self._tasks:
            yield t.result()

    def cancel(self):
        """
        Cancel all running tasks in this task group
        """
        for t in self._tasks:
            if not t.done():
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
                await self._join_policy(self.tasks)
            finally:
                # tasks not awaited to completion by join policy are cancelled
                self.cancel()

    def __len__(self):
        return len(self._tasks)

    @property
    def status(self):
        return self._state

    def __repr__(self):
        return f"{type(self).__name__}(name={self.name}, size={len(self._tasks)}, status={self._state})"

    @classmethod
    def scoped(cls, f):
        """Decorator to inject a task group as first argument of a callable"""
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            return f(cls(), *args, **kwargs)
        return wrapper

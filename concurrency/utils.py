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
    done, pending = await asyncio.wait(aws, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()
    else:
        # wait for all tasks to finish, i.e. for cancellation to propagate
        await asyncio.wait(aws, return_when=asyncio.ALL_COMPLETED)
    assert all(t.done() for t in pending), str(pending)
    assert all(t.done() for t in done)
    #assert len(done) <= 1
    # note: there may be more than 1 task in done, but we only return one "winner"
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

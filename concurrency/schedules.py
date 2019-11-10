import itertools
import functools
import asyncio
from typing import (
    Iterable,
    AsyncGenerator,
    TypeVar,
    Callable,
    NamedTuple,
    Any,
    Awaitable,
    Generator,
    Generic,
    Tuple
)
import enum

A = TypeVar("A")
B = TypeVar("B")


DelayStream = Iterable[float]
"""Type of a (potentially infinite) stream of delay values suitable for asyncio.sleep"""


ScheduleState = Generator[A, B, None]
"""Type of the internal state of a Schedule"""

            
class ScheduleStateStatus(enum.Enum):
    STARTED = enum.auto() # coroutine created but not initialized
    READY = enum.auto() # coroutine created and initialized, ready to receive input
    CONCLUDED = enum.auto() # coroutine concluded, cannot receive new input
   
    
class ScheduleConcluded(Exception): pass


class Error(NamedTuple):
    value: Any

class Result(NamedTuple):
    value: Any


def try_all(fs, *args):
    results = []
    for f in fs:
        try:
            result = f(*args)
        except Exception as ex:
            results.append(Error(ex))
        else:
            results.append(Result(result))
    else:
        return results

    
def recur_delay(delay_stream):
    x = (yield)
    for d in delay_stream:
        x = yield (x, d)


def identity(x):
    return x
        
    
class Schedule(Generic[A, B]):
    """
    Wrapper for the state of a running schedule
    """
    def __init__(self, generator: ScheduleState[A,B], ready=False):
        self._generator = generator
        self._state = ScheduleStateStatus.STARTED if not ready else ScheduleStateStatus.READY

    def _initialize(self):
        self._generator.send(None)
        self._state = ScheduleStateStatus.READY

    def _finalize(self):
        self._generator.close()
        self._state = ScheduleStateStatus.CONCLUDED

    def update(self, x):
        if self._state is ScheduleStateStatus.STARTED:
            # schedule coroutine was created but not initialized
            self._initialize()
        elif self._state is ScheduleStateStatus.CONCLUDED:
            # schedule coroutine has concluded, cannot update
            raise ScheduleConcluded
        try:
            (output, delay) = self._generator.send(x)
        except StopIteration as ex:
            # schedule concluded
            self._finalize()
            raise ScheduleConcluded from ex        
        else:
            return (output, delay)

    def __next__(self):
        try:
            return self.update(None)
        except ScheduleConcluded:
            raise StopIteration

    def __iter__(self):
        return self
        
    def feed(self, inputs):
        """
        Feed the schedule a stream of inputs(padded with None),
        and return an iterator over the recurrences, 
        until it's conclusion.
        """
        extended_inputs = itertools.chain(
            inputs,
            itertools.repeat(None)
        )
        for x in extended_inputs:
            try:
                yield self.update(x)
            except ScheduleConcluded:
                break

    @classmethod
    def from_delay_stream(cls, delay_stream):        
        return Schedule(recur_delay(delay_stream))

    def __or__(self, other):
        return Schedule(union(self, other))

    def __and__(self, other):
        return Schedule(intersection(self, other))

    def and_then(self, other):
        return Schedule(chain(self, other))

    def map(self, f):
        return Schedule(map_schedule(f, self))

    def limit(self, n: int):
        return Schedule(limit(self, n))

    @classmethod
    def lift(cls, f, delay_stream=itertools.repeat(0)):
        return Schedule(lift(f, delay_stream))

    @classmethod
    def recur_until(cls, pred, delay_stream=itertools.repeat(0)):
        return Schedule(recur_until(pred, delay_stream))

    @classmethod
    def recur_while(cls, pred, delay_stream=itertools.repeat(0)):
        return Schedule(recur_while(pred, delay_stream))

    @classmethod
    def accumulate(cls, f, delay_stream=itertools.repeat(0), initial=identity):
        return Schedule(accumulate(f, delay_stream, initial=initial))
    

def limit(schedule, n: int):
    input_ = (yield)
    for i in range(n):
        input_ = yield schedule.update(input_)
        
    
    
def map_schedule(f, schedule):
    input_ = (yield)
    while True:
        try:
            out, d = schedule.update(input_)            
        except ScheduleConcluded:
            # break out of while loop
            break
        else:
            input_ = yield f(out), d

            
def lift(f: Callable[[A], B], delay_stream):
    input_ = (yield)
    for d in delay_stream:
        input_ = yield (f(input_), d)
    

def chain(*schedules: Schedule[A, B]) -> Generator[A, B, None]:
    """
    Schedule combinator performing sequential chaining of multiple schedules.
    Creates a new schedule state that runs each schedule to conclusion in sequence.
    """
    input_ = (yield)
    for schedule in schedules:
        while True:
            try:
                input_ = yield schedule.update(input_)
            except ScheduleConcluded:
                # break out of while loop, go to next schedule
                break

            
C = TypeVar("C")
            
            
def intersection(a: Schedule[A, B], b: Schedule[A, C]) -> Generator[Tuple[B, C], A, None]:
    """
    Schedule combinator performing the intersection of two schedules.
    Create a new schedule state that runs both schedules concurrently 
    and continues only if both continues, concluding whenever either conclude.
    The output is a tuple of the outputs of both original schedules,
    and the delay is the maximum of both.
    """

    input_ = (yield)
    while True:
        results = try_all([a.update, b.update], input_)
        exceptional = next(
            (
                r
                for r in results
                if isinstance(r, Error)
                and not isinstance(r.value, ScheduleConcluded)
            ), None
        )
        if exceptional is not None:
            raise exceptional.value from exceptional.value

        if any(isinstance(r, Error) and isinstance(r.value, ScheduleConcluded) for r in results):
            return
        else:
            result_a, result_b = results
            (oa, da) = result_a.value
            (ob, db) = result_b.value
            input_ = yield (oa, ob), max(da, db)

            
def union(a: Schedule[A, B], b: Schedule[A, C]) -> Generator[Tuple[B, C], A, None]:
    """
    Schedule combinator performing the union of two schedules, 
    creating a new schedule which continues 
    if either continues and concluding only when both conclude.
    The output is a tuple of the outputs of both original schedule(with None as default if already concluded),
    and the delay is the minimum of both.
    """
    input_ = (yield)
    while True:
        results = try_all([a.update, b.update], input_)
        exceptional = next(
            (
                r
                for r in results
                if isinstance(r, Error)
                and isinstance(r.value, ScheduleConcluded)
            ), None
        )
        if exceptional is not None:
            raise exceptional.value from exceptional.value

        result_a, result_b = results
        if isinstance(result_a, Result) and isinstance(result_b, Result):
            (oa, da) = result_a.value
            (ob, db) = result_b.value
            # yield both outputs, but min delay
            input_ = yield (oa, ob), min(da, db)
        elif isinstance(result_a, Result):
            # yield output and delay from first schedule
            (oa, da) = result_a.value
            input_ = yield (oa, None), da
        elif isinstance(result_a, Result):
            # yield output and delay from second schedule
            (ob, db) = result_b.value
            input_ = yield (None, ob), db
        else:
            return


def recur_until(pred, delay_stream):
    input_ = (yield)
    for d in delay_stream:
        if not pred(input_):
            input_ = yield (input_, d)
        else:
            break
        
def recur_while(pred, delay_stream):
    input_ = (yield)
    for d in delay_stream:
        if pred(input_):
            input_ = yield (input_, d)
        else:
            break

def accumulate(f, delay_stream, initial=identity):
    """Schedule that outputs a state computed from a reduction of inputs"""
    input_ = (yield)
    acc = initial(input_)
    for d in delay_stream:
        input_ = yield (acc, d)
        acc = f(acc, input_)


        
OpinionT = TypeVar("OpinionT")
async def retry(f: Callable[..., Awaitable[A]], schedule: Schedule[Exception, Tuple[OpinionT, float]]):
    """
    Run an awaitable computation, 
    retrying on failures according to a schedule.
    """
    while True:
        try:
            result = await f()
        except Exception as ex:
            try:
                opinion, delay = schedule.update(ex)
            except ScheduleConcluded:
                raise ex from None
            else:
                await asyncio.sleep(delay)
                # TODO: do something with opinion
                yield (ex, opinion)
        else:
            return result

        
def result(retry_generator):
    try:
        while True:        
            next(retry_generator)
    except StopIterator as ex:
        return ex.value




async def repeat(f: Callable[..., Awaitable[A]], schedule: Schedule):
    """
    Repeat an awaitable computation according to a schedule.
    """
    while True:
        result = yield await f()
        try:
            opinion, delay = schedule.update(result)
        except ScheduleConcluded:
            return
        else:
            await asyncio.sleep(delay)
            # TODO: do something with opinion            

import queue
import enum
import typing
from typing import Generator, Any
import threading


class ThreadMessage(enum.Enum):
    Abort = enum.auto()

    class Signal(typing.NamedTuple):
        exc: BaseException

    class Continue(typing.NamedTuple):
        value: Any = None

    def __call__(self, *args, **kwargs):
        return self.value(*args, **kwargs)


class ThreadState(enum.Enum):
    INITIAL = enum.auto()
    """created, not yet started"""
    RUNNING = enum.auto()
    """thread is running, either doing work or waiting for message"""
    ABORTED = enum.auto()
    """aborted, not yet joined"""
    DEAD = enum.auto()
    """done"""

    
class InvalidWorkerStateError(Exception):
    """Raised when state of worker does not allow this operation"""
    pass


class WorkerManager:
    """
    A class to interact with a worker thread.
    Can be used as a context manager, starting the thread on enter
    and joining on exit(also trying to abort on exceptions).
    """
    def __init__(self, thread, control_queue, control_timeout):
        self.thread = thread
        self.control_queue = control_queue
        self.control_timeout = control_timeout
        assert not thread.is_alive()
        self._state = ThreadState.INITIAL
        
    def signal(self, exc: BaseException, timeout=None):
        """Raise an exception in the worker"""
        if self._state is not ThreadState.RUNNING:
            raise InvalidWorkerStateError
        self.control_queue.put(
            ThreadMessage.Signal(exc),
            timeout=timeout if timeout is not None else self.control_timeout
        )

    def abort(self, timeout=None):
        """Tell the worker to abort and return as soon as possible."""
        if self._state is not ThreadState.RUNNING:
            raise InvalidWorkerStateError
        self.control_queue.put(
            ThreadMessage.Abort,
            timeout=timeout if timeout is not None else self.control_timeout
        )
        self._state = ThreadState.ABORTED

    def continue_work(self, value=None, timeout=None):
        """Tell the worker to keep going, optionally providing a value."""
        if self._state is not ThreadState.RUNNING:
            raise InvalidWorkerStateError
        self.control_queue.put(
            ThreadMessage.Continue(value),
            timeout=timeout if timeout is not None else self.control_timeout
        )

    def __enter__(self):
        if self._state is not ThreadState.INITIAL:
            raise InvalidWorkerStateError
        self.thread.start()
        self._state = ThreadState.RUNNING
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # send abort message and join thread
        # note: if self.control_timeout != None, this might raise a queue.Full error
        # TODO: maybe ignore timeout and join the queue instead?
        if self._state is ThreadState.RUNNING and exc_value is not None:
            self.abort() # sends the abort signal
        try:
            self.thread.join()
        finally:
            self._state = ThreadState.DEAD

    def __repr__(self):
        return f"{type(self).__name__}(state={self._state}, control_timeout={self.control_timeout})"
    
        
CONTROL_TIMEOUT = 1


def worker(
        target: Generator,
        control_buffer_size=0,
        control_timeout=None,
        output_handler=None) -> WorkerManager:
    """
    Create a worker thread from a generator coroutine.

    The `target` generator coroutine only needs to implement the worker's application logic,
    yielding occasionally between work steps to allow the thread to handle cancellation and other signals.
    The target can use yield points to receive inputs and produce intermediary outputs, 
    which can be handled(e.g. printed, put in a queue, etc)
    by providing a generator coroutine for the `output_handler` parameter.
    By default, values yielded by the worker's target are ignored.

    A WorkerManager instance is returned, 
    which can be used as a context manager to start the worker, and wait for its completion on exit.
    """
    control_queue = queue.Queue(control_buffer_size)
    def thread_runner():
        while True:
            # TODO: define protocol with other control messages
            # e.g. `Abort` (instead of GeneratorExit), `Continue(value=None)`        
            message = control_queue.get()
            if message is ThreadMessage.Abort:
                target.close()
                control_queue.task_done()
                return
            elif isinstance(message, ThreadMessage.Signal.value):
                try:
                    target.throw(message.exc)
                finally:
                    control_queue.task_done()
            elif isinstance(message, ThreadMessage.Continue.value):
                output = target.send(message.value)                
                if output_handler is not None:
                    output_handler.send(output)
                control_queue.task_done()
            else:
                raise ValueError(message)
            
    t = threading.Thread(target=thread_runner)
    return WorkerManager(t, control_queue=control_queue, control_timeout=control_timeout)

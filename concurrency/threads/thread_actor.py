"""
Erlang-inspired implementation of an actor-style concurrent system using threads.
An actor is code running in a thread, interacting with other actors using message passing.
"""
import threading
import queue
import atexit
from typing import (
    FrozenSet,
    NamedTuple,
    Any,
    Callable,
    Optional,
    List,
    Tuple
)
import functools
import uuid
import logging
import collections
import time


Shutdown = object()
POLL_TIMEOUT = 0.1


class ActorTerminated(Exception):
    def __init__(self, actor):
        super().__init__(f"Actor {actor} is dead.")

class UnhandledMessage(Exception):
    def __init__(self, message, sender, receiver):
        super().__init__(f"Actor {receiver} could not handle message {message} from sender {sender}.")        
        self.message = message
        self.sender = sender
        self.receiver = receiver

class ActorRef:
    __slots__ = ()

    
class Message(NamedTuple):
    priority: int
    sender: Optional[ActorRef]
    data: Any

    def __lt__(self, other):
        return self.priority < other.priority

    def __gt__(self, other):
        return self.priority > other.priority

    def __le__(self, other):
        return self.priority <= other.priority

    def __ge__(self, other):
        return self.priority >= other.priority



    
        
class Priority:
    HIGH = 1
    NORMAL = 10
    LOW = 20

class Actor:
    ref: ActorRef
    def send(self, message, sender, priority): pass



class Timeout(NamedTuple):
    after: int

    def __call__(self, x):
        return False
    
timeout = Timeout

class Match(NamedTuple):
    pred: Callable[..., bool]

    def __call__(self, x):
        return self.pred(x)

match = Match


Predicate = Callable[[Message], bool]
        
class Request:
    class Self(NamedTuple):
        pass

    class Receive(NamedTuple):
        expectations: List[Tuple[Predicate, Any]]

    class Send(NamedTuple):
        message: Any
        target: ActorRef


class Signal:
    class Exit(NamedTuple):
        reason: Any

    class Kill(NamedTuple):
        reason: Any
        
        
class ExitSignalError(Exception):
    def __init__(self, signal: Signal.Exit, actor_ref, reaction=None):
        super().__init__(f"Exit signal received: {signal}")
        self.signal = signal
        self.reaction = reaction
        self.actor = actor_ref


class KillSignalError(Exception):
    def __init__(self, signal: Signal.Kill, actor_ref):
        super().__init__(f"Kill signal received: {signal}")
        self.signal = signal
        self.actor = actor_ref  
    

class System:
    actors: FrozenSet[ActorRef]

    def __init__(self, poll_timeout=POLL_TIMEOUT):
        self._actors = {}
        self._poll_timeout = poll_timeout
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__qualname__}")

    @property
    def actors(self):
        return frozenset(
            self._actors.keys()
        )
    
    def spawn(self, init, init_args=(), **kwargs) -> ActorRef:
        actor_id = uuid.uuid4()
        a = ThreadActor(self, actor_id=actor_id, init=init, init_args=init_args, **kwargs)
        a.start()
        ref = a.ref
        self._actors[ref] = a
        return ref

    def send(self, message, target, sender=None):
        actor = self._actors[target]
        actor.send(
            message=message,
            sender=sender,
            priority=Priority.NORMAL
        )

    def send_signal(self, signal, target, sender=None):
        actor = self._actors[target]
        actor.send_signal(signal=signal, sender=sender)

    def send_signal_all(self, signal, sender=None):
        for ref, actor in self._actors.items():
            actor.send_signal(signal=signal, sender=sender)

    def kill(self, target, reason=None, sender=None):
        self.send_signal(Signal.Kill(reason=reason), target, sender=sender)

    def exit(self, target, reason=None, sender=None):
        self.send_signal(Signal.Exit(reason=reason), target, sender=sender)

    def exit_all(self, reason=None, sender=None):
        self.send_signal_all(Signal.Exit(reason=reason), sender=sender)

    def kill_all(self, reason=None, sender=None):
        self.send_signal_all(Signal.Kill(reason=reason), sender=sender)

    def join(self, target, timeout=None):
        actor = self._actors[target]
        actor.join(timeout)
        # Should raise TimeoutError if thread still alive

    def join_all(self, timeout=None):
        for actor in self._actors.values():
            actor.join(timeout)
        else:
            # should raise TimeoutError if any thread still alive
            pass

class ThreadActorRef(ActorRef, tuple):
    def __new__(cls, actor_id, thread_id, name):
        return tuple.__new__(cls, (actor_id, thread_id, name))    

    
def iter_queue(q: queue.Queue, timeout=None):
    while True:
        try:
            value = q.get(timeout=timeout)
        except queue.Empty:
            break
        else:
            yield value            

            
def poll(tests, value):
    for key, pred in tests:
        if pred(value):
            yield key

            
def find_index(pred, seq, default=None):
    return next(((i, x) for i, x in enumerate(seq) if pred(x)), default)



class Mailbox:
    def __init__(self, q, logger=None):
        self.logger = logger or logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__qualname__}")
        self.queue = q
        self.storage = []

    def send(self, message):
        self.queue.put(message)

    def select(self, expectations):
        expectations = list(expectations)
        message_matchers = list(
            (key, m)
            for key, m in expectations
            if isinstance(m, Match)
        )
        found = next((
            (i, m, key)
            for i, m in enumerate(self.storage)
            for key, pred in expectations
            if pred(m)
        ), None)
        if found is not None:
            index, message, key = found
            self.storage.pop(index)
            self.logger.info("Found matching message in storage for match key %s", key)
            return message, key
        else:
            timeout_key, timeout = next((
                (key, cond.after)
                for key, cond in expectations
                if isinstance(cond, Timeout)
            ), (None, None))
            for m in iter_queue(self.queue, timeout=timeout):
                self.logger.info("Reading new message from queue: %s", m)
                match_key = next(poll(expectations, m), None)
                if match_key is not None:
                    self.logger.info("Found matching message for match key %s", match_key)
                    return m, match_key
                else:
                    self.logger.info("Message %s not expected, storing and skipping.", m)
                    self.storage.append(m)
            else:
                self.logger.info("Timeout on select")
                # timeout
                assert timeout is not None
                return None, timeout_key

    def __iter__(self):
        yield from self.storage
        yield from iter_queue(self.queue)

    def read(self, n=-1, timeout=None):
        yield from self.storage
        count = 0
        for m in iter_queue(self.queue, timeout=timeout):
            self.storage.append(m)
            yield m
            count += 1
            if n != -1 and count >= n:
                break       
        
        
class ThreadActor(threading.Thread):   
    def __init__(self, system, actor_id, init, init_args, poll_timeout=POLL_TIMEOUT):
        super().__init__()
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__qualname__}.{init.__name__}")
        self.system = system
        self.actor_id = actor_id
        self.init = init
        self.init_args = init_args
        self.poll_timeout = poll_timeout
        self.dispatcher = None
        self.mailbox = Mailbox(queue.SimpleQueue(), self.logger.getChild("mailbox"))
        self.signal_queue = queue.SimpleQueue()

    @property
    def ref(self):
        return ThreadActorRef(self.actor_id, self.ident, self.name)

    def receive(self, expectations):
        return self.mailbox.select(expectations)

    def handle_signal(self, signal, coro):
        if isinstance(signal, Signal.Kill):
            # Just raise exception, without allowing any cleanup of user state
            raise KillSignalError(signal, self.ref)
        elif isinstance(signal, Signal.Exit):
            try:
                reaction = coro.throw(ExitSignalError(signal, self.ref))
            except ExitSignalError:
                # exception was not handled
                raise
            except Exception as ex:
                # another exception occured
                raise
            else:
                # exception was handled. 
                raise ExitSignalError(signal, self.ref, reaction)
            finally:
                # make sure generator terminates and has no remaining state
                coro.close()

    def handle_request(self, request, coro):
        if request is None:
            return coro.send(None)
        elif isinstance(request, Request.Self):
            return coro.send(self.ref)
        elif isinstance(request, Request.Send):
            target = request.target
            message = request.message
            self.system.send(message, target, self.ref)
            return coro.send(None)
        elif isinstance(request, Request.Receive):
            result = self.receive(request.expectations)
            return coro.send(result)
                
    def run(self):
        coro = self.init(*self.init_args)
        request = None
        while True:
            try:
                self.logger.info("Checking for signals")
                signal = self.signal_queue.get_nowait()
            except queue.Empty:
                # run user code
                try:
                    self.logger.info("Handling user code")
                    request = self.handle_request(request, coro)
                except StopIteration as ex:
                    # user code terminated normally
                    # do something with ex.value?
                    self.logger.info(f"User routine terminated normally with return value {ex.value}")
                    break
            else:
                self.logger.info("Received signal: %s", signal)
                # handle signal
                try:
                    self.handle_signal(signal, coro)
                except ExitSignalError:
                    self.logger.info("Terminating following exit signal")
                    break
                except KillSignalError:
                    self.logger.info("Terminating following kill signal")
                    break

    def send(self, message, sender, priority=Priority.NORMAL):
        if not self.is_alive():
            raise ActorTerminated(self)
        else:
            self.mailbox.send(Message(priority=priority, sender=sender, data=message))

    def send_signal(self, signal, sender):
        if not self.is_alive():
            raise ActorTerminated(self)
        else:
            self.signal_queue.put_nowait(signal)
        


def pong():
    while True: 
        message, key = yield Request.Receive([
            ("ping", Match(lambda m: m.data == "ping")),
            ("shutdown", Match(lambda m: m.data is Shutdown)), 
            ("timeout", Timeout(5))
        ])
        if key == "ping":
            print("Received ping", flush=True)
            sender = message.sender
            if sender is not None:
                yield Request.Send("pong", message.sender)
                #yield Request.Send(Shutdown, message.sender)
        elif key == "shutdown":
            print("okay, goodbye", flush=True)
            break
        elif key == "timeout":
            print("Took a bit too long", flush=True)


def ping(server):
    while True:
        yield Request.Send("ping", server)
        message, key = yield Request.Receive([
            ("reply", Match(lambda m: m.data == "pong")),
            ("shutdown", Match(lambda m: m.data is Shutdown)),
            ("timeout", Timeout(5))
        ])
        if key == "reply":
            print(f"Received reply to ping from {message.sender}", flush=True)
            #yield Request.Send(Shutdown, server)
        elif key == "shutdown":
            print("okay, goodbye", flush=True)
            break
        elif key == "timeout": 
            print("Took a bit too long", flush=True) 
            
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    system = System()

    a = system.spawn(pong)
    b = system.spawn(ping, (a,))
    time.sleep(10)
    system.exit_all("because")
    # system.send(Shutdown, a)
    # system.send(Shutdown, b)

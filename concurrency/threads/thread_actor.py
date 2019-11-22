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
    Optional
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

    def shutdown(self, target, sender=None, priority=Priority.NORMAL):
        actor = self._actors[target]
        if actor.is_alive():
            actor.send(
                message=Shutdown,
                sender=sender,
                priority=priority
            )
        else:
            # raise, log
            ...

    def shutdown_all(self, priority=Priority.NORMAL):
        print("Initiating shutdown of all actors")
        for ref, actor in self._actors.items():
            print(f"Shutting down actor {ref}")
            self.shutdown(ref, None, priority)
            

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


class Timeout(NamedTuple):
    after: int

    def __call__(self, x):
        return False
    

class Match(NamedTuple):
    pred: Callable[..., bool]

    def __call__(self, x):
        return self.pred(x)
    
match = Match

class Mailbox:
    def __init__(self, q):
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
            print("Found matching message in storage for match key {key}")
            return message, key
        else:
            timeout_key, timeout = next((
                (key, cond.after)
                for key, cond in expectations
                if isinstance(cond, Timeout)
            ), (None, None))
            for m in iter_queue(self.queue, timeout=timeout):
                print("Reading new message from queue: ", m)
                match_key = next(poll(expectations, m), None)
                if match_key is not None:
                    print(f"Found matching message for match key {match_key}")
                    return m, match_key
                else:
                    print(f"Message {m} not expected, storing and skipping.")
                    self.storage.append(m)
            else:
                print("Timeout on select.")
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
        self.system = system
        self.actor_id = actor_id
        self.init = init
        self.init_args = init_args
        self.poll_timeout = poll_timeout
        self.dispatcher = None
        self.mailbox = Mailbox(queue.SimpleQueue())

    @property
    def ref(self):
        return ThreadActorRef(self.actor_id, self.ident, self.name)

    def receive(self, expectations):
        return self.mailbox.select(expectations)

    def run(self):
        self.init(self, *self.init_args)

    def send(self, message, sender, priority=Priority.NORMAL):
        if not self.is_alive():
            raise ActorTerminated(self)
        else:
            self.mailbox.send(Message(priority=priority, sender=sender, data=message))



def pong(self):
    while True: 
        message, key = self.receive([ 
            ("ping", Match(lambda m: m.data == "ping")),
            ("shutdown", Match(lambda m: m.data is Shutdown)), 
            ("timeout", Timeout(5))
        ])
        if key == "ping":
            print("Received ping")
            sender = message.sender
            if sender is not None:
                self.system.send("pong", message.sender, self.ref)
                self.system.send(Shutdown, message.sender, self.ref)
        elif key == "shutdown":
            print("okay, goodbye") 
            break
        elif key == "timeout": 
            print("Took a bit too long") 


def ping(self, server):
    self.system.send("ping", server, self.ref)
    while True:
        message, key = self.receive([
            ("reply", Match(lambda m: m.data == "pong")),
            ("shutdown", Match(lambda m: m.data is Shutdown)), 
            ("timeout", Timeout(5))
        ])
        if key == "reply":
            print(f"Received reply to ping from {message.sender}")
            self.system.send(Shutdown, server, self.ref)
        elif key == "shutdown": 
            print("okay, goodbye") 
            break
        elif key == "timeout": 
            print("Took a bit too long") 
            
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    system = System()

    a = system.spawn(pong)
    b = system.spawn(ping, (a,))
    # system.send(Shutdown, a)
    # system.send(Shutdown, b)

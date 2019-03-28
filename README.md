# concurrency-utils
Utilities for concurrent programming in python

Mostly intended to work with and build on top of standard python modules(like asyncio, threading, concurrent.futures, ...).


## Schedule
Inspired by [Zio](https://scalaz.github.io/scalaz-zio/datatypes/schedule.html).

A schedule is a stateful computation that can be used to describe a pattern of recurrence for a repeatable task.
Examples of application include retrying a task on transient errors, or repeating a task.

## TaskManager and TaskGroup
From task_manager package.
Taken directly from Curio's TaskGroup, Trio's nursery, and other similar constructs, and ported to asyncio.

## Thread utilities
Some utilities and abstractions for working with thread.
* threads.thread_scope: similar construct to TaskGroup/nursery, but for threads.
* threads.worker: a framework/interface to implement interruptible worker threads using generator coroutines. 
  Inspired by the "Threaded Target" chapter of http://www.dabeaz.com/coroutines/Coroutines.pdf .
  
## Channel
Inspired by [CSP](https://en.wikipedia.org/wiki/Communicating_sequential_processes) and go.
Both an asyncio(non-threadsafe) and blocking(threadsafe) implementation with similar interfaces will exist.

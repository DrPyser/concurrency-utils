# concurrency-utils
Utilities for concurrent programming in python

Mostly intended to work with and build on top of standard python modules(like asyncio, threading, concurrent.futures, ...).


## Schedule
Inspired by [Zio](https://scalaz.github.io/scalaz-zio/datatypes/schedule.html).

A schedule is a stateful computation that can be used to describe a pattern of recurrence for a repeatable task.
Examples of application include retrying a task on transient errors, or repeating a task.

## TaskManager and TaskGroup
Taken directly from Curio's TaskGroup, Trio's nursery, and other similar constructs, and ported to asyncio.

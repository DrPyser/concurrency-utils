import threading

POLL_TIMEOUT = 0.01


def spawn(f, *args, **kwargs):
    t = threading.Thread(target=f, args=args, kwargs=kwargs)
    t.start()
    return t

class ThreadScopeErrors(Exception):
    def __init__(self, exceptions, scope):
        self.exceptions = exceptions
        self.scope = scope
        

class ThreadScope:
    def __init__(self, threads=(), poll_timeout=POLL_TIMEOUT):
        self._threads = set(threads)
        self._poll_timeout = poll_timeout

    def spawn(self, f, *args, **kwargs):
        t = spawn(f, *args, **kwargs)
        self._threads.add(
            t
        )
        return t

    def join_all(self):
        alive = set(
            t
            for t in self._threads
            if t.is_alive()
        )
        exceptions = []
        while alive:
            dead = set()
            for t in alive:
                try:
                    t.join(self._poll_timeout)
                except Exception as ex:
                    exceptions.append(ex)
                else:
                    if not t.is_alive():
                        dead.add(t)
            else:
                alive.difference_update(dead)
        else:
            if exceptions:
                raise ThreadScopeErrors(exceptions, self)

    def __enter__(self):
        for t in self._threads:
            if not t.is_alive():
                try:
                    t.start()
                except RuntimeError:
                    pass
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.join_all()

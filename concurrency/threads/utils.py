import threading


POLL_TIMEOUT = 0.01


def spawn(f, *args, **kwargs):
    t = threading.Thread(target=f, args=args, kwargs=kwargs)
    t.start()
    return t

import os

def singleton(cls, *args, **kw):
    """
    Per-process singleton decorator.

    Caches one instance per class per OS process (os.getpid()), so each worker
    process in a daemon (producer, consumer, Spark executor) gets its own instance.
    """
    instances = {}

    def _singleton():
        key = str(cls) + str(os.getpid())
        if key not in instances:
            instances[key] = cls(*args, **kw)
        return instances[key]
    return _singleton
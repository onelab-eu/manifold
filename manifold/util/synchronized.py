# http://blog.dscpl.com.au/2014/01/the-synchronized-decorator-as-context.html
# http://blog.dscpl.com.au/2014/01/the-missing-synchronized-decorator.html


def synchronized(wrapped): 
    def _synchronized_lock(owner):
        lock = vars(owner).get('_synchronized_lock', None) 
        if lock is None:
            meta_lock = vars(synchronized).setdefault(
                    '_synchronized_meta_lock', threading.Lock()) 
            with meta_lock:
                lock = vars(owner).get('_synchronized_lock', None)
                if lock is None:
                    lock = threading.RLock()
                    setattr(owner, '_synchronized_lock', lock) 
        return lock 
    def _synchronized_wrapper(wrapped, instance, args, kwargs):
        with _synchronized_lock(instance or wrapped):
            return wrapped(*args, **kwargs) 
    class _synchronized_function_wrapper(function_wrapper): 
        def __enter__(self):
            self._lock = _synchronized_lock(self.wrapped)
            self._lock.acquire()
            return self._lock 
        def __exit__(self, *args):
            self._lock.release() 
    return _synchronized_function_wrapper(wrapped, _synchronized_wrapper)

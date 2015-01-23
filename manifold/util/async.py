from twisted.internet                   import defer
from manifold.util.reactor_thread       import ReactorThread

def async_sleep(secs):
    d = defer.Deferred()
    ReactorThread().callLater(secs, d.callback, None)
    return d

@defer.inlineCallbacks
def async_wait(fun, interval = 1):
    while not fun():
        yield async_sleep(interval)

from manifold.operators      import LAST_RECORD
from manifold.util.log       import Log
import threading

#------------------------------------------------------------------
# Class callback
#------------------------------------------------------------------

class Callback:
    def __init__(self, deferred=None, router=None, cache_id=None):
    #def __init__(self, deferred=None, event=None, router=None, cache_id=None):
        self.results = []
        self._deferred = deferred

        #if not self.event:
        self.event = threading.Event()
        #else:
        #    self.event = event

        # Used for caching...
        self.router = router
        self.cache_id = cache_id

    def __call__(self, value):
        Log.tmp("call: ",value)
        if value == LAST_RECORD:
            if self.cache_id:
                # Add query results to cache (expires in 30min)
                #print "Result added to cached under id", self.cache_id
                self.router.cache[self.cache_id] = (self.results, time.time() + CACHE_LIFETIME)

            if self._deferred:
                self._deferred.callback(self.results)
            else:
                self.event.set()
            return self.event

        self.results.append(value)

    def wait(self):
        self.event.wait()
        self.event.clear()

    def get_results(self):
        self.wait()
        return self.results
        

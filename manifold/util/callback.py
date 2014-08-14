import threading

#------------------------------------------------------------------
# Class callback
#------------------------------------------------------------------

class Callback:
    def __init__(self, deferred=None, router=None, cache_id=None, store_empty=False):
        self.results = []
        self._deferred = deferred

        #if not self.event:
        self.event = threading.Event()
        #else:
        #    self.event = event

        # Used for caching...
        self.router = router
        self.cache_id = cache_id
        self.store_empty = store_empty

    def __call__(self, record):
        # End of the list of records sent by Gateway
        # XXX routerv2 : add last record to results

        if self.store_empty or not record.is_last():
            # In routerv2, just avoid empty record
            self.results.append(record)

        if record.is_last():
            if self.cache_id:
                # Add query results to cache (expires in 30min)
                self.router.cache[self.cache_id] = (self.results, time.time() + CACHE_LIFETIME)

            if self._deferred:
                # Send results back using deferred object
                self._deferred.callback(self.results)
            else:
                # Not using deferred, trigger the event to return results
                self.event.set()
            return self.event


    def wait(self):
        self.event.wait()
        self.event.clear()

    def get_results(self):
        self.wait()
        return self.results
        

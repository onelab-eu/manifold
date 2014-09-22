
# XXX We need to keep track of clients to propagate announces

class Interface(object):
    def __init__(self, router):
        self._router = router

    def terminate(self):
        pass


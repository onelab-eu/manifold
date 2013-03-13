class Capabilities(object):
    
    KEYS = ['selection', 'projection', 'sort', 'limit', 'offset']

    def __init__(self, *args, **kwargs):
        for key in self.KEYS:
             object.__setattr__(self, key, False)

    def __setattr__(self, key, value):
        assert key in self.KEYS, "Unknown capability '%s'" % key
        assert isinstance(value, bool)
        object.__setattr__(self, key, value)

    def __getattr__(self, key):
        assert key in self.KEYS, "Unknown capability '%s'" % key
        object.__getattr__(self, key)

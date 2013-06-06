class Capabilities(object):
    
    KEYS = ['retrieve', 'join', 'selection', 'projection', 'sort', 'limit', 'offset', 'fullquery']

    def __init__(self, *args, **kwargs):
        for key in self.KEYS:
             object.__setattr__(self, key, False)

    def __deepcopy__(self, memo):
        capabilities = Capabilities()
        for key in self.KEYS:
            setattr(capabilities, key, getattr(self, key))
        return capabilities

    def __setattr__(self, key, value):
        assert key in self.KEYS, "Unknown capability '%s'" % key
        assert isinstance(value, bool)
        object.__setattr__(self, key, value)

    def __getattr__(self, key):
        assert key in self.KEYS, "Unknown capability '%s'" % key
        object.__getattr__(self, key)

    def __str__(self):
        list_capabilities = map(lambda x: x if getattr(self, x, False) else '', self.KEYS)
        list_capabilities = ', '.join([x for x in self.KEYS if getattr(self, x, False)])
        return '<Capabilities: %s>' % list_capabilities

    def __repr__(self):
        return self.__str__()

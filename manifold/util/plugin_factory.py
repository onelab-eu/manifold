#-------------------------------------------------------------------------
# Class Singleton
#
# Classes that inherit from Singleton can be instanciated only once 
#-------------------------------------------------------------------------

class PluginFactory(type):
    def __init__(cls, name, bases, dic):
        #super(PluginFactory, cls).__init__(name, bases, dic)
        type.__init__(cls, name, bases, dic)

        try:
            registry = getattr(cls, 'registry')
        except AttributeError:
            setattr(cls, 'registry', {})
            registry = getattr(cls, 'registry')
        if name != "Gateway":
            if name.endswith('Gateway'):
                name = name[:-7]
            name = name.lower()
            registry[name] = cls

        def get(self, name):
            return registry[name]

        # Adding a class method get to retrieve plugins by name
        setattr(cls, 'get', classmethod(get))

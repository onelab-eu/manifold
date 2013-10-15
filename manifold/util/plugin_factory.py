from manifold.util.log             import Log

class PluginFactory(type):
    def __init__(cls, name, bases, dic):
        #super(PluginFactory, cls).__init__(name, bases, dic)
        type.__init__(cls, name, bases, dic)

        try:
            registry = getattr(cls, 'registry')
        except AttributeError:
            setattr(cls, 'registry', {})
            registry = getattr(cls, 'registry')
        # XXX
        if name != "Gateway":
            if name.endswith('Gateway'):
                name = name[:-7]
            name = name.lower()
            registry[name] = cls

        def get(self, name):
            """
            (For the moment PluginFactory is only used to register Manifold gateways)
            Retrieve a registered Gateway according to a given name.
            Args:
                name: The name of the Gateway (gateway_type in the Storage).
                    Example: pass "foo" to retrieve FooGateway.
            Returns:
                The corresponding Gateway.
            """
            registered_name = name.lower()
            try:
                return registry[registered_name]
            except KeyError:
                Log.error("Cannot find %s in {%s}" % (registered_name, ', '.join(registry.keys())))

        # Adding a class method get to retrieve plugins by name
        setattr(cls, 'get', classmethod(get))

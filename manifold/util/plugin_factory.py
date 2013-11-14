import pkgutil
from manifold.util.log import Log

PROPERTY_NAME = '__plugin_factory_registry__'
METAPROPERTY  = '__plugin__name__attribute__'

class PluginFactory(type):
    def __init__(cls, class_name, parents, attrs):
        """
        upperattr_metaclass
        future_class_name, 
        future_class_parents
        future_class_attr
        """
        type.__init__(cls, class_name, parents, attrs)

        # Be sure the property storing the mapping exists
        try:
            registry = getattr(cls, PROPERTY_NAME)
        except AttributeError:
            setattr(cls, PROPERTY_NAME, {})
            registry = getattr(cls, PROPERTY_NAME)

        if not METAPROPERTY in attrs:
            # We are not in the base class: register plugin
            plugin_name_attribute = cls.get_plugin_name_attribute()
            plugin_names = attrs.get(plugin_name_attribute, [])

            if not isinstance(plugin_names, list):
                plugin_names = [plugin_names]
                
            for plugin_name in plugin_names:
                registry[plugin_name] = cls #class_name

        else:
            # We are in the base class
            cls.plugin_name_attribute = attrs[METAPROPERTY]

            # Adding a class method get to retrieve plugins by name
            def get(self, name):
                """
                Retrieve a registered Gateway according to a given name.
                Args:
                    name: The name of the Gateway (gateway_type in the Storage).
                Returns:
                    The corresponding Gateway.
                """
                try:
                    return registry[name]
                except KeyError:
                    Log.error("Cannot find %s in {%s}" % (name, ', '.join(registry.keys())))

            setattr(cls, 'get', classmethod(get))
            setattr(cls, 'list', classmethod(lambda self: registry))
            setattr(cls, 'get_plugin_name_attribute', classmethod(lambda self: self.plugin_name_attribute))
        
        #return super(PluginFactory, cls).__new__(cls, class_name, parents, attrs)

    @staticmethod
    def register(package):
        prefix = package.__name__ + "."
        for importer, modname, ispkg in pkgutil.iter_modules(package.__path__, prefix):
            try:
                module = __import__(modname, fromlist="dummy")
            except Exception, e:
                Log.info("Could not load %s : %s" % (modname, e))

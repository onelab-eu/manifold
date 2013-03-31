import os
from manifold.metadata.Metadata         import import_file_h
#from manifold.metadata.MetadataClass    import MetadataClass
from manifold.core.table                import Table
from manifold.core.capabilities         import Capabilities

STATIC_ROUTES_FILE = "/usr/share/myslice/metadata/"

class Announce(object):
    def __init__(self, table, capabilities, cost=None):
        """
        \brief Constructor
        """
        assert isinstance(capabilities, Capabilities), "Wrong capabilities argument"

        self.table = table
        self.capabilities = capabilities
        self.cost = cost

class Announces(object):

    @classmethod
    def from_dot_h(self, platform_name, gateway_type):
        return self.import_file_h(STATIC_ROUTES_FILE, platform_name, gateway_type)

    @classmethod
    def import_file_h(self, directory, platform, gateway_type):
        """
        \brief Import a .h file (see manifold.metadata/*.h)
        \param directory The directory storing the .h files
            Example: router.conf.STATIC_ROUTES_FILE = "/usr/share/myslice/metadata/"
        \param platform The name of the platform we are configuring
            Examples: "ple", "senslab", "tophat", "omf", ...
        \param gateway_types The type of the gateway
            Examples: "SFA", "XMLRPC", "MaxMind"
            See:
                sqlite3 /var/myslice/db.sqlite
                > select gateway_type from platform;
        """
        # Check path
        filename = os.path.join(directory, "%s.h" % gateway_type)
        if not os.path.exists(filename):
            filename = os.path.join(directory, "%s-%s.h" % (gateway_type, platform))
            if not os.path.exists(filename):
                raise Exception, "Metadata file '%s' not found (platform = %r, gateway_type = %r)" % (filename, platform, gateway_type)

        # Read input file
        #print "I: Platform %s: Processing %s" % (platform, filename)
        (classes, enums) = import_file_h(filename)

        # Check class consistency
        for cur_class_name, cur_class in classes.items():
            invalid_keys = cur_class.get_invalid_keys()
            if invalid_keys:
                raise ValueError("In %s: in class %r: key(s) not found: %r" % (filename, cur_class_name, invalid_keys))

        # Rq: We cannot check type consistency while a table might refer to types provided by another file.
        # Thus we can't use get_invalid_types yet

        announces = []
        for cur_class_name, cur_class in classes.items():
            # t = Table(platform, None, cur_class_name, cur_class.fields, cur_class.keys) # None = methods
            # self.rib[t] = platform

            #mc = MetadataClass('class', cur_class_name)
            #mc.fields = set(cur_class.fields) # XXX set should be mandatory
            #mc.keys = cur_class.keys
            t = Table(platform, None, cur_class_name, cur_class.fields, cur_class.keys) # XXX qualifier ?

            cap = Capabilities()

            announce = Announce(t, cap)
            announces.append(announce)
        return announces

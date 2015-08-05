"""
MANIFOLD
Licensed under GLPv3
"""
# Please keep __version__ in sync with %{version} in manifold.spec
# (This version is currently automatically handled by the RPM package script)
__version__ = (2, 0, 141)

# META

from manifold.core.object import ObjectFactory
from manifold.core.announce import Announces

BASE="manifold"

GIT_ANNOUNCE="""
class git {
    const string url;
    const string log;

    CAPABILITY(join);
    KEY(url);
};
"""

import imp
import sys

class ManifoldImporter(object):
    def find_module(self, fullname, path=None):
        if fullname == BASE:
            return self
        if fullname.rsplit('.')[0] != BASE:
            return None

        if True: # object exists
            return self
        else:
            return None

    def load_module(self, fullname):
        if fullname in sys.modules:
            return sys.modules[fullname]
        mod = imp.new_module(fullname)
        mod.__loader__ = self
        sys.modules[fullname] = mod
        if fullname != 'manifold':
            # Here we create the specific class
            mod.__file__ = 'manifold://XXX'
            code = "POUET"
            announce = Announces.from_string(GIT_ANNOUNCE)
            mod.__dict__['Git'] = ObjectFactory('Git').from_announce(announce[0])
        else:
            mod.__file__ = "[fake module %r]" % fullname
            mod.__path__ = []
        return mod

def initialize_meta():
    sys.meta_path = [ManifoldImporter()]

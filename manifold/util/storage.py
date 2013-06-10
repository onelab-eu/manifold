from manifold.gateways      import Gateway
from manifold.util.callback import Callback

class Storage(object):
    pass
    # We can read information from files, database, commandline, etc
    # Let's focus on the database

    @classmethod
    def register(self, object):
        """
        Registers a new object that will be stored locally by manifold.
        This will live in the 
        """ 
        pass

class DBStorage(Storage):
    @classmethod
    def execute(self, query, user=None, format='dict'):
        # XXX Need to pass local parameters
        gw = Gateway.get('sqlalchemy')(user=user, format=format)
        gw.set_query(query)
        cb = Callback()
        gw.set_callback(cb)
        gw.start()
        return cb.get_results()

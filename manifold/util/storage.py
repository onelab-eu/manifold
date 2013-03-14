from manifold.gateways      import Gateway
from manifold.util.callback import Callback

class Storage(object):
    pass
    # We can read information from files, database, commandline, etc
    # Let's focus on the database


class DBStorage(Storage):
    @classmethod
    def execute(self, query):
        # XXX Need to pass local parameters
        gw = Gateway.get('sqlalchemy')(format='object')
        gw.set_query(query)
        cb = Callback()
        gw.set_callback(cb)
        gw.start()
        return cb.get_results()

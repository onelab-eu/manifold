from manifold.gateways      import Gateway
from manifold.util.callback import Callback
from manifold.util.options  import Options
from manifold.util.log      import Log

#URL='sqlite:///:memory:?check_same_thread=False'
#URL='sqlite:////var/myslice/db.sqlite?check_same_thread=False'
#URL='mysql://root:onelab@localhost/manifold'

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

    @staticmethod
    def init_options():
        opt = Options()
        opt.add_argument(
            "-Se", "--storage-engine", dest = "storage_engine",
            help = "Engine of the local storage",
            default = None
        )
        opt.add_argument(
            "-Sl", "--storage-login", dest = "storage_login",
            help = "Login of the local storage",
            default = None
        )
        opt.add_argument(
            "-Sp", "--storage-password", dest = "storage_password",
            help = "Password of the local storage",
            default = None
        )
        opt.add_argument(
            "-SH", "--storage-host", dest = "storage_host",
            help = "Host of the local storage",
            default = None
        )
        opt.add_argument(
            "-SP", "--storage-port", dest = "storage_port",
            help = "Port of the local storage",
            default = None
        )
        opt.add_argument(
            "-Sd", "--storage-database", dest = "storage_database",
            help = "Database of the local storage",
            default = None
        )

    @classmethod
    def execute(cls, query, user=None, format='record'):
        # XXX Need to pass local parameters
        URL = DBStorage.get_url()
        gw = Gateway.get('sqlalchemy')(config={'url': URL}, user=user, format=format)
        gw.set_query(query)
        cb = Callback()
        gw.set_callback(cb)
        gw.start()
        return cb.get_results()

    @staticmethod
    def get_url():
        url = ''
        options  = Options()
        engine   = Options().storage_engine
        login    = Options().storage_login
        password = Options().storage_password
        host     = Options().storage_host
        port     = Options().storage_port
        database = Options().storage_database
        if not database:
            raise RuntimeError("check storage_database in /etc/manifold/manifold.conf")

        if not engine:
            engine = 'sqlite' 

        if engine=='sqlite' or engine=='sqlite3':
            url = engine + ':///'
            url += database + '?check_same_thread=False'
            #URL='sqlite:////var/myslice/db.sqlite?check_same_thread=False'
        else:
            url = engine + '://'
            if not login:
                raise RuntimeError("check storage_login in /etc/manifold/manifold.conf")
            if not password:
                raise RuntimeError("check storage_password in /etc/manifold/manifold.conf")
            if not host:
                raise RuntimeError("check storage_host in /etc/manifold/manifold.conf")
            url += login + ':' + password + '@' + host 
            if port:
               url += ':'+port
            url = '/' + database 
            #URL='mysql://root:onelab@localhost/manifold'

        return url

#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Shell allows to read Query from the standard input and
# forwards it to a Manifold Interface.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import os, sys, pprint, json
from socket                         import gethostname
from optparse                       import OptionParser
from getpass                        import getpass
from traceback                      import format_exc

from twisted.internet               import defer, ssl
from twisted.web                    import xmlrpc

# XXX Those imports may fail for xmlrpc calls
from manifold.auth                  import Auth
from manifold.core.annotation       import Annotation 
from manifold.core.query            import Query
from manifold.core.router           import Router
from manifold.core.receiver         import Receiver 
from manifold.core.result_value     import ResultValue
from manifold.input.sql             import SQLParser
from manifold.util.log              import Log
from manifold.util.options          import Options
from manifold.util.reactor_thread   import ReactorThread
from manifold.util.type             import accepts, returns

# This could be moved outside of the Shell
DEFAULT_USER      = 'demo'
DEFAULT_PASSWORD  = 'demo'
DEFAULT_PKEY_FILE = '/etc/manifold/keys/client.pkey' 
DEFAULT_CERT_FILE = '/etc/manifold/keys/client.cert'

class ManifoldClient(Receiver):
    def log_info(self): pass
    def whoami(self): return None

class ManifoldLocalClient(ManifoldClient):
    def __init__(self, user_email = None):
        self.interface = Router()
        self.interface.__enter__()

        try:
            users = self.interface.execute_local_query(
                Query.get('local:user').filter_by('email', '==', user_email)
            )
        except:
            users = list()

        if not len(users) >= 1:
            Log.warning('Could not retrieve current user... going anonymous')
            self.user = None
        else:
            self.user = users[0]
            if "config" in self.user and self.user["config"]:
                self.user["config"] = json.loads(self.user["config"])
            else:
                self.user["config"] = None

    def __del__(self):
        try:
            if self.interface:
                self.interface.__exit__()
            self.interface = None
        except: pass

    def forward(self, query, annotation = None):
        if not annotation:
            annotation = Annotation() 
        if not "user" in annotation.keys():
            annotation["user"] = self.user
        self.interface.forward(query, annotation, self)

    def log_info(self):
        Log.info("Shell using local account %s" % self.user["email"])

    def whoami(self):
        return self.user

class ManifoldXMLRPCClientXMLRPCLIB(ManifoldClient):
    # on ne sait pas si c'est secure ou non

    def __init__(self):
        import xmlrpclib
        url = Options().xmlrpc_url
        self.interface = xmlrpclib.ServerProxy(url, allow_none=True)
        self.auth = None

    def forward(self, query, annotation = None):
        if not annotation:
            annotation = Annotation() 
        if not "authentication" in annotation.keys():
            annotation["authentication"] = self.auth
        self.interface.forward(query.to_dict(), annotation, self)

    # mode_str      = 'XMLRPC'
    # interface_str = ' towards XMLRPC API %s' % self.interface

class Proxy(xmlrpc.Proxy):
    def __str__(self):
        return "<XMLRPC client to %s>" % self.url

    def __init__(self, url, user=None, password=None, allowNone=False, useDateTime=False, connectTimeout=30.0, reactor=None): # XXX
        import threading
        xmlrpc.Proxy.__init__(self, url, user, password, allowNone, useDateTime, connectTimeout, reactor)
        self.url = url
        self.event = threading.Event() 
        self.result = None
        self.error = None

    def setSSLClientContext(self,SSLClientContext):
        self.SSLClientContext = SSLClientContext

    def callRemote(self, method, *args):
        def cancel(d):
            factory.deferred = None
            connector.disconnect()
        factory = self.queryFactory(
            self.path, self.host, method, self.user,
            self.password, self.allowNone, args, cancel, self.useDateTime)
        #factory = xmlrpc._QueryFactory(
        #    self.path, self.host, method, self.user,
        #    self.password, self.allowNone, args)

        if self.secure:
            try:
                self.SSLClientContext
            except NameError:
                print "Must Set a SSL Context"
                print "use self.setSSLClientContext() first"
                # Its very bad to connect to ssl without some kind of
                # verification of who your talking to.
                # Using the default sslcontext without verification
                # Can lead to man in the middle attacks
            ReactorThread().connectSSL(self.host, self.port or 443,
                               factory, self.SSLClientContext,
                               timeout=self.connectTimeout)

        else:
           ReactorThread().connectTCP(self.host, self.port or 80, factory, timeout=self.connectTimeout)
        return factory.deferred

    def __getattr__(self, name):
        # We transfer missing methods to the remote server
        def _missing(*args):
            from twisted.internet import defer
            d = defer.Deferred()
            
            def proxy_success_cb(result):
                self.result = result
                self.event.set()
            def proxy_error_cb(error):
                self.error = error
                self.event.set()
            
            #@defer.inlineCallbacks
            def wrap(source, args):
                args = (name,) + args
                self.callRemote(*args).addCallbacks(proxy_success_cb, proxy_error_cb)
            
            ReactorThread().callInReactor(wrap, self, args)
            self.event.wait()
            self.event.clear()
            if self.error:
                Log.error("ERROR IN PROXY: %s" % self.error)
                self.error = None
                return None
            else:
                result = self.result
                self.result = None
                return result
        return _missing

class ManifoldXMLRPCClient(ManifoldClient):
    def __init__(self, url):
        self.url = url
        ReactorThread().start_reactor()

    def __del__(self):
        ReactorThread().stop_reactor()

    def forward(self, query, annotation = None):
        if not annotation:
            annotation = Annotation() 
        annotation.update(self.annotation)
        self.interface.forward(query.to_dict(), annotation, self)
        

    @defer.inlineCallbacks
    def whoami(self, query, annotation = None):
        Log.tmp("TBD")
        #if not annotation:
        #    annotation = {}
        #annotation.update(self.annotation)
        #ret = yield self.interface.AuthCheck(annotation)
        #defer.returnValue(ret)

class ManifoldXMLRPCClientSSLPassword(ManifoldXMLRPCClient):
    
    def __init__(self, url, user_email=None, password=None):
        ManifoldXMLRPCClient.__init__(self, url)
        self.user_email = user_email

        if user_email:
            self.annotation = { 'authentication': {'AuthMethod': 'password', 'Username': user_email, 'AuthString': password} }
        else:
            self.annotation = { 'authentication': {'AuthMethod': 'anonymous'} } 

        self.interface = Proxy(self.url, allowNone=True, useDateTime=False)
        self.interface.setSSLClientContext(ssl.ClientContextFactory())

    def log_info(self):
        Log.info("Shell using XMLRPC account '%r' (password) on %s" % (self.user_email, self.url))

class ManifoldXMLRPCClientSSLGID(ManifoldXMLRPCClient):
    
    ## We need this to define the private key and certificate (GID) to use
    #class CtxFactory(ssl.ClientContextFactory):
    #    def getContext(self):
    #        self.method = SSL.SSLv23_METHOD
    #        ctx = ssl.ClientContextFactory.getContext(self)
    #        ctx.use_certificate_chain_file(self.cert_file)
    #        ctx.use_privatekey_file(self.pkey_file)
    #        return ctx

    def __init__(self, url, pkey_file, cert_file):
        self.url = url
        self.gid_subject = 'NULL'
        self.interface = Proxy(self.url, allowNone=True, useDateTime=False)
        #self.interface.setSSLClientContext(CtxFactory(pkey_file, cert_file))

        self.annotation = { 'authentication': {'AuthMethod': 'gid'} } 

        # This has to be tested to get rid of the previously defined CtxFactory class
        self.interface.setSSLClientContext(ssl.DefaultOpenSSLContextFactory(pkey_file, cert_file))

    def log_info(self):
        Log.info("Shell using XMLRPC account '%r' (GID) on %s" % (self.gid_subject, self.url))

class Shell(object):

    PROMPT = 'manifold'

    @classmethod
    def print_error(self, result_value):
        """
        Print a ResultValue to the standard output.
        Args:
            result_value: A ResultValue instance.
        """
        print "-" * 80
        print "Exception %(code)s raised by %(origin)s %(description)s" % {
            "code"        : result_value["code"],
            "origin"      : result_value["origin"],
            "description" : ": %s" % result_value["description"] if result_value["description"] else ""
        }
        if result_value["traceback"]:
            for line in result_value["traceback"].split("\n"):
                Log.error("\t", line)
        else:
            Log.error("(Traceback not set)")
        print ""

    @classmethod
    def init_options(self):
        """
        Prepare options supported by a Shell.
        """
        opt = Options()
        opt.add_argument(
            "-C", "--cacert", dest = "xmlrpc_cacert",
            help = "API SSL certificate", 
            default = None
        )
        opt.add_argument(
            "-K", "--insecure", dest = "xmlrpc_insecure",
            help = "Do not check SSL certificate", 
            default = 7080
        )
        opt.add_argument(
            "-U", "--url", dest = "xmlrpc_url",
            help = "API URL", 
            default = 'https://localhost:7080'
        )
        opt.add_argument(
            "-u", "--username", dest = "user_email",
            help = "API user name", 
            default = DEFAULT_USER
        )
        opt.add_argument(
            "-p", "--password", dest = "password",
            help = "API password", 
            default = DEFAULT_PASSWORD
        )
        opt.add_argument(
            "-k", "--private-key", dest = "pkey_file",
            help = "Private key file to use for the SSL connection",
            default = DEFAULT_PKEY_FILE
        )
        opt.add_argument(
            "-g", "--cert-file", dest = "cert_file",
            help = "Certificate (chain) file to use for the SSL connection (= SFA GID)",
            default = DEFAULT_CERT_FILE
        )
        opt.add_argument(
            "-x", "--xmlrpc", action="store_true", dest = "xmlrpc",
            help = "Use XML-RPC interface", 
            default = False
        )
        opt.add_argument(
            "-z", "--auth-method", dest = "auth_method",
            help    = 'Choice of the authentication method: auto, anonymous, password, gid',
            default = 'auto'
        )
        opt.add_argument(
            "-e", "--execute", dest = "execute",
            help = "Execute a shell command", 
            default = None
        )
        #parser.add_argument("-m", "--method", help = "API authentication method")
        #parser.add_argument("-s", "--session", help = "API session key")

#XXX#<<<<<<< HEAD
#XXX#    def __init__(self, interactive = False):
#XXX#        """
#XXX#        Constructor.
#XXX#        Args:
#XXX#            interactive: A boolean.
#XXX#        """
#XXX#        super(Shell, self).__init__()
#XXX#        self.interactive = interactive
#XXX#
#XXX#        if not Options().anonymous:
#XXX#            # If user is specified but password is not
#XXX#=======
    def select_auth_method(self, auth_method):
        if auth_method == 'auto':
            for method in ['local', 'gid', 'password']:
                try:
                    Log.debug("Trying client authentication '%s'" % method)
                    self.select_auth_method(method)
                    return
                except Exception, e:
                    Log.error(format_exc())
                    Log.debug("Failed client authentication '%s': %s" % (method, e))
            raise Exception, "Could not authentication automatically (tried: local, gid, password)"

        elif auth_method == 'local':
#XXX#>>>>>>> devel
            user_email = Options().user_email
            
            self.client = ManifoldLocalClient(user_email)

        else: # XMLRPC 
            url = Options().xmlrpc_url
#XXX#            self.interface = xmlrpclib.ServerProxy(url, allow_none = True)

            if auth_method == 'gid':
                pkey_file = Options().pkey_file
                cert_file = Options().cert_file

                self.client = ManifoldXMLRPCClientSSLGID(url, pkey_file, cert_file)

            elif auth_method == 'password':
                # If user is specified but password is not
                user_email = Options().user_email
                password = Options().password

                if user_email != DEFAULT_USER and password == DEFAULT_PASSWORD:
                    if Options().interactive:
                        try:
                            _password = getpass("Enter password for '%s' (or ENTER to keep default):" % user_email)
                        except (EOFError, KeyboardInterrupt):
                            print
                            sys.exit(0)
                        if _password:
                            password = _password
                    else:
                        Log.warning("No password specified, using default.")

                self.client = ManifoldXMLRPCClientSSLPassword(url, user_email, password)

            elif auth_method == 'anonymous':

                self.client = ManifoldXMLRPCClientSSLPassword(url)

            else:
                raise Exception, "Authentication method not supported: '%s'" % auth_method


    def __init__(self, interactive=False):
        self.interactive = interactive
        
        auth_method = Options().auth_method
        if not auth_method: auth_method = "local"

        self.select_auth_method(auth_method)
        if self.interactive:
            self.client.log_info()
        self.whoami()

    def terminate(self):
        # XXX Issues with the reference counter
        #del self.client
        #self.client = None
        try:
            self.client.__del__()
        except: pass

    @returns(ResultValue)
    def get_result_value(self):
        """
        Returns:
            The ResultValue corresponding to the last Query executed
            on this Shell.
        """
        return self.client.get_result_value()

    def display(self):
        """
        Print the ResultValue of a Query in the standard output.
        If this ResultValue carries error(s), those error(s) are recursively
        unested and printed to the standard output.
        """
        result_value = self.get_result_value()
        if not result_value:
            return

        assert isinstance(result_value, ResultValue), "Invalid ResultValue: %s (%s)" % (result_value, type(result_value))

        if not result_value.is_success():
            print ''
            print 'ERROR:'
            if isinstance(result_value.get_error_message(), list):
                # We have a list of errors
                for nested_result_value in result_value["description"]:
                    Shell.print_error(nested_result_value)
                    return
            else:
                print result_value.get_error_message()

        records = result_value["value"]
    
        if self.interactive:
            # Command-line
            print "===== RESULTS ====="
            pprint.pprint(records)
        elif Options().execute:
            # Used by script to it may be piped.
            print json.dumps(records)

    #@returns(list)
    def evaluate(self, command):
        """
        Parse a command type by the User, and run the corresponding Query.
        Args:
            command: A String instance containing the command typed by the user.
        Raises:
            Exception: In case of Failure.
        Returns:
            A list of Records corresponding to the Query deduced from the command.
        """

        # Prepare annotation
        annotation = Annotation({
            "user" : {
                "email"    : Options().user_email,
                "password" : Options().password
            }
        })

        # Prepare query
        dic = SQLParser().parse(command)
        if not dic:
            return None
        query = Query(dic)
        if "*" in query.get_select():
            query.fields = None

        # Run the query
        self.client.forward(query, annotation)
        result_value = self.get_result_value()

        if not result_value.is_success():
            raise Exception, "Error evaluating command: %s (%s)" % (command, result_value.get_error_message())

        return result_value["value"]

    def execute(self, query):
        """
        Execute a Query (used if the Shell is run with option "-e").
        Args:
            query: The Query typed by the user.
        """
        return self.client.forward(query)

    def whoami(self):
        # Who is authenticating ?
        # authentication
        return self.client.whoami
        
        
#    # If called by a script 
#    if args:
#        if not os.path.exists(args[0]):
#            print 'File %s not found'%args[0]
#            parser.print_help()
#            sys.exit(1)
#        else:
#            # re-append --help if provided
#            if options.help:
#                args.append('--help')
#            # use args as sys.argv for the next shell, so our own options get removed for the next script
#            sys.argv = args
#            script = sys.argv[0]
#            # Add of script to sys.path 
#            path = os.path.dirname(os.path.abspath(script))
#            sys.path.append(path)
#            execfile(script)

    def start(self):
        #if shell.server is None:
        #    print "PlanetLab Central Direct API Access"
        #    prompt = ""
        #elif shell.auth['AuthMethod'] == "anonymous":
        #    prompt = "[anonymous]"
        #    print "Connected anonymously"
        #elif shell.auth['AuthMethod'] == "session":
        #    # XXX No way to tell node and user sessions apart from the
        #    # client point of view.
        #    prompt = "[%s]" % gethostname()
        #    print "%s connected using session authentication" % gethostname()
        #else:
        #    prompt = "[%s]" % shell.auth['Username']
        #    print "%s connected using %s authentication" % \
        #          (shell.auth['Username'], shell.auth['AuthMethod'])

        # Readline and tab completion support
        import atexit
        import readline
        import rlcompleter

        #print 'Type "system.listMethods()" or "help(method)" for more information.'
        # Load command history
        history_path = os.path.join(os.environ["HOME"], ".plcapi_history")
        try:
            file(history_path, 'a').close()
            readline.read_history_file(history_path)
            atexit.register(readline.write_history_file, history_path)
        except IOError:
            pass

        # Enable tab completion
        readline.parse_and_bind("tab: complete")

        print "Welcome to MANIFOLD shell. Press ^C to clean up commandline, and ^D to exit."
        try:
            while True:
                command = ""
                while True:
                    # Get line
                    try:
                        if command == "":
                            sep = ">>> "
                        else:
                            sep = "... "
                        line = raw_input(self.PROMPT + sep)
                    # Ctrl-C
                    except KeyboardInterrupt:
                        command = ""
                        print
                        break

                    # Build up multi-line command
                    command += line

                    # Blank line or first line does not end in :
                    if line == "" or (command == line and line[-1] != ':'):
                        break

                    command += os.linesep

                # Blank line
                if command == "":
                    continue
                # Quit
                elif command in ["q", "quit", "exit"]:
                    break

                try:
                    self.evaluate(command)
                    self.display()
                except KeyboardInterrupt:
                    command = ""
                    print
                except Exception, err:
                    Log.error(format_exc())

        except EOFError:
            self.terminate()

def main():
    Shell.init_options()
    Log.init_options()
    Options().parse()
    command = Options().execute
    if command:
        shell = Shell(interactive = False)
        shell.evaluate(command)
        shell.display()
        #shell.terminate()
    else:
        Shell(interactive = True).start()

if __name__ == '__main__':
    main()

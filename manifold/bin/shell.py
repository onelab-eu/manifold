#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os, sys, pprint
from socket                import gethostname
from optparse              import OptionParser
from getpass               import getpass
from traceback             import print_exc

# XXX Those import may fail for xmlrpc calls
from manifold.core.query   import Query
from manifold.core.router  import Router
from manifold.util.log     import Log
from manifold.util.options import Options
from manifold.input.sql    import SQLParser
from manifold.auth         import Auth

from twisted.internet      import defer

import json

# This could be moved outside of the Shell
DEFAULT_USER     = 'demo'
DEFAULT_PASSWORD = 'demo'

class ManifoldClient(object):
    def log_info(self): pass
    def auth_check(self): pass

class ManifoldLocalClient(ManifoldClient):
    def __init__(self):
        self.interface = Router()
        self.interface.__enter__()
        self.auth = None
        self.user = None

    def __del__(self):
        if self.interface:
            self.interface.__exit__()
        self.interface = None

    def forward(self, query, annotations=None):
        if not annotations:
            annotations = {}
        annotations['user'] = self.user
        return self.interface.forward(query, annotations)

    def auth_check(self):
        pass

    def log_info(self):
        Log.info("Shell using local account '%r'" % self.user)

class ManifoldXMLRPCClientXMLRPCLIB(ManifoldClient):
    # on ne sait pas si c'est secure ou non

    def __init__(self):
        import xmlrpclib
        url = Options().xmlrpc_url
        self.interface = xmlrpclib.ServerProxy(url, allow_none=True)
        self.auth = None

    def forward(self, query, annotations=None):
        if not annotations:
            annotations = {}
        annotations['authentication'] = self.auth
        return self.interface.forward(query.to_dict(), annotations)

    # mode_str      = 'XMLRPC'
    # interface_str = ' towards XMLRPC API %s' % self.interface

class Proxy(object):
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
                # verfication of who your talking to
                # Using the default sslcontext without verification
                # Can lead to man in the middle attacks
            reactor.connectSSL(self.host, self.port or 443,
                               factory, self.SSLClientContext,
                               timeout=self.connectTimeout)

        else:
           reactor.connectTCP(self.host, self.port or 80, factory, timeout=self.connectTimeout)
        return factory.deferred

    def __getattr__(self, name):                                                                                
        def _missing(*args):
            from twisted.internet import defer
            d = defer.Deferred()
            
            success_cb = lambda result: d.callback(result)                                                      
            error_cb   = lambda error : d.errback(ValueError("Proxy %s" % error))                               
                                                                                                                
            @defer.inlineCallbacks
            def wrap(source, args):
                token = yield SFATokenMgr().get_token(self.interface)                                           
                args = (name,) + args                                                                           
                
                # XXX Should add annotations

                self.callRemote(*args).addCallbacks(proxy_success_cb, proxy_error_cb)                           
                
            ReactorThread().callInReactor(wrap, self, args)                                                     
            return d
        return _missing

class ManifoldXMLRPCClient(ManifoldClient):

    def auth_check(self):
        self.interface.AuthCheck(self.auth)

    @defer.inlineCallbacks
    def forward(self, query, annotations=None):
        if not annotations:
            annotations = {}
        annotations.update(self.annotations)
        ret = yield self.interface.forward(query.to_dict(), annotations)
        print "RETURN", ret
        defer.resultValue(ret)

class ManifoldXMLRPCClientSSLPassword(ManifoldXMLRPCClient):
    
    from OpenSSL import SSL
    from twisted.internet import ssl, reactor
    from twisted.internet.protocol import ClientFactory, Protocol

    def __init__(self, url, username=None, password=None):
        self.url = url
        self.username = username

        if username:
            self.annotations = { 'authentication': {'AuthMethod': 'password', 'Username': username, 'AuthString': password} }
        else:
            self.annotations = { 'authentication': {'AuthMethod': 'anonymous'} } 

        self.interface = Proxy('https://localhost:7080/', allowNone=True, useDateTime=False)
        self.interface.setSSLClientContext(ssl.ClientContextFactory())

    def log_info(self):
        Log.info("Shell using XMLRPC account '%r' (password) on %s" % (self.username, self.url))

class ManifoldXMLRPCClientSSLGID(ManifoldXMLRPCClient):
    
    from OpenSSL import SSL
    from twisted.internet import ssl, reactor
    from twisted.internet.protocol import ClientFactory, Protocol

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
        self.interface = Proxy('https://localhost:7080/', allowNone=True, useDateTime=False)
        #self.interface.setSSLClientContext(CtxFactory(pkey_file, cert_file))

        self.annotations = { 'authentication': {'AuthMethod': 'gid'} } 

        # This has to be tested to get rid of the previously defined CtxFactory class
        self.interface.setSSLClientContext(ssl.DefaultOpenSSLContextFactory(self.pkey_file, self.cert_file))

    def log_info(self):
        Log.info("Shell using XMLRPC account '%r' (GID) on %s" % (self.gid_subject, self.url))

class Shell(object):

    PROMPT = 'manifold'

    def print_err(self, err):
        print '-'*80
        print 'Exception', err['code'], 'raised by', err['origin'], ':', err['description']
        for line in err['traceback'].split("\n"):
            print "\t", line
        print ''

    @classmethod
    def init_options(self):
        # Processing
        opt = Options()
        opt.add_option(
            "-C", "--cacert", dest = "xmlrpc_cacert",
            help = "API SSL certificate", 
            default = None
        )
        opt.add_option(
            "-k", "--insecure", dest = "xmlrpc_insecure",
            help = "Do not check SSL certificate", 
            default = 7080
        )
        opt.add_option(
            "-U", "--url", dest = "xmlrpc_url",
            help = "API URL", 
            default = 'http://localhost:7080'
        )
        opt.add_option(
            "-u", "--username", dest = "username",
            help = "API user name", 
            default = DEFAULT_USER
        )
        opt.add_option(
            "-p", "--password", dest = "password",
            help = "API password", 
            default = DEFAULT_PASSWORD
        )
        opt.add_option(
            "-x", "--xmlrpc", action="store_true", dest = "xmlrpc",
            help = "Use XML-RPC interface", 
            default = False
        )
        opt.add_option(
            "-a", "--auth-method", dest = "auth_method",
            help    = 'Choice of the authentication method: auto, anonymous, password, gid',
            default = 'auto'
        )
        opt.add_option(
            "-e", "--execute", dest = "execute",
            help = "Execute a shell command", 
            default = None
        )
        #parser.add_option("-m", "--method", help = "API authentication method")
        #parser.add_option("-s", "--session", help = "API session key")

    def select_auth_method(self, auth_method):
        if auth_method == 'auto':
            for method in ['local', 'gid', 'password']:
                try:
                    Log.debug("Trying client authentication '%s'" % method)
                    self.select_auth_method(method)
                    return
                except Exception, e:
                    Log.debug("Failed client authentication '%s': %s" % (method, e))
            raise Exception, "Could not authentication automatically (tried: local, gid, password)"

        elif auth_method == 'local':
            self.client = ManifoldLocalClient() # XXX choice of the user ?

        else: # XMLRPC 
            url = Options().xmlrpc_url

            if auth_method == 'gid':
                pkey_file = Options().pkey_file
                cert_file = Options().cert_file

                self.client = ManifoldXMLRPCClientSSLGID(url, pkey_file, cert_file)

            elif auth_method == 'password':
                # If user is specified but password is not
                username = Options().username
                password = Options().password

                if username != DEFAULT_USER and password == DEFAULT_PASSWORD:
                    if interactive:
                        try:
                            _password = getpass("Enter password for '%s' (or ENTER to keep default):" % username)
                        except (EOFError, KeyboardInterrupt):
                            print
                            sys.exit(0)
                        if _password:
                            password = _password
                    else:
                        Log.warning("No password specified, using default.")

                self.client = ManifoldXMLRPCClientSSLPassword(url, username, password)

            elif auth_method == 'anonymous':

                self.client = ManifoldXMLRPCClientSSLPassword(url)

            else:
                raise Exception, "Authentication method not supported: '%s'" % auth_method


    def __init__(self, interactive=False):
        self.interactive = interactive
        
        self.select_auth_method(Options().auth_method)
        if self.interactive:
            self.client.log_info()
        self.auth_check()

    def terminate(self):
        # XXX Issues with the reference counter
        #del self.client
        #self.client = None
        self.client.__del__()

    def display(self, ret):
        if ret['code'] != 0:
            if isinstance(ret['description'], list):
                # We have a list of errors
                for err in ret['description']:
                    self.print_err(err)
    
        ret = ret['value']
    
        if self.interactive:
            print "===== RESULTS ====="
            pprint.pprint(ret)
        else:
            print json.dumps(ret)

    def evaluate(self, command, value=False):
        #username, password = Options().username, Options().password
        query = Query(SQLParser().parse(command))
        ret = self.client.forward(query)
        if not value:
            return ret 
        else:
            if ret['code'] != 2:
                return ret['value']
            else:
                raise Exception, "Error evaluating command: %s (%s)" % (command, ret['description'])

    def execute(self, query):
        return self.client.forward(query)

    def auth_check(self):
        return self.client.auth_check()

    def whoami(self):
        # Who is authenticating ?
        # authentication
        return None
        
        
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
                    ret = self.evaluate(command)
                    self.display(ret)
                except KeyboardInterrupt:
                    command = ""
                    print
                except Exception, err:
                    print_exc()

        except EOFError:
            self.terminate()

def main():
#    Log.init_options()
#    Options().parse()
    command = Options().execute
    if command:
        s = Shell(interactive=False)
        s.display(s.evaluate(command))
    else:
        Shell(interactive=True).start()

Shell.init_options()
    
if __name__ == '__main__':
    main()

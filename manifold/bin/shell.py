#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# A Shell allows to read Query from the standard input and
# forwards it to a Manifold Interface.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.aue@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import os, sys, pprint, json, traceback
from getpass                        import getpass
from optparse                       import OptionParser
from socket                         import gethostname
from traceback                      import format_exc
from types                          import StringTypes

# XXX Those imports may fail for xmlrpc calls
#from manifold.auth                  import Auth
from manifold.core.packet           import ErrorPacket
from manifold.core.query            import Query
from manifold.core.sync_receiver    import SyncReceiver
from manifold.core.result_value     import ResultValue
from manifold.input.sql             import SQLParser
from manifold.util.colors           import BOLDBLUE, NORMAL
from manifold.util.log              import Log
from manifold.util.options          import Options
from manifold.util.type             import accepts, returns

# This could be moved outside of the Shell
DEFAULT_USER      = "demo"
DEFAULT_PASSWORD  = "demo"
DEFAULT_PKEY_FILE = "/etc/manifold/keys/client.pkey" 
DEFAULT_CERT_FILE = "/etc/manifold/keys/client.cert"
DEFAULT_API_URL   = "https://localhost:7080"

class Shell(object):

    PROMPT = "manifold"

    @classmethod
    def print_error(self, error):
        """
        Print ErrorPacket content
        Args:
            error: An ErrorPacket instance.
        """
        assert isinstance(error, ErrorPacket),\
            "Invalid error = %s (%s)" % (error, type(error))

        message   = error.get_message()
        traceback = error.get_traceback()

        print "* Error:"
        if message:
            Log.error(message)

        print "* Traceback:"
        if traceback:
            Log.error(traceback)

    @staticmethod
    def init_options():
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
            default = DEFAULT_API_URL 
        )
        opt.add_argument(
            "-u", "--username", dest = "username",
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
            default = "auto"
        )
        opt.add_argument(
            "-e", "--execute", dest = "execute",
            help = "Execute a shell command", 
            default = None
        )
        #parser.add_argument("-m", "--method", help = "API authentication method")
        #parser.add_argument("-s", "--session", help = "API session key")

    def authenticate_local(self, username):
        """
        Prepare a Client to dial with a local Manifold Router.
        Args:
            username: A String containing the user's email address.
        """
        from manifold.clients.local import ManifoldLocalClient
        self.client = ManifoldLocalClient(username, self.storage, self.load_storage)

    def authenticate_xmlrpc_password(self, url, username, password):
        """
        Prepare a Client to dial with a Manifold Router through a XMLRPC
        API using a SSL connection.
        Args:
            url: A String containing the URL of the XMLRPC server.
                Example: DEFAULT_API_URL 
            username: A String containing the user's login. 
            password: A String containing the user's password. 
        """
        from manifold.clients.xmlrpc_ssl_password import ManifoldXMLRPCClientSSLPassword
        self.client = ManifoldXMLRPCClientSSLPassword(url, username, password)

    def authenticate_xmlrpc_gid(self, url, pkey_file, cert_file):
        """
        Prepare a Client to dial with a Manifold Router through a XMLRPC
        API using a SSL connection using GIT authentication. 
        Args:
            url: A String containing the URL of the XMLRPC server.
                Example: DEFAULT_API_URL 
            pkey_file: A String containing the absolute path of the user's
                private key.
                Example: "/etc/manifold/keys/client.pkey"
            cert_file: A String containing the absolute path of the user's
                certificate.
                Example: "/etc/manifold/keys/client.cert"
        """
        from manifold.clients.xmlrpc_ssl_gid import ManifoldXMLRPCClientSSLGID
        self.client = ManifoldXMLRPCClientSSLGID(url, pkey_file, cert_file)

    def authenticate_xmlrpc_anonymous(self, url):
        """
        Prepare a Client to dial with a Manifold Router through a XMLRPC
        API using a SSL connection using an anonymous account.
        Args:
            url: The URL of the XMLRPC server.
                Example: DEFAULT_API_URL 
        """
        from manifold.clients.xmlrpc_ssl_password import ManifoldXMLRPCClientSSLPassword
        self.client = ManifoldXMLRPCClientSSLPassword(url)

    def select_auth_method(self, auth_method):
        """
        Args:
            auth_method: A String instance among "auto", "gid", "local"
        """
        if auth_method == "auto":
            methods = ["gid", "password"] if Options().xmlrpc else ["local"]
            for method in methods:
                try:
                    #Log.tmp("Trying client authentication '%s'" % method)
                    self.select_auth_method(method)
                    #Log.tmp("Automatically selected '%s' authentication method" % method)
                    return
                except Exception, e:
                    Log.error(format_exc())
                    Log.debug("Failed client authentication '%s': %s" % (method, e))
            raise Exception, "Could not authenticate automatically (tried: local, gid, password)"

        elif auth_method == 'local':
            username = Options().username
            self.authenticate_local(username)

        else: # XMLRPC 
            url = Options().xmlrpc_url

            if auth_method == 'gid':
                pkey_file = Options().pkey_file
                cert_file = Options().cert_file
                self.authenticate_xmlrpc_gid(url, pkey_file, cert_file)

            elif auth_method == 'password':
                # If user is specified but password is not
                username = Options().username
                password = Options().password

                if username != DEFAULT_USER and password == DEFAULT_PASSWORD:
                    if Options().interactive:
                        try:
                            _password = getpass("Enter password for '%s' (or ENTER to keep default):" % username)
                        except (EOFError, KeyboardInterrupt):
                            print
                            sys.exit(0)
                        if _password:
                            password = _password
                    else:
                        Log.warning("No password specified, using default.")

                self.authenticate_xmlrpc_password(url, username, password)

            elif auth_method == 'anonymous':

                self.authenticate_xmlrpc_anonymous(url)

            else:
                raise Exception, "Authentication method not supported: '%s'" % auth_method

    def __init__(self, interactive = False, storage = None, load_storage = True):
        """
        Constructor.
        Args:
            interactive: A boolean set to True if this Shell is used in command-line,
                and set to False otherwise (Shell used in a script, etc.).
            storage: A Storage instance or None. 
                Example: See MANIFOLD_STORAGE defined in manifold.bin.config
            load_storage: A boolean set to True if the local Router of this Shell must
                load this Storage.
        """
        self.interactive    = interactive
        self.storage        = storage 
        self.load_storage   = load_storage
        
# This does not work for shell command like manifold-enable-platform because self.client
# won't be initialized.
#MANDO|        if not interactive:
#MANDO|            return
#MANDO|
#MANDO|        auth_method = Options().auth_method
#MANDO|        if not auth_method:
#MANDO|            auth_method = "local"
#MANDO|
#MANDO|        self.select_auth_method(auth_method)
#MANDO|        if self.interactive:
#MANDO|            Log.info(self.client.welcome_message())

        auth_method = Options().auth_method if interactive else None
        if not auth_method:
            auth_method = "local"

        # Initialize self.client
        self.select_auth_method(auth_method)

        # No client has been set, so we cannot run anything.
        if not self.client:
            raise RuntimeError("No client set")

        if self.interactive:
            Log.info(self.client.welcome_message())

    def terminate(self):
        """
        Leave gracefully the Shell by shutdowning properly the nested ManifoldClient.
        """
        # XXX Issues with the reference counter
        #del self.client
        #self.client = None
        try:
            self.client.__del__()
        except Exception, e:
            Log.error(e)
            pass

    def display(self, result_value):
        """
        Print the ResultValue of a Query in the standard output.
        If this ResultValue carries error(s), those error(s) are recursively
        unested and printed to the standard output.
        Args:
            result_value: The ResultValue instance corresponding to this Query.
        """
        assert isinstance(result_value, ResultValue), "Invalid ResultValue: %s (%s)" % (result_value, type(result_value))

        if result_value.is_success():
            records = result_value["value"]
            dicts = [record.to_dict() for record in records]
            if self.interactive:
                # Command-line
                print "===== RESULTS ====="
                pprint.pprint(dicts)
            elif Options().execute:
                # Used by script to it may be piped.
                print json.dumps(dicts)

        else:
            print "===== ERROR ====="
            Log.reset_duplicates()
            errors = result_value["description"]
            if isinstance(errors, StringTypes):
                # String
                Log.error(errors)
            elif isinstance(errors, list):
                # list of ErrorPacket 
                i = 0
                num_errors = len(errors)
                for error in errors:
                    i += 1
                    print "Error (%s/%s)" % (i, num_errors)
                    Shell.print_error(error)
            else:
                raise RuntimeError("Invalid description type (%s) in result_value = %s" % (errors, result_value))

    @returns(ResultValue)
    def evaluate(self, command, value = False):
        """
        Parse a command type by the User, and run the corresponding Query.
        Args:
            command: A String instance containing the command typed by the user.
        Returns:
            The ResultValue resulting from the Query
        """
        #username, password = Options().username, Options().password
        dic = SQLParser().parse(command)
        if not dic:
            raise RuntimeError("Can't parse input command: %s" % command)
        query = Query(dic)
        if "*" in query.get_select():
            query.fields = None

        return self.execute(query)

    @returns(ResultValue)
    def execute(self, query):
        """
        Execute a Query (used if the Shell is run with option "-e").
        Args:
            query: The Query typed by the user.
        Returns:
            The ResultValue resulting from the Query
        """
        try:
            result_value = self.client.forward(query)
        except Exception, e:
            Log.error(traceback.format_exc())
            message = "Exception raised while performing this query:\n\n\t%s\n\n\t%s" % (query, e)
            Log.error(message)
            result_value = ResultValue.error(message)
        return result_value

    @returns(dict)
    def whoami(self):
        """
        Returns:
            The dictionnary representing the User currently using this Shell.
        """
        # Who is authenticating ?
        # authentication
        return self.client.whoami

    @returns(StringTypes)
    def welcome_message(self):
        """
        Returns:
            The welcome message to print in this Shell, depending
            on the ManifoldClient set for this Shell.
        """
        return self.client.welcome_message()
        
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
        """
        Start this Shell.
        """
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
        history_path = os.path.join(os.environ["HOME"], ".manifold_history")
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
                        line = raw_input(BOLDBLUE + Shell.PROMPT + sep + NORMAL)
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
                    self.display(self.evaluate(command))
                except KeyboardInterrupt:
                    # Ctrl c
                    command = ""
                    print
                except RuntimeError, e:
                    # Parse error raised by evaluate()
                    Log.error(e)
                except Exception:
                    # Unhandled Exception raised by this Shell
                    Log.reset_duplicates()
                    print "=== UNHANDLED EXCEPTION ==="
                    Log.error(format_exc())

        except EOFError:
            self.terminate()

def main():
    Shell.init_options()
    Log.init_options()
    Options().parse()
    command = Options().execute

    # Do not import MANIFOLD_STORAGE at the begining of the file, because
    # it will cascadly include Options() and interrupts its initialization,
    # breaking Shell options initialization.
    # Ex: manifold-shell -u michel.bizot@upmc.fr
    from manifold.bin.config  import MANIFOLD_STORAGE

    if command:
        try:
            shell = Shell(interactive = False, storage = MANIFOLD_STORAGE, load_storage = True)
            shell.display(shell.evaluate(command))
        except:
            shell.terminate()
    else:
        shell = Shell(interactive = True, storage = MANIFOLD_STORAGE, load_storage = True).start()

if __name__ == '__main__':
    main()

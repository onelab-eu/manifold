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

import json, os, sys, traceback
from getpass                        import getpass
from optparse                       import OptionParser
from pprint                         import pprint
from socket                         import gethostname
from traceback                      import format_exc
from types                          import StringTypes

from twisted.internet               import defer

# XXX Those imports may fail for xmlrpc calls
#from manifold.auth                  import Auth
from manifold.core.annotation       import Annotation
from manifold.core.packet           import ErrorPacket
from manifold.core.query_factory    import QueryFactory
from manifold.core.record           import Record
from manifold.core.result_value     import ResultValue
from manifold.core.sync_receiver    import SyncReceiver
from manifold.input.sql             import SQLParser
from manifold.util.colors           import BOLDBLUE, NORMAL
from manifold.util.log              import Log
from manifold.util.options          import Options
from manifold.util.reactor_thread   import ReactorThread
from manifold.util.type             import accepts, returns

from manifold.clients.client        import ManifoldClient
from manifold.interfaces            import Interface

# This could be moved outside of the Shell
DEFAULT_USER      = "demo"
DEFAULT_PASSWORD  = "demo"
DEFAULT_PKEY_FILE = "/etc/manifold/keys/client.pkey"
DEFAULT_CERT_FILE = "/etc/manifold/keys/client.cert"
DEFAULT_API_URL   = "https://localhost:7080"

TRUE_VALUES  = [1, '1', 'true', 'on', 'yes']
FALSE_VALUES = [0, '0', 'false', 'off', 'no']

CMD_ADD_PLATFORM = """
INSERT INTO local:platform
    SET
        platform          = '%(platform_name)s',
        platform_longname = '%(platform_longname)s',
        gateway_type      = '%(gateway_type)s',
        auth_type         = '%(auth_type)s',
        config            = '%(config)s',
        disabled          = %(disabled)s
"""

CMD_ENABLE_PLATFORM = """
UPDATE local:platform
    SET   disabled = %(disabled)s
    WHERE platform == '%(platform_name)s'
"""

MAN_ADD_PLATFORM = """
Usage: ADD NAME GATEWAY [CONFIG [ENABLED|DISABLED]]
  Config is a JSON string; no space allowed
  The platform is enabled by default
"""

class Shell(object):

    PROMPT = "manifold"

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, interface_type = None,
        auth_method = None,
        interactive = False,
        **kwargs
    ):
        """
        Constructor.
        Args:
            interactive: A boolean set to True if this Shell is used in command-line,
                and set to False otherwise (Shell used in a script, etc.).
        """
        self._auth_method = auth_method
        self._interactive = interactive
        self.client       = None
        self.terminated_clients = list()
        if interface_type:
            Interface.register_all()
            self.set_interface(interface_type, **kwargs)
            # XXX Note that the interface can fail to connect
        else:
            # Old method
            self.bootstrap()
        # {String : list(dict)} : maps a variable name with its the corresponding Records
        self._environment = dict()
        self._annotation = dict()
        self._query_plan = False
#       self._environment = {"$USER" : "marc-olivier.buob@lip6.fr"}

    def terminate(self):
        """
        Leave gracefully the Shell by shutdowning properly the nested ManifoldClient.
        """
        # Workaround to avoid terminating reactor thread...
        for client in self.terminated_clients:
            client.terminate()

        if self.client: # In case of error, the self.client might be equal to None
            self.client.terminate()

    def terminate_client(self, client):
        if not client:
            return
        self.terminated_clients.append(client)

    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def set_auth_method(self, auth_method):
        """
        Force this Shell to authenticates using a given method.
        Args:
            auth_method: A String among {"local", "router", "gid"}
        """
        self._auth_method = auth_method
        self.select_auth_method(auth_method)

    # This is becoming the new way of selecting a client
    def set_interface(self, interface_type, **kwargs):
        self.terminate_client(self.client)
        self.client = ManifoldClient(interface_type, **kwargs)

    @returns(bool)
    def is_interactive(self):
        """
        Returns:
            True iif this Shell is interactive (i.e. if it parses command-line).
        """
        return self._interactive

    def is_connected(self):
        return self.client and self.client.is_connected()

    #---------------------------------------------------------------------------
    # Initialization
    #---------------------------------------------------------------------------

    def bootstrap(self):
        if not self._auth_method:
            self._auth_method = Options().auth_method
        self.select_auth_method(self._auth_method)
        if not self.client:
            raise RuntimeError("No client set")
        if self.is_interactive():
            Log.info(self.client.welcome_message())

    #---------------------------------------------------------------------------
    # Environment
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def environment_evaluate(self, command):
        """
        Parse a command type by the User, and update each environment variable
        (let say "$x") by its corresponding value.
        Args:
            command: A String instance containing the command typed by the user.
        Returns:
            The updated String.
        """
        @returns(StringTypes)
        def value_to_sql(value):
            if isinstance(value, StringTypes):
                x = value.replace('"', '\\"')
                ret = '"%s"' % x
                return ret
            elif value == None:
                return "\"None\""
            else:
                return "%s" % value

        @returns(StringTypes)
        def dict_to_sql(d):
            values = d.values()
            if len(values) > 1:
                # records made of several columns => list(tuple)
                values = [value_to_sql(value) for value in values]
                ret = "(%s)" % ", ".join(values)
                return ret
            else:
                # records made only one columns => list(String)
                return value_to_sql(values[0])

        @returns(StringTypes)
        def dicts_to_sql(dicts):
            return "[%s]" % ", ".join([dict_to_sql(d) for d in dicts])

        # Substitute environment variables by their values
        has_store = command.startswith("$")
        s = command.split("=", 1)[1] if has_store else command
        for variable, value in self._environment.items():
            if isinstance(value, list):
                dicts = value
                value = dicts_to_sql(dicts)
            elif isinstance(value, (int, float, StringTypes)):
                value = value_to_sql(value)
            else:
                raise TypeError("The type of value mapped with %s is not supported (%s)" % (variable, type(value)))
            s = s.replace(variable, value)

        # Return the updated command
        if has_store:
            return " = ".join([command.split("=", 1)[0].strip(), s.strip()])
        else:
            return s.strip()

    def environment_store(self, variable, value):
        """
        Store in the environment of this Shell the value carried by
        a ResultValue.
        Args:
            variable: A String starting with "$" corresponding to
                the name of the variable.
            dicts: A list of dict or a base type (int, float, String)
        """
        assert isinstance(variable, StringTypes) and variable.startswith("$"),\
            "Invalid variable name '%s' (%s)" % (variable, type(variable))
        assert isinstance(value, (int, float, list)),\
            "Invalid value = %s (%s)" % (value, type(value))

        self._environment[variable] = value

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

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

        if traceback and not traceback.startswith("None"):
            print "* Traceback:"
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
            help    = 'Choice of the authentication method: auto, password, gid, local, router',
            default = "auto"
        )

        # XXX While previous options are common to all shells, this one is
        # specific to the command line program
        opt.add_argument(
            "-e", "--execute", dest = "execute",
            help = "Execute a shell command",
            default = None
        )

        opt.add_argument(
            "-t", "--output-type", dest = "output_type",
            help = "Output type",
            default = 'text'
        )
        #parser.add_argument("-m", "--method", help = "API authentication method")
        #parser.add_argument("-s", "--session", help = "API session key")

    def authenticate_local(self, username = None):
        """
        Prepare a Client to dial with a local Manifold Router (run as a demon,
        using manifold-router).
        Args:
            username: A String containing the user's email address.
        """
        from manifold.clients.local import ManifoldLocalClient
        self.terminate_client(self.client)
        self.client = ManifoldLocalClient(username)

    def authenticate_tcp(self, host, port):
        from manifold.clients.tcp   import ManifoldTcpClient
        self.terminate_client(self.client)
        self.client = ManifoldTcpClient(host, port)


    def authenticate_router(self, username):
        """
        Prepare a Client to dial with a local Manifold Router (nested in the shell).
        Args:
            username: A String containing the user's email address.
        """
        from manifold.clients.router import ManifoldRouterClient
        self.terminate_client(self.client)
        self.client = ManifoldRouterClient(username)

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
        self.terminate_client(self.client)
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
        self.terminate_client(self.client)
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
        self.terminate_client(self.client)
        self.client = ManifoldXMLRPCClientSSLPassword(url)

    def select_auth_method(self, auth_method):
        """
        Args:
            auth_method: A String instance among "auto", "gid", "local"
        """
        if not auth_method or auth_method == "auto":
            methods = ["gid", "password"] if Options().xmlrpc else ["local"]
            for method in methods:
                try:
                    self.select_auth_method(method)
                    return
                except Exception, e:
                    #Log.error(format_exc())
                    Log.debug("Failed client authentication '%s': %s" % (method, e))
            raise Exception, "Could not authenticate automatically (tried: local, gid, password)"

        elif auth_method == 'local':
            username = Options().username
            try:
                self.authenticate_local(username)
            except Exception, e:
                Log.error(traceback.format_exc())

        elif auth_method == 'router':
            username = Options().username
            self.authenticate_router(username)

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

    def _display(self, records):
        # Command-line
        print "===== RESULTS ====="
        pprint(records)

    def display(self, result_value):
        """
        Print the ResultValue of a Query in the standard output.
        If this ResultValue carries error(s), those error(s) are recursively
        unested and printed to the standard output.
        Args:
            result_value: The ResultValue instance corresponding to this Query.
        """
        assert isinstance(result_value, ResultValue), "Invalid ResultValue: %s (%s)" % (result_value, type(result_value))
        if result_value.is_success() or result_value.is_warning():
            #records = result_value["value"]
            #dicts = [record.to_dict() for record in records]
            records = result_value.get_all().to_dict_list()
            if Options().output_type == 'text':
                self._display(records)
            elif Options().output_type == 'json':
                print json.dumps(records)
            elif Options().output_type == 'csv':
                if not records:
                    # XXX headers only
                    return
                headers = records[0].keys()
                print '\t'.join(headers)
                # XXX order ?
                for r in records:
                    line = [str(r.get(h, 'N/A')) for h in headers]
                    print '\t'.join(line)
            else:
                print "E: Unknown output type: %s" % Options().output_type

        # Some queries have failed, report the errors
        if not result_value.is_success():
            print "===== ERRORS ====="
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
    def evaluate(self, command, annotation = None):
        """
        Parse a command type by the User, and run the corresponding Query.
        Args:
            command: A String instance containing the command typed by the user.
        Returns:
            The ResultValue resulting from the Query
        """
        #username, password = Options().username, Options().password
        updated_command = self.environment_evaluate(command)
        if command != updated_command:
            command = updated_command
            Log.info("Running:\n%s" % command)

        dic = SQLParser().parse(command)
        if not dic:
            raise RuntimeError("Can't parse input command: %s" % command)

        if not annotation:
            annotation = Annotation()
        receiver = dic.pop('receiver', None)
        if receiver:
            annotation['receiver'] = receiver
        annotation.update(dic.pop('annotation', dict()))

        query = QueryFactory.from_dict(dic)

        return self.execute(query, annotation)

    def evaluate2(self, command, annotation = None):
        result_value = self.evaluate(command, annotation)
        if result_value.is_success() or result_value.is_warning():
            #records = result_value["value"]
            #dicts = [record.to_dict() for record in records]
            records = result_value.get_all().to_dict_list()
            return records
        else:
            raise Exception, result_value['description']

    @defer.inlineCallbacks
    def deferred_execute(self, query, annotation = None, receiver = None):
        """
        Execute a Query (used if the Shell is run with option "-e").
        Args:
            query: The Query typed by the user.
        Returns:
            The ResultValue resulting from this Query.
        """

        try:
            result_value = yield self.client.forward(query, annotation, deferred = True)
            if result_value.is_success():
                variable = query.get_variable()
                if variable and result_value.is_success():
                    Log.info("Storing the result into %s" % variable)
                    self.environment_store(variable, result_value.get_all().to_dict_list())
        except Exception, e:
            Log.error(traceback.format_exc())
            message = "Exception raised while performing this query:\n\n\t%s\n\n\t%s" % (query, e)
            Log.error(message)
            result_value = ResultValue.error(message)
        defer.returnValue(result_value)

    @defer.inlineCallbacks
    def deferred_evaluate(self, command, annotation = None):
        """
        Parse a command type by the User, and run the corresponding Query.
        Args:
            command: A String instance containing the command typed by the user.
        Returns:
            The ResultValue resulting from the Query
        """
        updated_command = self.environment_evaluate(command)
        if command != updated_command:
            command = updated_command
            Log.info("Running:\n%s" % command)

        dic = SQLParser().parse(command)
        if not dic:
            raise RuntimeError("Can't parse input command: %s" % command)

        if not annotation:
            annotation = Annotation()
        receiver = dic.pop('receiver', None)
        if receiver:
            annotation['receiver'] = receiver
        annotation.update(dic.pop('annotation', dict()))

        query = QueryFactory.from_dict(dic)

        result_value = yield self.deferred_execute(query, annotation)
        defer.returnValue(result_value)

    @defer.inlineCallbacks
    def deferred_evaluate2(self, command, annotation = None):
        result_value = yield self.deferred_evaluate(command, annotation)
        if result_value.is_success() or result_value.is_warning():
            #records = result_value["value"]
            #dicts = [record.to_dict() for record in records]
            records = result_value.get_all().to_dict_list()
            defer.returnValue(records)
        else:
            raise Exception, result_value['description']

    @returns(ResultValue)
    def execute(self, query, annotation = None, receiver = None):
        """
        Execute a Query (used if the Shell is run with option "-e").
        Args:
            query: The Query typed by the user.
        Returns:
            The ResultValue resulting from this Query.
        """

        try:
            result_value = self.client.forward(query, annotation)
            if result_value.is_success():
                variable = query.get_variable()
                if variable and result_value.is_success():
                    Log.info("Storing the result into %s" % variable)
                    self.environment_store(variable, result_value.get_all().to_dict_list())
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

    def handle_dump(self, args):
        if len(args) != 3 or args[1] != 'INTO' or not args[0].startswith('$'):
            Log.error("Wrong DUMP arguments: %r" % args)
            Log.error("Usage: DUMP [variable] INTO [filename]")
            return

        # VARIABLE
        variable = args[0]

        # . Does it exists ?
        if not variable in self._environment:
            Log.error("Variable %s does not exist." % (variable, ))
            return

        # FILENAME
        filename = args[2]
        if (filename[0] == '"' and filename[-1] == '"') or (filename[0] == "'" and filename[-1] == "'"):
            filename = filename[1:-1]

        # . Can we write into it ?
        try:
            file = open(filename, "w")

            records = self._environment[variable]

            if not records:
                # Empty file
                f.close()
                return

            fields = records[0].keys()

            # Headers
            print >>file, ", ".join(fields)

            # Records
            for record in records:
                line = ", ".join(['"%s"' % (record[f], ) for f in fields])
                print >>file, line

            file.close()
        except Exception, e:
            Log.error("Error in writing variable content into %r: %s" % (filename, e))
            import traceback
            traceback.print_exc()
            return

    def handle_show(self, args):
        if len(args) == 0:
            # SHOW
            print "Current variables:", self._environment.keys()
            return

        elif len(args) == 1 and args[0].startswith('$'):
            # SHOW $variable
            variable = args[0]
            self._display(self._environment[variable])

        elif len(args) == 1 and args == ['platforms']:
            # SHOW platforms
            command = "select * from local:platform where disabled == 0"
            self.display(self.evaluate(command))
        elif len(args) == 2 and args == ['all', 'platforms']:
            # SHOW ALL platforms
            command = "select * from local:platform"
            self.display(self.evaluate(command))
        else:
            print len(args), args
            Log.error("Wrong SHOW arguments: %r" % args)
            Log.error("Usage:")
            Log.error("  SHOW $variable")
            Log.error("  SHOW [ALL] PLATFORMS")
            return

    def handle_add(self, name, gateway_type, config='{}', enabled='enabled'):
        try:
            try:
                json.loads(config)
            except Exception, e:
                Log.error("Wrong configuration: %s [%s]" % (config, e))
                return
            platform_params = {
                'platform_name': name,
                'platform_longname': name,
                'gateway_type': gateway_type,
                'auth_type': 'none',
                'config': config,
                'disabled': 0 if enabled == 'enabled' else 1
            }
            command = CMD_ADD_PLATFORM % platform_params
            self.display(self.evaluate(command))
            Log.info("The platform will not be active until the next restart")
        except Exception, e:
            Log.error(MAN_ADD_PLATFORM)
            raise e

    def handle_enable(self, name, enabled=True):
        try:
            platform_params = {
                'platform_name': name,
                'disabled': 0 if enabled else 1
            }
            if enabled:
                status = "enabled"
            else:
                status = "disabled"
            command = CMD_ENABLE_PLATFORM % platform_params
            self.display(self.evaluate(command))
            Log.info("The platform %s is %s" % (name, status))
        except Exception, e:
            if enabled:
                Log.error("Usage: enable NAME")
            else:
                Log.error("Usage: disable NAME")
            raise e

    def handle_set(self, args):
        if len(args) == 0:
            print "Current annotation:"
            for key, value in self._annotation.items():
                print " - ", key, "=", value
            return
        elif len(args) > 2:
            Log.error("Wrong SET arguments: %r" % args)
            Log.error("Usage: SET key value")
            return

        key, value = args
        if key == 'queryplan':
            if not value.lower() in TRUE_VALUES or value.lower() in FALSE_VALUES:
                Log.error("Wrong value for queryplan setting")
                return
            self._query_plan = value.lower() in TRUE_VALUES
            return

        self._annotation[key] = value

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
                        line = raw_input(Shell.PROMPT + sep)
                        #line = raw_input(BOLDBLUE + Shell.PROMPT + sep + NORMAL)
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

                # Shell commands
                try:
                    command_tokens = command.lower().split(' ')
                    if command_tokens[0] == 'dump':
                        self.handle_dump(command_tokens[1:])
                        continue;
                    elif command_tokens[0] == 'show':
                        self.handle_show(command_tokens[1:])
                        continue;
                    elif command_tokens[0] == 'set':
                        self.handle_set(command_tokens[1:])
                        continue;
                    elif command_tokens[0] == 'add':
                        self.handle_add(*command_tokens[1:])
                    elif command_tokens[0] == 'enable':
                        self.handle_enable(command_tokens[1])
                    elif command_tokens[0] == 'disable':
                        self.handle_enable(command_tokens[1],False)

                    elif self._query_plan:
                        self._annotation['queryplan'] = True
                    else:
                        self.display(self.evaluate(command))
                        self._annotation = dict()
                except KeyboardInterrupt:
                    # Ctrl c
                    command = ""
                    print
                except RuntimeError, e:
                    # Parse error raised by evaluate()
                    Log.error("Shell runtime error", e)
                except Exception, e:
                    # Unhandled Exception raised by this Shell
                    Log.reset_duplicates()
                    print "=== UNHANDLED EXCEPTION ==="
                    print e
                    Log.error(format_exc())

        except EOFError: # Ctrl d
            print
        self.terminate()

def main():
    try:
        Shell.init_options()
        Log.init_options()
        Options().parse()
        command = Options().execute
        annotation = None # XXX undefined

        if command:
            try:
                shell = Shell(interactive = False)
                shell.display(shell.evaluate(command, annotation))
            except Exception, e:
                Log.error(traceback.format_exc())
                shell.terminate()
        else:
            shell = Shell(interactive = True).start()

    except Exception, e:
        Log.error(traceback.format_exc())

if __name__ == '__main__':
    main()

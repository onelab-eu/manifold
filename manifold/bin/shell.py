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

# XXX Those imports may fail for xmlrpc calls
from manifold.auth                  import Auth
from manifold.core.query            import Query
from manifold.core.router           import Router
from manifold.core.result_value     import ResultValue
from manifold.input.sql             import SQLParser
from manifold.util.log              import Log
from manifold.util.options          import Options
from manifold.util.type             import accepts, returns

# This could be moved outside of the Shell
DEFAULT_USER     = 'demo'
DEFAULT_PASSWORD = 'demo'

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
                Log.debug("\t", line)
        else:
            Log.debug("(Traceback not set)")
        print ""

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
            "-a", "--anonymous", action = "store_true", dest = "anonymous",
            help = "Use anonymous authentication", 
            default = False 
        )
        opt.add_option(
            "-e", "--execute", dest = "execute",
            help = "Execute a shell command", 
            default = None
        )
        #parser.add_option("-m", "--method", help = "API authentication method")
        #parser.add_option("-s", "--session", help = "API session key")

    def __init__(self, interactive = False):
        """
        Constructor.
        Args:
            interactive: A boolean.
        """
        self.result_value = None # Result of the last Query
        self.interactive = interactive

        if not Options().anonymous:
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

            #if Options().xmlrpc:
            self.auth = {'AuthMethod': 'password', 'Username': username, 'AuthString': password}
            #else:
            #    self.auth = username
        else:
            self.auth = None

        if Options().xmlrpc:
            import xmlrpclib
            url = Options().xmlrpc_url
            self.interface = xmlrpclib.ServerProxy(url, allow_none=True)

            mode_str      = 'XMLRPC'
            interface_str = ' towards XMLRPC API %s' % self.interface
        else:
            self.interface = Router()
            self.interface.__enter__()
            mode_str      = 'local'
            interface_str = ''

        if self.interactive:
            msg = "Shell using %(mode_str)s"
            if not Options().anonymous:
                msg += " account %(username)r"
            msg += "%(interface_str)s"
            Log.info(msg, **locals())

            if not Options().anonymous:
                if Options().xmlrpc:
                    try:
                        self.interface.AuthCheck(self.auth)
                        Log.info('Authentication successful')
                    except:
                        Log.error('Authentication error')

    def terminate(self):
        """
        Stops gracefully the Manifold interface managed by this Shell.
        """
        if not Options().xmlrpc: self.interface.__exit__()

    def set_result_value(self, result_value):
        """
        Function called back by self.interface.forward() once the Query
        has been executed.
        Args:
            result_value: A ResultValue built once the Query has terminated.
        """
        self.result_value = result_value

    @returns(ResultValue)
    def get_result_value(self):
        """
        Returns:
            The ResultValue corresponding to the last issued Query.
        """
        return self.result_value

    @returns(ResultValue)
    def forward(self, query):
        """
        Forward a Query to the interface configured for this Shell.
        Args:
            query: A Query instance representing the command typed
                by the User.
        """
        # XMLRPC-API
        if Options().xmlrpc:
            if Options().anonymous:
                self.interface.forward(query.to_dict())
            else:
                self.interface.forward(self.auth, query.to_dict())
            Log.warning("self.result_value must be initialized")
        # Local API
        else:
            if Options().anonymous:
                self.interface.forward(query, user = None, receiver = self)
            else:
                self.interface.forward(query, user = Auth(self.auth).check(), receiver = self)
        return self.result_value

    def display(self, result_value):
        """
        Print the ResultValue of a Query in the standard output.
        If this ResultValue carries error(s), those error(s) are recursively
        unested and printed to the standard output.
        Args:
            result_value: The ResultValue instance corresponding to this Query.
        """
        assert isinstance(result_value, ResultValue), "Invalid ResultValue: %s (%s)" % (result_value, type(result_value))

        if result_value["code"] != ResultValue.SUCCESS:
            if isinstance(result_value["description"], list):
                # We have a list of errors
                for nested_result_value in result_value["description"]:
                    Shell.print_error(nested_result_value)
                    return
    
        results = result_value["value"]
    
        if self.interactive:
            # Command-line
            print "===== RESULTS ====="
            pprint.pprint(results)
        elif Options().execute:
            # Used by script to it may be piped.
            print json.dumps(results)

    def evaluate(self, command):
        """
        Parse a command type to the user,  and run the corresponding Query.
        Args:
            command: A String instance containing the command typed by the user.
        """
        #username, password = Options().username, Options().password
        d = SQLParser().parse(command)
        if d:
            query = Query(d)
            if "*" in query.get_select(): query.fields = None
            self.display(self.forward(query))

    def execute(self, query):
        """
        Execute a Query.
        Args:
            query: The Query typed by the user.
        """
        result_value = self.forward(query)
        self.display(self.get_result_value())
        
        
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
                    self.terminate()
                    break

                try:
                    self.evaluate(command)
                except KeyboardInterrupt:
                    command = ""
                    print
                except Exception, err:
                    Log.debug(format_exc())

        except EOFError:
            self.terminate()

def main():
#    Log.init_options()
#    Options().parse()
    command = Options().execute
    if command:
        s = Shell(interactive = False)
        s.evaluate(command)
        s.terminate()
    else:
        Shell(interactive = True).start()

Shell.init_options()
    
if __name__ == '__main__':
    main()

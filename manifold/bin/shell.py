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

import json

# This could be moved outside of the Shell
DEFAULT_USER     = 'demo'
DEFAULT_PASSWORD = 'demo'

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

    def __init__(self, interactive=False):
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
                else:
                    self.auth = Auth(self.auth).check()

    def terminate(self):
        if not Options().xmlrpc: self.interface.__exit__()

    def forward(self, query):
        # XXX this line will differ between xmlrpc and local calls
        if Options().xmlrpc:
            # XXX The XMLRPC server might not require authentication
            if not Options().anonymous:
                return self.interface.forward(self.auth, query.to_dict())
            else:
                return self.interface.forward(query.to_dict())
        else:
            return self.interface.forward(query, user=self.auth)

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

    def evaluate(self, command):
        #username, password = Options().username, Options().password
        query = Query(SQLParser().parse(command))
        ret = self.forward(query)
        self.display(ret)

    def execute(self, query):
        ret = self.forward(query)
        self.display(ret)
        
        
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
                    print_exc()

        except EOFError:
            self.terminate()

def main():
#    Log.init_options()
#    Options().parse()
    command = Options().execute
    if command:
        s = Shell(interactive=False)
        s.evaluate(command)
        s.terminate()
    else:
        Shell(interactive=True).start()

Shell.init_options()
    
if __name__ == '__main__':
    main()

#!/usr/bin/python
#
# Interactive shell for testing PLCAPI
#
# Mark Huang <mlhuang@cs.princeton.edu>
# Copyright (C) 2005 The Trustees of Princeton University
#

import os, sys
from socket               import gethostname
from optparse             import OptionParser
from getpass              import getpass
from traceback            import print_exc
from manifold.input.sql   import SQLParser
from manifold.test.config import auth
from manifold.core.router import Router
from manifold.auth        import Auth

with Router() as router:
    def evaluate(command):
        query, = SQLParser().parse(command)
        ret = router.forward(query, user=Auth(auth).check())
        print ret

sys.path.append(os.path.dirname(os.path.realpath(sys.argv[0])))

usage="""Usage: %prog [options]
   runs an interactive shell
Usage: %prog [options] script script-arguments
Usage: %prog script [plcsh-options --] script arguments
   run a script"""

parser = OptionParser(usage=usage,add_help_option = False)
parser.add_option("-f", "--config", help = "PLC configuration file")
parser.add_option("-h", "--url", help = "API URL")
parser.add_option("-c", "--cacert", help = "API SSL certificate")
parser.add_option("-k", "--insecure", help = "Do not check SSL certificate")
parser.add_option("-m", "--method", help = "API authentication method")
parser.add_option("-s", "--session", help = "API session key")
parser.add_option("-u", "--user", help = "API user name")
parser.add_option("-p", "--password", help = "API password")
parser.add_option("-r", "--role", help = "API role")
parser.add_option("-x", "--xmlrpc", action = "store_true", default = False, help = "Use XML-RPC interface")
# pass this to the invoked shell if any
parser.add_option("--help", action = "store_true", dest="help", default=False,
                  help = "show this help message and exit")
(options, args) = parser.parse_args()

if not args and options.help:
    parser.print_help()
    sys.exit(1)    

# If user is specified but password is not
if options.user is not None and options.password is None:
    try:
        options.password = getpass()
    except (EOFError, KeyboardInterrupt):
        print
        sys.exit(0)

# Initialize a single global instance (scripts may re-initialize
# this instance and/or create additional instances).
#try:
#    shell = Shell(globals = globals(),
#                  config = options.config,
#                  url = options.url, xmlrpc = options.xmlrpc, cacert = options.cacert,
#                  method = options.method, role = options.role,
#                  user = options.user, password = options.password,
#                  session = options.session)
#    # Register a few more globals for backward compatibility
#    auth = shell.auth
#    api = shell.api
#    config = shell.config
#except Exception, err:
#    print "Error:", err
#    print
#    parser.print_help()
#    sys.exit(1)

# If called by a script 
if args:
    if not os.path.exists(args[0]):
        print 'File %s not found'%args[0]
        parser.print_help()
        sys.exit(1)
    else:
        # re-append --help if provided
        if options.help:
            args.append('--help')
        # use args as sys.argv for the next shell, so our own options get removed for the next script
        sys.argv = args
        script = sys.argv[0]
        # Add of script to sys.path 
        path = os.path.dirname(os.path.abspath(script))
        sys.path.append(path)
        execfile(script)

# Otherwise, run an interactive shell environment
else:
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
    prompt="manifold"

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
                    line = raw_input(prompt + sep)
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
                evaluate(command)
            except Exception, err:
                print_exc()

    except EOFError:
        print
        pass

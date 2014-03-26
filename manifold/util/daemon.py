#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Daemon: superclass used to implement a daemon easily
#
# Copyright (C)2009-2012, UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

# see also: http://www.jejik.com/files/examples/daemon3x.py

# This is used to import the daemon package instead of the local module which is
# named identically...
from __future__ import absolute_import

from manifold.util.singleton    import Singleton
from manifold.util.log          import Log
from manifold.util.options      import Options
from manifold.util.type         import accepts, returns

import atexit, os, signal, lockfile, logging, sys

#UNUSED|# python2.6 bugfix, useless in python2.7
#UNUSED|def _checkLevel(level):
#UNUSED|    from logging import _levelNames
#UNUSED|    if isinstance(level, int):
#UNUSED|        rv = level
#UNUSED|    elif str(level) == level:
#UNUSED|        if level not in _levelNames:
#UNUSED|            raise ValueError("Unknown level: %r" % level)
#UNUSED|        rv = _levelNames[level]
#UNUSED|    else:
#UNUSED|        raise TypeError("Level not an integer or a valid string: %r" % level)
#UNUSED|    return rv

class Daemon(object):
    __metaclass__ = Singleton

    DEFAULTS = {
        # Running
        "uid"                 : os.getuid(),
        "gid"                 : os.getgid(),
        "working_directory"   : "/",
        "debugmode"           : False,
        "no_daemon"           : False,
        "pid_filename"        : "/var/run/%s.pid" % Options().get_name()
    }

    #-------------------------------------------------------------------------
    # Checks
    #-------------------------------------------------------------------------

    def check_python_daemon(self):
        """
        Check whether python-daemon is properly installed.
        Returns:
            True iiff everything is fine.
        """
        # http://www.python.org/dev/peps/pep-3143/
        ret = False
        try:
            import daemon
            getattr(daemon, "DaemonContext")
            ret = True
        except AttributeError, e:
            # daemon and python-daemon conflict with each other
            Log.critical("Please install python-daemon instead of daemon. Remove daemon first.")
        except ImportError:
            Log.critical("Please install python-daemon - apt-get python-daemon.")
        return ret

    #------------------------------------------------------------------------
    # Initialization
    #------------------------------------------------------------------------

    def __init__(
        self,
        terminate_callback = None
    ):
        """
        Constructor.
        Args:
            terminate_callback: An optional function, called when
                this Daemon must die. You may pass None if not needed.
        """
        self.terminate_callback = terminate_callback

        # Reference which file descriptors must remain opened while
        # daemonizing (for instance the file descriptor related to
        # the logger, a socket file created before daemonization etc.)
        self.files_to_keep = list()
        self.lock_file = None

    @classmethod
    @returns(Options)
    def init_options(self):
        """
        Returns:
            An Options instance containing the options related to
            a daemon program.
        """
        opt = Options()

        opt.add_argument(
            "--uid", dest = "uid",
            help = "UID used to run the daemon.",
            default = self.DEFAULTS['uid']
        )
        opt.add_argument(
            "--gid", dest = "gid",
            help = "GID used to run the daemon.",
            default = self.DEFAULTS['gid']
        )
        opt.add_argument(
            "-w", "--working-directory", dest = "working_directory",
            help = "Working directory.",
            default = self.DEFAULTS['working_directory']
        )
        opt.add_argument(
            "-D", "--debugmode", action = "store_false", dest = "debugmode",
            help = "Daemon debug mode (useful for developers).",
            default = self.DEFAULTS['debugmode']
        )
        opt.add_argument(
            "-n", "--no-daemon", action = "store_true", dest = "no_daemon",
            help = "Run as daemon (detach from terminal).",
            default = self.DEFAULTS["no_daemon"]
        )
        opt.add_argument(
            "-i", "--pid-file", dest = "pid_filename",
            help = "Absolute path to the pid-file to use when running as daemon.",
            default = self.DEFAULTS['pid_filename']
        )

    #------------------------------------------------------------------------
    # Daemon stuff
    #------------------------------------------------------------------------

    @returns(bool)
    def must_be_detached(self):
        """
        Returns:
            True iif the daemon must run in background.
        """
        return (Options().no_daemon != True)

    def remove_pid_file(self):
        """
        (Internal usage)
        Remove the PID file
        """
        if os.path.exists(Options().pid_filename) == True:
            Log.info("Removing %s" % Options().pid_filename)
            os.remove(Options().pid_filename)

        if self.lock_file and self.lock_file.is_locked():
            self.lock_file.release()

    def make_pid_file(self):
        """
        Create a PID file if required in which we store the PID of the daemon if needed
        """
        if Options().pid_filename and self.must_be_detached():
            atexit.register(self.remove_pid_file)
            file(Options().pid_filename, "w+").write("%s\n" % str(os.getpid()))

    @returns(int)
    def get_pid_from_pid_file(self):
        """
        Retrieve the PID of the daemon thanks to the pid file.
        Returns:
            An integer containing the PID of this daemon.
            None if the pid file is not readable or does not exists
        """
        pid = None
        if Options().pid_filename:
            try:
                f_pid = file(Options().pid_filename, "r")
                pid = int(f_pid.read().strip())
                f_pid.close()
            except IOError:
                pid = None
        return pid

    @returns(bool)
    def make_lock_file(self):
        """
        Prepare the lock file required to manage the pid file.
        Initialize self.lock_file
        Returns:
            True iif successful.
        """
        if Options().pid_filename and self.must_be_detached():
            Log.debug("Daemonizing using pid file '%s'" % Options().pid_filename)
            self.lock_file = lockfile.FileLock(Options().pid_filename)
            if self.lock_file.is_locked() == True:
                Log.error("'%s' is already running ('%s' is locked)." % (Options().get_name(), Options().pid_filename))
                return False
            self.lock_file.acquire()
        else:
            self.lock_file = None
        return True

    def start(self):
        """
        Start the daemon.
        """
        # Check whether daemon module is properly installed
        if self.check_python_daemon() == False:
            self.terminate()
        import daemon

        # Prepare self.lock_file
        if not self.make_lock_file():
            sys.exit(1)

        # Prepare the daemon context
        dcontext = daemon.DaemonContext(
            detach_process    = self.must_be_detached(),
            working_directory = Options().working_directory,
            pidfile           = Options().pidfile if self.must_be_detached() else None,
            stdin             = sys.stdin,
            stdout            = sys.stdout,
            stderr            = sys.stderr,
            uid               = Options().uid,
            gid               = Options().gid,
            files_preserve    = Log().files_to_keep
        )

        # Prepare signal handling to stop properly if the daemon is killed
        # Note that signal.SIGKILL can't be handled:
        # http://crunchtools.com/unixlinux-signals-101/
        dcontext.signal_map = {
            signal.SIGTERM : self.signal_handler,
            signal.SIGQUIT : self.signal_handler,
            signal.SIGINT  : self.signal_handler
        }

        with dcontext:
            Log.info("Entering daemonization")
            self.make_pid_file()
            try:
                self.main()
            except Exception, why:
                Log.error("Unhandled exception in start: %s" % why)
                self.terminate()

    def signal_handler(self, signal_id, frame):
        """
        (Internal use)
        Stop the daemon (signal handler)
        Args:
            signal_id: The integer identifying the signal
                (see also "man 7 signal")
                Example: 15 if the received signal is signal.SIGTERM
            frame:
        """
        self.terminate()

    def terminate(self):
        """
        Stops gracefully the daemon.
        Note:
            The lockfile should implicitly released by the daemon package.
        """
        Log.info("Stopping %s" % self.__class__.__name__)
        if self.terminate_callback:
            self.terminate_callback()
        self.remove_pid_file()
        sys.exit(0)

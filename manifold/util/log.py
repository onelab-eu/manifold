#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Log is a Singleton providing several methods allowing to log
# message locally or thanks to a rsyslog. It can write colored
# messages colors.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

import sys, logging, traceback, inspect, os.path
from logging                    import handlers
from types                      import StringTypes

from manifold.util              import colors
from manifold.util.options      import Options
from manifold.util.misc         import caller_name, make_list
from manifold.util.singleton    import Singleton
from manifold.util.type         import accepts, returns

# TODO Log should take separately message strings and arguments to be able to
# remember which messages are seen several times, and also to allow for
# translation
# TODO How to log to stdout without putting None in self.log

class Log(object):
    __metaclass__ = Singleton

    DEFAULTS = {
        # Logging
        "rsyslog_enable"      : False,
        "rsyslog_host"        : None, #"log.top-hat.info",
        "rsyslog_port"        : None, #28514,
        "log_file"            : "/var/log/manifold.log",
        "log_level"           : "DEBUG",
        "debug"               : "default",
        "log_duplicates"      : False
    }

    # COLORS
    color_ansi = {
        "DEBUG"   : colors.MYGREEN,
        "INFO"    : colors.MYBLUE,
        "WARNING" : colors.MYWARNING,
        "ERROR"   : colors.MYRED,
        "HEADER"  : colors.MYHEADER,
        "END"     : colors.MYEND,
        "RECORD"  : colors.MYBLUE,
        "TMP"     : colors.MYCYAN,
    }

    @classmethod
    def color(cls, color):
        """
        Args:
            color: A String among Log.color_ansi.keys().
        Returns:
            The String changing the color used to write message.
        """
        return cls.color_ansi[color] if color else ""

    # To remove duplicate messages
    seen = dict()

    def __init__(self, name = "(default)"):
        """
        Constructor.
        Args:
            name: A String identifying the logger (not yet supported).
        """
        self.log = None # logging.getLogger(name)
        self.files_to_keep = list()
        self.init_log()
        self.color = True

    @staticmethod
    def reset_duplicates():
        Log.seen = dict()

    @classmethod
    def init_options(self):
        """
        This function is used in a script to enrich Options exposed by a
        program to the user by adding every options concerning the logger.

        Example:

            @returns(int)
            def main():
                Shell.init_options()
                Log.init_options()
                Options().parse()
                #...
        """
        opt = Options()

        opt.add_argument(
            "--rsyslog-enable", action = "store_false", dest = "rsyslog_enable",
            help = "Specify if log have to be written to a rsyslog server.",
            default = self.DEFAULTS["rsyslog_enable"]
        )
        opt.add_argument(
            "--rsyslog-host", dest = "rsyslog_host",
            help = "Rsyslog hostname.",
            default = self.DEFAULTS["rsyslog_host"]
        )
        opt.add_argument(
            "--rsyslog-port", type = int, dest = "rsyslog_port",
            help = "Rsyslog port.",
            default = self.DEFAULTS["rsyslog_port"]
        )
        opt.add_argument(
            "-o", "--log-file", dest = "log_file",
            help = "Log filename.",
            default = self.DEFAULTS["log_file"]
        )
        opt.add_argument(
            "-L", "--log-level", dest = "log_level",
            choices = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            help = "Log level",
            default = self.DEFAULTS["log_level"]
        )
        opt.add_argument(
            "-d", "--debug", dest = "debug",
            help = "Debug paths (a list of coma-separated python path: path.to.module.function).",
            default = self.DEFAULTS["debug"]
        )
        opt.add_argument(
            "--log-duplicates", action = "store_true", dest = "log_duplicates",
            help = "Remove duplicate messages in logs",
            default = self.DEFAULTS["log_duplicates"]
        )

    def init_log(self):
        """
        (Internal use)
        Initialize self.log
        """
        # Initialize self.log (require self.files_to_keep)
        if self.log: # for debugging by using stdout, log may be equal to None
            if Options().rsyslog_host:
                shandler = self.make_handler_rsyslog(
                    Options().rsyslog_host,
                    Options().rsyslog_port,
                    Options().log_level
                )
            elif Options().log_file:
                shandler = self.make_handler_locallog(
                    Options().log_file,
                    Options().log_level
                )

    #------------------------------------------------------------------------
    # Log
    #------------------------------------------------------------------------

    @returns(handlers.SysLogHandler)
    def make_handler_rsyslog(self, rsyslog_host, rsyslog_port, log_level):
        """
        (Internal usage) Prepare logging via rsyslog
        Args:
            rsyslog_host: A String containing rsyslog server's FQDN.
            rsyslog_port: An positive integer equal to the rsyslog server's port.
            log_level: A value among:
                {logging.DEBUG, logging.INFO, logging.WARNING,
                 logging.ERROR, logging.CRITICAL}
        Returns:
            The corresponding handler.
        """
        # Prepare the handler
        shandler = handlers.SysLogHandler(
            (rsyslog_host, rsyslog_port),
            facility = handlers.SysLogHandler.LOG_DAEMON
        )

        # The log file must remain open while daemonizing
        self.prepare_handler(shandler, log_level)
        return shandler

    @returns(handlers.RotatingFileHandler)
    def make_handler_locallog(self, log_filename, log_level):
        """
        (Internal usage) Prepare local logging
        Args:
            log_filename: A String containing the filename of the file
                in which the logger has to write log messages.
            log_level: A value among:
                {logging.DEBUG, logging.INFO, logging.WARNING,
                 logging.ERROR, logging.CRITICAL}
        """
        # Create directory in which we store the log file
        log_dir = os.path.dirname(log_filename)
        if log_dir and not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir)
            except OSError, why:
                # XXX here we don't log since log is not initialized yet
                print "OS error: %s" % why

        # Prepare the handler
        shandler = handlers.RotatingFileHandler(
            log_filename,
            backupCount = 0
        )

        # The log file must remain open while daemonizing
        self.files_to_keep.append(shandler.stream)
        self.prepare_handler(shandler, log_level)
        return shandler

    def prepare_handler(self, shandler, log_level):
        """
        (Internal usage) Prepare the logger's handler.
        Args:
            shandler: Handler used to log information
            log_level: A value among:
                {logging.DEBUG, logging.INFO, logging.WARNING,
                 logging.ERROR, logging.CRITICAL}
        """
        shandler.setLevel(log_level)
        formatter = logging.Formatter("%(asctime)s: %(name)s: %(levelname)s %(message)s")
        shandler.setFormatter(formatter)
        self.log.addHandler(shandler)
        self.log.setLevel(getattr(logging, log_level, logging.INFO))

    def get_logger(self):
        return self.log

    @classmethod
    def print_msg(cls, msg, level = None, caller = None):
        """
        Args:
            msg: A String storing the message to print.
            level: The log level related to this message.
            caller: The function which has called the Log method.
        """
        sys.stdout.write(cls.color(level))
        if level:
            print "%s" % level,
        if caller:
            print "[%30s]" % caller,
        print msg,
        print cls.color("END")

    #---------------------------------------------------------------------
    # Log: logger abstraction
    #---------------------------------------------------------------------

    @classmethod
    def build_message_string(cls, msg, ctx):
        if ctx:
            msg = [m % ctx for m in msg]
        if isinstance(msg, (tuple, list)):
            msg = map(lambda s : "%s" % s, msg)
            msg = " ".join(msg)
        else:
            msg = "%s" % msg
        return msg

    @classmethod
    def log_message(cls, level, msg, ctx):
        """
        (Internal usage) Logs a message.
        Args:
            level: A String corresponding to the Log level.
            msg: A String / tuple of Strings corresponding to the message(s).
            ctx: A dict describing the context for the message strings.
        """
        caller = None

        if not Options().log_duplicates:
            try:
                count = cls.seen.get(msg, 0)
                cls.seen[msg] = count + 1
            except TypeError, e:
                # Unhashable types in msg
                count = 0

            if count == 1:
                if isinstance(msg, StringTypes):
                    msg = (msg,)
                msg += (" -- REPEATED -- Future similar messages will be silently ignored. Please use the --log-duplicates option to allow for duplicates",)
            elif count > 1:
                return

        if level == "DEBUG":
            caller = caller_name(skip = 3)
            # Eventually remove "" added to the configuration file
            try:
                paths = tuple(s.strip(' \t\n\r') for s in Options().debug.split(','))
            except:
                paths = None
            if not paths or not caller.startswith(paths):
                return

        logger = Log().get_logger()
        msg_str = cls.build_message_string(msg, ctx)

        if logger:
            logger_fct = getattr(logger, level.lower())
            logger_fct("%s(): %s" % (inspect.stack()[2][3], msg_str))
        else:
            cls.print_msg(msg_str, level, caller)

    @classmethod
    def critical(cls, *msg, **ctx):
        """
        Log a critical message.
        Args:
            level: A String corresponding to the Log level.
            msg: A String / tuple of Strings corresponding to the message(s).
            ctx: A dict describing the context for the message strings.
        """
        if not Options().log_level in ["CRITICAL"]:
            return
        cls.log_message("CRITICAL", msg, ctx)
        logger = Log().get_logger()
        if not Log().get_logger():
            traceback.print_exc()
        sys.exit(0)

    @classmethod
    def error(cls, *msg, **ctx):
        """
        Log an error message.
        Args:
            level: A String corresponding to the Log level.
            msg: A String / tuple of Strings corresponding to the message(s).
            ctx: A dict describing the context for the message strings.
        """
        if not Options().log_level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            return
        #cls.log_message("ERROR", "%s" % traceback.format_exc(), ctx)
        #print "%s" % traceback.format_exc()
        cls.log_message("ERROR", msg, ctx)

    @classmethod
    def warning(cls, *msg, **ctx):
        """
        Log a warning message.
        Args:
            level: A String corresponding to the Log level.
            msg: A String / tuple of Strings corresponding to the message(s).
            ctx: A dict describing the context for the message strings.
        """
        if not Options().log_level in ["DEBUG", "INFO", "WARNING"]:
            return
        cls.log_message("WARNING", msg, ctx)

    @classmethod
    def info(cls, *msg, **ctx):
        """
        Log an info message.
        Args:
            level: A String corresponding to the Log level.
            msg: A String / tuple of Strings corresponding to the message(s).
            ctx: A dict describing the context for the message strings.
        """
        if not Options().log_level in ["DEBUG", "INFO"]:
            return
        cls.log_message("INFO", msg, ctx)

    @classmethod
    def debug(cls, *msg, **ctx):
        """
        Log a debug message.
        Args:
            level: A String corresponding to the Log level.
            msg: A String / tuple of Strings corresponding to the message(s).
            ctx: A dict describing the context for the message strings.
        """
        if not Options().log_level in ["DEBUG"]:
            return
        cls.log_message("DEBUG", msg, ctx)

    @classmethod
    def tmp(cls, *msg):
        """
        Log a temporary message (used most of time for debug purposes).
        Args:
            level: A String corresponding to the Log level.
            msg: A String / tuple of Strings corresponding to the message(s).
        """
        cls.print_msg(" ".join(map(lambda x: "%r" % x, make_list(msg))), "TMP", caller_name())

    @classmethod
    def record(cls, packet, source = None):
        """
        Log a Packet instance flowing from a Producer toward a Consumer.
        Args:
            packet: A Packet instance. In pratice this can be either an
                ErrorPacket or a Record instance.
            source: A reference to the Node (for instance the Operator)
                which has called Log.record().
        """
        msg = [
            "%s :" % (source.format_node()) if source else "",
            "%r" % packet,
        ]
#        from manifold.core.record import Record
#        if isinstance(packet, Record) and "config" in packet.keys():
#            # Hide config field which is often overcrowded ;)
#            record2 = dict()
#            for k, v in packet.items():
#                if k != "config":
#                    record2[k] = v
#                else:
#                    record2[k] = "<config>"
#            msg = [
#                "%s :" % (source.format_node()) if source else "",
#                "%r" % record2,
#            ]
#        cls.print_msg(" ".join(msg), "RECORD", caller_name())

    @classmethod
    def deprecated(cls, new):
        #cls.print_msg("Function %s is deprecated, please use %s" % (caller_name(skip=3), new))
        pass

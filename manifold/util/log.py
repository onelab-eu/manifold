import sys, logging, traceback, inspect
from logging                 import handlers
from manifold.util.singleton import Singleton
from manifold.util.options   import Options
from manifold.util.misc      import caller_name, make_list

class Log(object):
    __metaclass__ = Singleton

    DEFAULTS = {
        # Logging
        "rsyslog_enable"      : False,
        "rsyslog_host"        : "log.top-hat.info",
        "rsyslog_port"        : 28514,
        "log_file"            : "/var/log/tophat/dispatcher.log",
        "log_level"           : "DEBUG",
        "debug"               : "default"
    }

    # COLORS
    color_ansi = {
        'DEBUG'  : '\033[92m', # green
        'INFO'   : '\033[94m', # blue
        'WARNING': '\033[93m',
        'ERROR'  : '\033[91m',
        'HEADER' : '\033[95m', # ?
        'END'    : '\033[0m',
        'RECORD' : '\033[94m', # temporary messages 
        'TMP'    : '\033[91m', # temporary messages 
    }

    @classmethod
    def color(cls, color):
        return cls.color_ansi[color] if color else ''

    def __init__(self, name='(default)'):
        self.log = None #logging.getLog(name)
        self.files_to_keep = []
        self.init_log()
        self.color = True

    @classmethod
    def init_options(self):
        opt = Options()

        opt.add_option(
            "--rsyslog-enable", action = "store_false", dest = "rsyslog_enable",
            help = "Specify if log have to be written to a rsyslog server.",
            default = self.DEFAULTS["rsyslog_enable"]
        )
        opt.add_option(
            "--rsyslog-host", dest = "rsyslog_host",
            help = "Rsyslog hostname.",
            default = self.DEFAULTS["rsyslog_host"]
        )
        opt.add_option(
            "--rsyslog-port", type = "int", dest = "rsyslog_port",
            help = "Rsyslog port.",
            default = self.DEFAULTS["rsyslog_port"]
        )
        opt.add_option(
            "-o", "--log-file", dest = "log_file",
            help = "Log filename.",
            default = self.DEFAULTS["log_file"]
        )
        opt.add_option(
            "-L", "--log-level", dest = "log_level",
            choices = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            help = "Log level",
            default = self.DEFAULTS["log_level"]
        )
        opt.add_option(
            "-d", "--debug", dest = "debug",
            help = "Debug paths (a list of coma-separated python path: path.to.module.function).",
            default = self.DEFAULTS["debug"]
        )

    def init_log(self, options=object()):
        # Initialize self.log (require self.files_to_keep)
        if self.log: # for debugging by using stdout, log may be equal to None
            if options.rsyslog_host:
                shandler = self.make_handler_rsyslog(
                    options.rsyslog_host,
                    options.rsyslog_port,
                    options.log_level
                )
            elif options.log_file:
                shandler = self.make_handler_locallog(
                    options.log_file,
                    options.log_level
                )

    #------------------------------------------------------------------------
    # Log
    #------------------------------------------------------------------------

    def make_handler_rsyslog(self, rsyslog_host, rsyslog_port, log_level):
        """
        \brief (Internal usage) Prepare logging via rsyslog
        \param rsyslog_host The hostname of the rsyslog server
        \param rsyslog_port The port of the rsyslog server
        \param log_level Log level
        """
        # Prepare the handler
        shandler = handlers.SysLogHandler(
            (rsyslog_host, rsyslog_port),
            facility = handlers.SysLogHandler.LOG_DAEMON
        )

        # The log file must remain open while daemonizing 
        self.prepare_handler(shandler, log_level)
        return shandler

    def make_handler_locallog(self, log_filename, log_level):
        """
        \brief (Internal usage) Prepare local logging
        \param log_filename The file in which we write the logs
        \param log_level Log level
        """
        # Create directory in which we store the log file
        log_dir = os.path.dirname(log_filename)
        if not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir)
            except OSError, why:
                log_error(self.log, "OS error: %s" % why)

        # Prepare the handler
        shandler = logging.handlers.RotatingFileHandler(
            log_filename,
            backupCount = 0
        )

        # The log file must remain open while daemonizing 
        self.files_to_keep.append(shandler.stream)
        self.prepare_handler(shandler, log_level)
        return shandler

    def prepare_handler(self, shandler, log_level):
        """
        \brief (Internal usage)
        \param shandler Handler used to log information
        \param log_level Log level
        """
        shandler.setLevel(log_level)
        formatter = logging.Formatter("%(asctime)s: %(name)s: %(levelname)s %(message)s")
        shandler.setFormatter(formatter)
        self.log.addHandler(shandler)
        self.log.setLevel(getattr(logging, log_level, logging.INFO))
                      
    def get_logger(self):
        return self.log

    @classmethod
    def msg(cls, msg, level=None, caller=None):
        sys.stdout.write(cls.color(level))
        if level:
            print "%s" % level,
        if caller:
            print "[%30s]" % caller,
        print msg,
        print cls.color('END')

    #---------------------------------------------------------------------
    # Log: logger abstraction
    #---------------------------------------------------------------------

    @classmethod
    def critical(cls, msg):
        logger = Log().get_logger()
        if logger:
            logger.critical("%s(): %s" % (inspect.stack()[2][3], msg))
        else:
            cls.msg(msg, 'CRITICAL')
        sys.exit(0)

    @classmethod
    def error(cls, msg):
        logger = Log().get_logger()
        if logger:
            logger.error("%s(): %s" % (inspect.stack()[2][3], msg))
        else:
            cls.msg(msg, 'ERROR')
            traceback.print_exc()
        sys.exit(0)

    @classmethod
    def warning(cls, msg):
        logger = Log().get_logger()
        if logger:
            logger.warning("%s(): %s" % (inspect.stack()[2][3], msg))
        else:
            cls.msg(msg, 'WARNING')

    @classmethod
    def info(cls, msg):
        logger = Log().get_logger()
        if logger:
            logger.info("%s(): %s" % (inspect.stack()[2][3], msg))
        else:
            cls.msg(msg, 'INFO')

    @classmethod
    def debug(cls, *msg):
        logger = Log().get_logger()
        caller = caller_name()
        # Eventually remove "" added to the configuration file
        try:
            paths = tuple(Options().debug.split(','))
        except:
            paths = None
        if not paths or not caller.startswith(paths):
            return
        
        if logger:
            logger.debug("%s(): %s" % (inspect.stack()[2][3], msg))
        else:
            cls.msg(' '.join(map(lambda x: "%r"%x, make_list(msg))), 'DEBUG', caller_name())

    @classmethod
    def tmp(cls, *msg):
        cls.msg(' '.join(map(lambda x: "%r"%x, make_list(msg))), 'TMP', caller_name())

    @classmethod
    def record(cls, *msg):
        pass
        #cls.msg(' '.join(map(lambda x: "%r"%x, make_list(msg))), 'RECORD', caller_name())

    @classmethod
    def deprecated(cls, new):
        #cls.msg("Function %s is deprecated, please use %s" % (caller_name(skip=3), new))
        pass

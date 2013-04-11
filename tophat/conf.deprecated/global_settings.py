import logging
import logging.handlers

DEBUG = True

LOG_LEVEL = logging.DEBUG
USE_SYSLOG = False

LOGGING_CONFIG='tophat.conf.logconfig.initialize_logging'
LOGGING = {
   'loggers': {
        'agent': {},
    },
    'syslog_facility': logging.handlers.SysLogHandler.LOG_LOCAL0,
    'syslog_tag': "agent",
    'log_level': LOG_LEVEL,
    'use_syslog': USE_SYSLOG,
}

if DEBUG:
    DATABASE_PATH = ":memory:"
else:
    DATABASE_PATH = "db"


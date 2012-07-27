import logging
import logging.handlers

DEBUG = True

if DEBUG:
    DATABASE_PATH = ":memory:"
else:
    DATABASE_PATH = "db"


#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Helpers to manage options passed in command-line.
# Options is a Singleton which is fed for instance by
# classes such as Log, Shell, etc.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

# TODO:
#
# We should ensure some contraints on options, for example:
# - to be not null after parsing,
# - the directory should exist,
# - ...
# otherwise throw an error at the beginning. This would help us diminish the
# amount of code for checks later.
#
# This option singleton should only be used within executable, options to
# classes should be passed via the constructor exclusively 

import sys, argparse
import os.path
import StringIO

from types                      import StringTypes
from ConfigParser               import SafeConfigParser

# Do not import manifold.util.log here!
from manifold.util.singleton    import Singleton
from manifold.util.type         import accepts, returns

# http://docs.python.org/dev/library/argparse.html#upgrading-optparse-code
# http://stackoverflow.com/questions/2819696/parsing-properties-file-in-python/2819788#2819788

FAKE_SECTION = 'fake'

class Options(object):

    __metaclass__ = Singleton

    # We should be able to use another default conf file
    CONF_FILE = "/etc/manifold/manifold.conf"
    
    def __init__(self, name = None):
        """
        Constructor
        Args:
            name: A String instance which should contains the basename
                of the program related to these options.
        """
        self._parser = argparse.ArgumentParser()
        self._defaults = {}
        self._name = name
        self.clear()

    def clear(self):
        """
        Clear options related to this Options Singleton.
        """
        self.options = dict() 
        self.add_argument(
            "-c", "--config", dest = "cfg_filename",
            help = "Config file to use.", metavar = 'FILE',
            default = self.CONF_FILE
        )

        self.add_argument('positional', nargs='*')
        self.uptodate = False #True

    def parse(self):
        cfg = SafeConfigParser()

        # Load configuration file
        try:
            cfg_filename = sys.argv[sys.argv.index("-c") + 1]
            try:
                with open(cfg_filename): pass
            except IOError: 
                raise Exception, "Cannot open specified configuration file: %s" % cfg_filename
        except ValueError:
            try:
                with open(self.CONF_FILE): pass
                cfg_filename = self.CONF_FILE
            except IOError:
                print "[!!] No default configuration file found (%s), skipping" % self.CONF_FILE
                cfg_filename = None

        cfg_options = None
        if cfg_filename:
            #print "[OK] Loading configuration file: %s" % cfg_filename
            config = StringIO.StringIO()
            config.write('[%s]\n' % FAKE_SECTION)
            config.write(open(cfg_filename).read())
            config.seek(0, os.SEEK_SET)
            cfg.readfp(config)
            cfg_options = dict(cfg.items(FAKE_SECTION))

        # Load/override options from configuration file and command-line 
        args = self._parser.parse_args()
        self.options = dict() 

        # Default values
        self.options.update(self._defaults)

        # Configuration file
        if cfg_options:
            self.options.update(cfg_options)

        # Command line
        arg_options = {k: v for k, v in vars(args).items() if v}
        self.options.update(arg_options)
        self.uptodate = True
        
# Please use add_argument
#DEPRECATED|    def add_option(self, *args, **kwargs):
#DEPRECATED|        """
#DEPRECATED|        Add an option to this Options Singleton.
#DEPRECATED|        Example:
#DEPRECATED|            opt = Options()
#DEPRECATED|            opt.add_option(
#DEPRECATED|                "-U", "--url", dest = "xmlrpc_url",
#DEPRECATED|                help = "API URL", 
#DEPRECATED|                default = 'http://localhost:7080'
#DEPRECATED|            )
#DEPRECATED|        """
#DEPRECATED|
#DEPRECATED|        print "I: add_option has to be replaced by add_argument"
#DEPRECATED|        default = kwargs.get('default', None)
#DEPRECATED|        self._defaults[kwargs['dest']] = default
#DEPRECATED|        if 'default' in kwargs:
#DEPRECATED|            # This is very important otherwise file content is not taken into account
#DEPRECATED|            del kwargs['default']
#DEPRECATED|        kwargs['help'] += " Defaults to %r." % default
#DEPRECATED|        self._parser.add_argument(*args, **kwargs)
#DEPRECATED|        self.uptodate = False
        
    @returns(StringTypes)
    def get_name(self):
        """
        Returns:
            Retrieve the program name (String or None) related to this
            Options Singleton.
        """
        return self._name if self._name else os.path.basename(sys.argv[0])

    @returns(StringTypes)
    def __repr__(self):
        """
        Returns:
            The '%r' representation of this Options Singleton.
        """
        return "<Options: %r>" % self.options

    def add_argument(self, *args, **kwargs):
        """
        Add an option to this Options Singleton.
        Example:
            opt = Options()
            opt.add_option(
                "-U", "--url", dest = "xmlrpc_url",
                help = "API URL", 
                default = 'http://localhost:7080'
            )
        """
        default = kwargs.pop('default', None)
        dest = kwargs.get('dest', None)
        if dest and default:
            self._defaults[dest] = default
        self._parser.add_argument(*args, **kwargs)

    def __getattr__(self, key):
        """
        Getter related to an option.
        Args:
            key: The "dest" corresponding to an option involved in
                this Options Singleton.
        Returns:
            The corresponding value.
        Example:
            opt = Options()
            opt.add_option(
                "-e", "--execute", dest = "execute",
                help = "Execute a shell command", 
                default = None
            )
            foo = Options().execute
        """
        try:

            # We ignore some special keys, since sphinx for example will trigger
            # this function while inspecting classes 
            if key.startswith('__'):
                return

            # Handling default values
            parser_method = getattr(self._parser, key)
            self.uptodate = False
            return parser_method
        except Exception, e:
            if not self.uptodate:
                self.parse()
            return self.options.get(key, None)


#UNUSED|    def __setattr(self, key, value):
#UNUSED|        """
#UNUSED|        Getter related to an option.
#UNUSED|        Args:
#UNUSED|            key: The "dest" corresponding to an option involved in
#UNUSED|                this Options Singleton.
#UNUSED|            value: The value assigned to this option.
#UNUSED|        """
#UNUSED|        self.options[key] = value

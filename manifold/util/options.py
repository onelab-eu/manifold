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

import sys, optparse, cfgparse
import os.path

from types                      import StringTypes
from optparse                   import OptionConflictError

from manifold.util.singleton    import Singleton
from manifold.util.type         import accepts, returns
# http://docs.python.org/dev/library/argparse.html#upgrading-optparse-code

class Options(object):

    __metaclass__ = Singleton

    # We should be able to use another default conf file
    CONF_FILE = '/etc/manifold.conf'
    
    def __init__(self, name = None):
        """
        Constructor
        Args:
            name: A String instance which should contains the basename
                of the program related to these options.
        """
        self._opt = optparse.OptionParser()
        self._defaults = {}
        self._name = name
        self.clear()

    def clear(self):
        """
        Clear options related to this Options Singleton.
        """
        self.options  = {}
        self.add_option(
            "-c", "--config", dest = "cfg_file",
            help = "Config file to use.",
            default = self.CONF_FILE
        )
        self.uptodate = True

    def parse(self):
        """
        Parse options passed from command-line.
        """
        # add options here

        # if we have a logger singleton, add its options here too
        # get defaults too
        
        # Initialize options to default values
        cfg = cfgparse.ConfigParser()
        try:
            cfg.add_optparse_help_option(self._opt)
        except OptionConflictError, e:
            pass

        # Load configuration file
        try:
            cfg_filename = sys.argv[sys.argv.index("-c") + 1]
            try:
                with open(cfg_filename): cfg.add_file(cfg_filename)
            except IOError: 
                raise Exception, "Cannot open specified configuration file: %s" % cfg_filename
        except ValueError:
            try:
                with open(self.CONF_FILE): cfg.add_file(self.CONF_FILE)
            except IOError: pass

        for option_name in self._defaults:
            cfg.add_option(option_name, default = self._defaults[option_name])
            
        # Load/override options from configuration file and command-line 
        (options, args) = cfg.parse(self._opt)
        self.options.update(vars(options))
        self.uptodate = True

    def add_option(self, *args, **kwargs):
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
        default = kwargs.get('default', None)
        self._defaults[kwargs['dest']] = default
        if 'default' in kwargs:
            # This is very important otherwise file content is not taken into account
            del kwargs['default']
        kwargs['help'] += " Defaults to %r." % default
        self._opt.add_option(*args, **kwargs)
        self.uptodate = False
        
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
        if not self.uptodate:
            self.parse()
        return self.options.get(key, None)

    def __setattr(self, key, value):
        """
        Getter related to an option.
        Args:
            key: The "dest" corresponding to an option involved in
                this Options Singleton.
            value: The value assigned to this option.
        """
        self.options[key] = value

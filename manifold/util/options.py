import sys, argparse#, cfgparse
import os.path
import StringIO

from manifold.util.singleton import Singleton
from ConfigParser            import SafeConfigParser

# http://docs.python.org/dev/library/argparse.html#upgrading-optparse-code

# http://stackoverflow.com/questions/2819696/parsing-properties-file-in-python/2819788#2819788

FAKE_SECTION = 'fake'

class Options(object):

    __metaclass__ = Singleton

    # We should be able to use another default conf file
    CONF_FILE = '/etc/manifold.conf'
    
    def __init__(self, name = None):
        self._parser = argparse.ArgumentParser()
        self._defaults = {}
        self._name = name
        self.clear()

    def clear(self):
        self.options  = {}
        self.add_argument(
            "-c", "--config", dest = "cfg_file",
            help = "Config file to use.", metavar = 'FILE',
            default = self.CONF_FILE
        )
        self.uptodate = True

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
                cfg_filename = None

        if cfg_filename:
            config = StringIO.StringIO()
            config.write('[%s]\n' % FAKE_SECTION)
            config.write(open(cfg_filename).read())
            config.seek(0, os.SEEK_SET)

            cfg.readfp(config)
            self.options = dict(cfg.items(FAKE_SECTION))

        # Load/override options from configuration file and command-line 
        args = self._parser.parse_args()
        self.options.update(vars(args))
        print "self.optins=", self.options
        self.uptodate = True
        
    def add_option(self, *args, **kwargs):
        print "I: add_option has to be replaced by add_argument"
        default = kwargs.get('default', None)
        self._defaults[kwargs['dest']] = default
        if 'default' in kwargs:
            # This is very important otherwise file content is not taken into account
            del kwargs['default']
        kwargs['help'] += " Defaults to %r." % default
        self._parser.add_argument(*args, **kwargs)
        self.uptodate = False
        
    def get_name(self):
        return self._name if self._name else os.path.basename(sys.argv[0])

    def __repr__(self):
        return "<Options: %r>" % self.options

    def __getattr__(self, key):
        # Support for cfgparse being coded for optparse and not argparse
        if key == 'option_list':
            return self._parser.option_list

        try:
            parser_method = getattr(self._parser, key)
            self.uptodate = False
            return parser_method
        except Exception, e:
            if not self.uptodate:
                self.parse()
            return self.options.get(key, None)

    def __setattr(self, key, value):
        self.options[key] = value

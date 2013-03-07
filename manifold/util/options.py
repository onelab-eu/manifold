import sys, optparse, cfgparse
import os.path

from manifold.util.singleton    import Singleton

# http://docs.python.org/dev/library/argparse.html#upgrading-optparse-code


class Options(object):

    __metaclass__ = Singleton

    # We should be able to use another default conf file
    CONF_FILE = '/etc/manifold.conf'
    
    def __init__(self, name = None):
        self._opt = optparse.OptionParser()
        self._defaults = {}
        self._name = name

        self.add_option(
            "-c", "--config", dest = "cfg_file",
            help = "Config file to use.",
            default = self.CONF_FILE
        )

    def parse(self):
        """
        \brief Parse options passed from command-line
        """
        # add options here

        # if we have a logger singleton, add its options here too
        # get defaults too
        
        # Initialize options to default values
        cfg = cfgparse.ConfigParser()
        cfg.add_optparse_help_option(self._opt)
        for option_name in self._defaults:
            cfg.add_option(option_name, default = self._defaults[option_name])

        # Load configuration file
        try:
            cfg_filename = sys.argv[sys.argv.index("-c") + 1]
            cfg.add_file(cfg_filename)
        except ValueError:
            cfg.add_file(self.CONF_FILE)

        # Load/override options from configuration file and command-line 
        (options, args) = cfg.parse(self._opt)
        self.__dict__.update(vars(options))

    def add_option(self, *args, **kwargs):
        if 'default' in kwargs:
            self._defaults[kwargs['dest']] = kwargs['default']
            kwargs['help'] += " Defaults to %r." % kwargs['default']
            del kwargs['default']
        self._opt.add_option(*args, **kwargs)
        
    def get_name(self):
        return self._name if self._name else os.path.basename(sys.argv[0])

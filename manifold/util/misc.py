import os, glob, inspect

def find_local_modules():
    __all__ = [ os.path.basename(f)[:-3] for f in
    glob.glob(os.path.dirname(__file__)+"/*.py")]
    print inspect.getouterframes( inspect.currentframe() ) [1]
    print __file__


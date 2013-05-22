import os, glob, inspect
from types import StringTypes


# Define the inclusion operator
class contains(type): pass

def find_local_modules(filepath):
    modules = []
    for f in glob.glob(os.path.dirname(filepath)+"/*.py"):
        name = os.path.basename(f)[:-3]
        if name != '__init__':
            modules.append(name)
    return modules 

def make_list(elt):
    if not elt or isinstance(elt, list):
        return elt
    if isinstance(elt, StringTypes):
        return [elt]
    if isinstance(elt, (tuple, set, frozenset)):
        return list(elt)

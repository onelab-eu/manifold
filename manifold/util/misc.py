import os, glob, inspect, collections, httplib
from types import StringTypes

def find_local_modules(filepath):
    modules = []
    for f in glob.glob(os.path.dirname(filepath)+"/*.py"):
        name = os.path.basename(f)[:-3]
        if name != '__init__':
            modules.append(name)
    return modules 

def make_list(elt):
    if not elt:
        return []
    if isinstance(elt, list):
        return elt
    if isinstance(elt, StringTypes):
        return [elt]
    if isinstance(elt, (tuple, set, frozenset)):
        return list(elt)


# FROM: https://gist.github.com/techtonik/2151727
# Public Domain, i.e. feel free to copy/paste
# Considered a hack in Python 2

import inspect
 
def caller_name(skip=2):
    """Get a name of a caller in the format module.class.method
    
       `skip` specifies how many levels of stack to skip while getting caller
       name. skip=1 means "who calls me", skip=2 "who calls my caller" etc.
       
       An empty string is returned if skipped levels exceed stack height
    """
    stack = inspect.stack()
    start = 0 + skip
    if len(stack) < start + 1:
      return ''
    parentframe = stack[start][0]    
    
    name = []
    module = inspect.getmodule(parentframe)
    # `modname` can be None when frame is executed directly in console
    # TODO(techtonik): consider using __main__
    if module:
        name.append(module.__name__)
    # detect classname
    if 'self' in parentframe.f_locals:
        # I don't know any way to detect call from the object method
        # XXX: there seems to be no way to detect static method call - it will
        #      be just a function call
        name.append(parentframe.f_locals['self'].__class__.__name__)
    codename = parentframe.f_code.co_name
    if codename != '<module>':  # top level usually
        name.append( codename ) # function or a method
    del parentframe
    return ".".join(name)

def is_sublist(x, y, shortcut=None):
    if not shortcut: shortcut = []
    if x == []: return (True, shortcut)
    if y == []: return (False, None)
    if x[0] == y[0]:
        return is_sublist(x[1:],y[1:], shortcut)
    else:
        return is_sublist(x, y[1:], shortcut + [y[0]])

def is_iterable(x):
    return isinstance(x, collections.Iterable) and not isinstance(x, StringTypes)

# Simple function that can be used in a lambda ('x = y' is not allowed)
def dict_set(dic, key, value):
    dic[key] = value
    return dic

def dict_append(dic, key,value):
    dic[key].append(value)
    return dic

def url_exists(site, path):
    conn = httplib.HTTPConnection(site)
    conn.request('HEAD', path)
    response = conn.getresponse()
    conn.close()
    return response.status == 200

# http://stackoverflow.com/questions/8906926/formatting-python-timedelta-objects

# Solution 1:  fmt = "{days} days {hours}:{minutes}:{seconds}"
def strfdelta(tdelta, fmt):
    d = {"days": tdelta.days}
    d["hours"], rem = divmod(tdelta.seconds, 3600)
    d["minutes"], d["seconds"] = divmod(rem, 60)
    return fmt.format(**d)

# Solution 2:  fmt = "%D days %H:%M:%S"
from string import Template

class DeltaTemplate(Template):
    delimiter = "%"

def strfdelta2(tdelta, fmt):
    d = {"D": tdelta.days}
    d["H"], rem = divmod(tdelta.seconds, 3600)
    d["M"], d["S"] = divmod(rem, 60)
    t = DeltaTemplate(fmt)
    return t.substitute(**d)

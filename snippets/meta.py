#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# http://chimera.labs.oreilly.com/books/1230000000393/ch10.html#_solution_179

#DEPRECATED|class ManifoldFinder:
#DEPRECATED|    def find_module(self, fullname, path):
#DEPRECATED|            print('                    Looking for', fullname, path)
#DEPRECATED|            return None
#DEPRECATED|
#DEPRECATED|import sys
#DEPRECATED|sys.meta_path.insert(0, ManifoldFinder())   # Insert as first entry
#DEPRECATED|
#DEPRECATED|from manifold import Tata

class BaseClass(object):
    def __init__(self, classtype):
        self._type = classtype

def ClassFactory(name, argnames=None, BaseClass=BaseClass):
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            # here, the argnames variable is the one passed to the
            # ClassFactory call
            if key not in argnames:
                raise TypeError("Argument %s not valid for %s" 
                    % (key, self.__class__.__name__))
            setattr(self, key, value)
        BaseClass.__init__(self, name[:-len("Class")])
    newclass = type(name, (BaseClass,),{"__init__": __init__})
    return newclass





################################################################################
# MANIFOLD IMPORT
################################################################################

import sys
import importlib.abc
import imp
#from urllib.request import urlopen
#from urllib.error import HTTPError, URLError
#from html.parser import HTMLParser

# Debugging
import logging
log = logging.getLogger(__name__)

#DEPRECATED|# Get links from a given URL
#DEPRECATED|def _get_links(url):
#DEPRECATED|    class LinkParser(HTMLParser):
#DEPRECATED|        def handle_starttag(self, tag, attrs):
#DEPRECATED|            if tag == 'a':
#DEPRECATED|                attrs = dict(attrs)
#DEPRECATED|                links.add(attrs.get('href').rstrip('/'))
#DEPRECATED|
#DEPRECATED|    links = set()
#DEPRECATED|    try:
#DEPRECATED|        #log.debug('Getting links from %s' % url)
#DEPRECATED|        u = urlopen(url)
#DEPRECATED|        parser = LinkParser()
#DEPRECATED|        parser.feed(u.read().decode('utf-8'))
#DEPRECATED|    except Exception as e:
#DEPRECATED|        #log.debug('Could not get links. %s', e)
#DEPRECATED|    #log.debug('links: %r', links)
#DEPRECATED|    return links

def _get_links():
    return ['manifold', 'resource']

# This finder object will be installed as the last entry in sys.meta_path
# Whenever modules are imported, the finders in sys.meta_path are consulted in
# order to locate the module. In this example, the UrlMetaFinder instance
# becomes a finder of last resort that’s triggered when a module can’t be found
# in any of the normal locations.

# The ManifoldFinder wraps the url of a Manifold server instance. Internally, the finder builds a set of valid objects by requesting metadata information to the server. When imports are made, the module name is compared against this set of known objects. If a match can be found, a separate ManifoldModuleLoader class is used to load additional properties from the server and create the resulting module object. One reason for caching the objects is to avoid unnecessary XMLRPC requests on repeated imports.

# DEPRECATED # class ManifoldMetaFinder(importlib.abc.MetaPathFinder):
# DEPRECATED #     def __init__(self, baseurl):
# DEPRECATED #         self._baseurl = baseurl
# DEPRECATED #         self._links   = { }
# DEPRECATED #         self._loaders = { baseurl : ManifoldModuleLoader(baseurl) }
# DEPRECATED # 
# DEPRECATED #     def find_module(self, fullname, path=None):
# DEPRECATED #         #log.debug('find_module: fullname=%r, path=%r', fullname, path)
# DEPRECATED #         print('                    find_module: fullname=%r, path=%r' % (fullname, path))
# DEPRECATED #         if path is None:
# DEPRECATED #             baseurl = self._baseurl
# DEPRECATED #         else:
# DEPRECATED #             if not path[0].startswith(self._baseurl):
# DEPRECATED #                 return None
# DEPRECATED #             baseurl = path[0]
# DEPRECATED # 
# DEPRECATED #         parts = fullname.split('.')
# DEPRECATED #         basename = parts[-1]
# DEPRECATED #         #log.debug('find_module: baseurl=%r, basename=%r', baseurl, basename)
# DEPRECATED #         print('                    find_module: baseurl=%r, basename=%r' % (baseurl, basename))
# DEPRECATED # 
# DEPRECATED #         # Check link cache
# DEPRECATED #         if basename not in self._links:
# DEPRECATED #             self._links[baseurl] = _get_links() # baseurl)
# DEPRECATED # 
# DEPRECATED #         # Check if it's a package
# DEPRECATED #         if basename in self._links[baseurl]:
# DEPRECATED #             #log.debug('find_module: trying package %r', fullname)
# DEPRECATED #             print('                    find_module: trying package %r' % (fullname,))
# DEPRECATED #             fullurl = self._baseurl + '/' + basename
# DEPRECATED #             # Attempt to load the package (which accesses __init__.py)
# DEPRECATED #             print("                    Attempt to load the package: ManifoldPackageLoader")
# DEPRECATED #             loader = ManifoldPackageLoader(fullurl)
# DEPRECATED #             try:
# DEPRECATED #                 loader.load_module(fullname)
# DEPRECATED #                 self._links[fullurl] = _get_links() # fullurl)
# DEPRECATED #                 self._loaders[fullurl] = ManifoldModuleLoader(fullurl)
# DEPRECATED #                 #log.debug('find_module: package %r loaded', fullname)
# DEPRECATED #             except ImportError as e:
# DEPRECATED #                 #log.debug('find_module: package failed. %s', e)
# DEPRECATED #                 loader = None
# DEPRECATED #             return loader
# DEPRECATED # 
# DEPRECATED #         # A normal module
# DEPRECATED #         filename = basename + '.py'
# DEPRECATED #         if filename in self._links[baseurl]:
# DEPRECATED #             #log.debug('find_module: module %r found', fullname)
# DEPRECATED #             return self._loaders[baseurl]
# DEPRECATED #         else:
# DEPRECATED #             #log.debug('find_module: module %r not found', fullname)
# DEPRECATED #             return None
# DEPRECATED # 
# DEPRECATED #     def invalidate_caches(self):
# DEPRECATED #         #log.debug('invalidating link cache')
# DEPRECATED #         self._links.clear()

# Module Loader for a URL
class ManifoldModuleLoader(importlib.abc.SourceLoader):
    def __init__(self, baseurl):
        self._baseurl = baseurl
        self._source_cache = {}

    def module_repr(self, module):
        return '<urlmodule %r from %r>' % (module.__name__, module.__file__)

    # Required method
    # It is used both to load a module and a package
    def load_module(self, fullname):
        print("*** Loading module: %r" % (fullname))
        # First, if you want to create a new module object, you use the
        # imp.new_module() function.
        # Module objects usually have a few expected attributes, including
        # __file__ (the name of the file that the module was loaded from) and
        # __package__ (the name of the enclosing package, if any).
        # Second, modules are cached by the interpreter. The module cache can
        # be found in the dictionary sys.modules. Because of this caching, it’s
        # common to combine caching and module creation together into a single
        # step.
        # The main reason for doing this is that if a module with the given
        # name already exists, you’ll get the already created module instead.

        modulename, classname = fullname.rsplit('.', 1)
        classname = classname[:1].upper() + classname[1:]

        mod = sys.modules.setdefault(modulename, imp.new_module(modulename))

        # The module should already exist and be returned
# DEPRECATED #         mod = sys.modules.setdefault(fullname, imp.new_module(fullname))
# DEPRECATED #         mod.__file__ = self.get_filename(fullname)
# DEPRECATED #         mod.__loader__ = self
# DEPRECATED #         mod.__package__ = fullname.rpartition('.')[0]
# DEPRECATED # 
# DEPRECATED #         mod.__path__ = [ self._baseurl ]
# DEPRECATED #         mod.__package__ = fullname

        # We will create a class inside
        cls = ClassFactory(classname)
        mod.__dict__[classname] = cls
        # setattr(foo, generatedClass.__name__, generatedClass)


        return None # cls

    # Optional extensions
    #def get_code(self, fullname):
    #    src = self.get_source(fullname)
    #    return compile(src, self.get_filename(fullname), 'exec')

    def get_data(self, path):
        pass

    def get_filename(self, fullname):
        return self._baseurl + '/' + fullname.split('.')[-1] + '.py'

    #def get_source(self, fullname):
    #    filename = self.get_filename(fullname)
    #    #log.debug('loader: reading %r', filename)
    #    if filename in self._source_cache:
    #        #log.debug('loader: cached %r', filename)
    #        return self._source_cache[filename]
    #    try:
    #        u = urlopen(filename)
    #        source = u.read().decode('utf-8')
    #        #log.debug('loader: %r loaded', filename)
    #        self._source_cache[filename] = source
    #        return source
    #    except (HTTPError, URLError) as e:
    #        #log.debug('loader: %r failed.  %s', filename, e)
    #        raise ImportError("Can't load %s" % filename)


    def is_package(self, fullname):
        return False

# Package loader for a URL
class ManifoldPackageLoader(ManifoldModuleLoader):
    def load_module(self, fullname):
        
        mod = sys.modules.setdefault(fullname, imp.new_module(fullname))
        mod.__file__ = self.get_filename(fullname)
        mod.__loader__ = self
        mod.__package__ = fullname.rpartition('.')[0]

        mod.__path__ = [ self._baseurl ]
        mod.__package__ = fullname

        return mod

    def get_filename(self, fullname):
        return self._baseurl + '/' + '__init__.py'

    def is_package(self, fullname):
        return True

# Utility functions for installing/uninstalling the loader
_installed_meta_cache = { }
def install_meta(address):
    if address not in _installed_meta_cache:
        finder = ManifoldMetaFinder(address)
        _installed_meta_cache[address] = finder
        sys.meta_path.append(finder)
        #log.debug('%r installed on sys.meta_path', finder)

def remove_meta(address):
    if address in _installed_meta_cache:
        finder = _installed_meta_cache.pop(address)
        sys.meta_path.remove(finder)
        #log.debug('%r removed from sys.meta_path', finder)




# The second approach to customizing import is to write a hook that plugs
# directly into the sys.path variable, recognizing certain directory naming
# patterns. Add the following class and support functions to urlimport.py:

# To use this path-based finder, you simply add manifold:// URLs to sys.path.

# Path finder class for a URL
class ManifoldPathFinder(importlib.abc.PathEntryFinder):
    def __init__(self, baseurl):
        #self._links = None
        #self._loader = ManifoldModuleLoader(baseurl)
        self._baseurl = baseurl

    def is_package(self, basename, fullname):
        return (basename == 'manifold' and fullname == 'manifold') or (basename == 'local' and fullname == 'manifold.local')

    def is_module(self, module, package):
        return True

    def find_loader(self, fullname):
        #log.debug('find_loader: %r', fullname)
        #parts = fullname.split('.')
        #basename = parts[-1]

        mod_base_tuple = fullname.rsplit('.', 1)
        if len(mod_base_tuple) == 1:
            modulename = None
            basename,   = mod_base_tuple
        else:
            modulename, basename = mod_base_tuple

        # Check link cache
#         if self._links is None:
#             self._links = []     # See discussion
#             print("                    calling _get_links")
#             self._links = _get_links() # self._baseurl)

        # Check if it's a package
        if self.is_package(basename, fullname): #if basename in self._links:
            #log.debug('find_loader: trying package %r', fullname)
            fullurl = self._baseurl + '/' + basename
            # Attempt to load the package (which accesses __init__.py)
            loader = ManifoldPackageLoader(fullurl)
            try:
                loader.load_module(fullname)
                #log.debug('find_loader: package %r loaded', fullname)
            except ImportError as e:
                #log.debug('find_loader: %r is a namespace package', fullname)
                print('find_loader: %r is a namespace package', fullname)
                loader = None
            return (loader, [fullurl])

        # A normal module
        filename = basename # + '.py'
        if self.is_module(filename, modulename):
            #log.debug('find_loader: module %r found', fullname)
            if modulename:
                sys.modules[modulename].__dict__[basename] = ClassFactory(basename)
            return (None, []) # (loader, []) # (self._loader, [])
        else:
            #log.debug('find_loader: module %r not found', fullname)
            return (None, [])

    def invalidate_caches(self):
        #log.debug('invalidating link cache')
        #self._links = None
        pass

# Check path to see if it looks like a URL
_url_path_cache = {}
def handle_url(path):
    if path.startswith(('manifoldhttp://', 'manifoldhttps://')):
        #log.debug('Handle path? %s. [Yes]', path)
        if path in _url_path_cache:
            finder = _url_path_cache[path]
        else:
            finder = ManifoldPathFinder(path)
            _url_path_cache[path] = finder
        return finder
    else:
        #log.debug('Handle path? %s. [No]', path)
        pass

def install_path_hook():
    sys.path_hooks.append(handle_url)
    sys.path_importer_cache.clear()
    #log.debug('Installing handle_url')

def remove_path_hook():
    sys.path_hooks.remove(handle_url)
    sys.path_importer_cache.clear()
    #log.debug('Removing handle_url')



################################################################################
# USAGE
################################################################################

def manifold_setup(url):
    import sys
    sys.path.append(url) 
    install_path_hook()

if __name__ == '__main__':

    import sys
    sys.path.append('manifoldhttp://dev.myslice.info:7080')
    install_path_hook()

    from manifold import Resource, Slice

    print("Resource = %r" % (Resource,))

    print("")
    print("Now inst resource")
    r = Resource()
    print(r)

#DEPRECATED|def main():
#DEPRECATED|    name = sys.argv[1]
#DEPRECATED|
#DEPRECATED|    # Register the class passed into parameter into the global scope
#DEPRECATED|    globals()[name] = ClassFactory(name)
#DEPRECATED|    
#DEPRECATED|    t = Toto()
#DEPRECATED|    print t
#DEPRECATED|
#DEPRECATED|main()

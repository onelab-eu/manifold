import xmltodict
# Documentation: https://github.com/martinblech/xmltodict

from types                          import FunctionType, StringTypes
from manifold.core.record           import Record
from manifold.operators.rename      import do_rename
from manifold.util.type             import accepts, returns
from manifold.util.log              import Log

from sfa.util.xrn import urn_to_hrn

RENAME  = 0
HOOKS   = 1

class RSpecPostProcessor(object):
    def __init__(self, cls):
        self.cls = cls

    def __call__(self, path, key, value):
        path_headers = [p[0] for p in path]
        actions = self.cls.get_actions(path_headers)
        
        rename  = actions.get(RENAME)
        hooks    = actions.get(HOOKS)

        if rename:
            value = do_rename(value, rename) 
        if hooks:
            # http://stackoverflow.com/questions/12718187/calling-class-staticmethod-within-the-class-body
            if not isinstance(hooks, list):
                hooks = [hooks]
            for hook in hooks:
                value = hook(value) if type(hook) is FunctionType else hook.__func__(value)
        return key, value


class RSpecParser(object):

    #---------------------------------------------------------------------------
    # RSpec parsing helpers V2
    #
    # used by: OFELIA OCF et VTAM
    #---------------------------------------------------------------------------

    @staticmethod
    def xrn_hook(resource):
        urn = resource.get('component_id')
        if not urn:
            return resource
        resource['urn'] = urn
        resource['hrn'] = urn_to_hrn(urn)[0]
        return resource

    # XXX This is customized for Ofelia OCF
    # this is not really a hostname but the resource name
    @staticmethod
    def hostname_hook(resource):
        hostname = resource.get('hostname')
        if not hostname:
            urn = resource.get('component_id')
            elements = urn.split('+')
            resource['hostname'] = elements[len(elements)-1]
        return resource

    # XXX This is customized for Ofelia OCF
    # urn:publicid:IDN+openflow:ofam:univbris+datapath+05:00:00:00:00:00:00:01
    # urn:publicid:IDN+optical:openflow:ofam:univbris+datapath+00:00:00:00:0a:21:00:0a
    @staticmethod
    def testbed_hook(resource):
        urn = resource.get('component_id')
        Log.tmp(urn)
        if not urn:
            return resource
        elements = urn.split('+')
        if len(elements) > 1:
            authority = elements[1]
            authorities = authority.split(':')
            facility = authorities[len(authorities)-3]
            testbed = authorities[len(authorities)-1]
        else:
            return resource
        resource['facility_name'] = facility
        resource['testbed_name'] = testbed
        return resource

    @staticmethod
    def type_hook(resource_type):
        def _type_hook(resource):
            resource['type'] = resource_type
            return resource
        return _type_hook

    @classmethod
    def get_postprocessor(cls):
        return RSpecPostProcessor(cls)

    @classmethod
    def get_actions(cls, path):
        path_str = '/'.join(path)
        return cls.__actions__.get(path_str, dict())

    @classmethod
    def parse(cls, rspec, rspec_version = None, slice_urn = None):
        resources   = list()
        leases      = list()
        flowspaces  = list()
        # XXX Use OrderedDict for Record ? we might also need functions to
        # reorder
        d = xmltodict.parse(rspec,             \
                postprocessor    = cls.get_postprocessor(), \
                namespaces       = cls.__namespace_map__,   \
                dict_constructor = Record)

        resources, leases, flowspaces = cls.parse_impl(d)

        return {'resource': resources, 'lease': leases, 'flowspace': flowspaces}

    @classmethod
    def build_rspec(cls, slice_hrn, resources, leases, flowspace, rspec_version = None):
        rspec_dict = cls.build_rspec_impl(slice_hrn, resources, leases, flowspace)
        return xmltodict.unparse(rspec_dict, pretty=True)

    #---------------------------------------------------------------------------
    # RSpec parsing helpers V1
    # 
    # used by: NITOS
    #---------------------------------------------------------------------------

    @classmethod
    def get_element_tag(self, element):
        tag = element.tag
        if element.prefix in element.nsmap:
            # NOTE: None is a prefix that can be in the map (default ns)
            start = len(element.nsmap[element.prefix]) + 2 # {ns}tag
            tag = tag[start:]
        
        return tag

    @classmethod
    def prop_from_elt(self, element, prefix = '', list_elements = None):
        """
        Returns a property or a set of properties
        {key: value} or {key: (value, unit)}
        """
        ret = {}
        if prefix: prefix = "%s." % prefix
        tag = self.get_element_tag(element)
 
        if list_elements and tag in list_elements:
            subelement = dict()
            for k, v in element.attrib.items():
                key = "%s%s" % (prefix, k)
                #if list_elements and key in list_elements: # We need the path in list_elements
                #    ret[key] = [v]
                #else:
                subelement[key] = v

            if not tag in ret:
                ret[tag] = list()
            ret[tag].append(subelement)
        else:
            # Analysing attributes
            for k, v in element.attrib.items():
                key = "%s%s.%s" % (prefix, tag, k)
                if list_elements and key in list_elements:
                    ret[key] = [v]
                else:
                    ret[key] = v
 
        # Analysing the tag itself
        if element.text:
            ret["%s%s" % (prefix, tag)] = element.text
 
        # Analysing subtags
        for c in element.getchildren():
            # NOTE When merging fields that are in list_elements, we need to be sure to merge lists correctly
            if list_elements and tag in list_elements:
                subelement = self.prop_from_elt(c, prefix='', list_elements=list_elements)

                # WHY A DICT ????? dict_elements ?
                if not tag in ret:
                    ret[tag] = dict()
                ret[tag].update(subelement)
            else:
                ret.update(self.prop_from_elt(c, prefix=tag, list_elements=list_elements))
 
        # XXX special cases:
        # - tags
        # - units
        # - lists
 
        return ret
 
    @classmethod
    def dict_from_elt(self, network, element, list_elements = None):
        """
        Returns an object
        """
        ret            = {}
        ret['network'] = network
        ret['type']    = self.get_element_tag(element)
 
        for k, v in element.attrib.items():
            ret[k] = v
 
        for c in element.getchildren():
            c_dict = self.prop_from_elt(c, '', list_elements)
            for k, v in c_dict.items():
                if list_elements and k in list_elements:
                    if not k in ret:
                        ret[k] = list()
                    ret[k].extend(v)
                else:
                    ret[k] = v
 
        return ret
 
    # XXX MIGHT NOT WORK AS IS
    @classmethod
    def dict_rename(cls, dic, name):
        """
        Apply map and hooks
        """
        # XXX We might create substructures if the map has '.'
        ret = {}
        for k, v in dic.items():
            if name in cls.MAP and k in cls.MAP[name]:
                ret[cls.MAP[name][k]] = v
            else:
                ret[k] = v
            if name in cls.HOOKS and k in cls.HOOKS[name]:
                ret.update(cls.HOOKS[name][k](v))
            if '*' in cls.HOOKS and k in cls.HOOKS['*']:
                ret.update(cls.HOOKS['*'][k](v))
        if name in cls.HOOKS and '*' in cls.HOOKS[name]:
            ret.update(cls.HOOKS[name]['*'](ret))
        return ret
 
    @returns(StringTypes)
    def __repr__(self):
        return "<%s>" % self.__class__.__name__

    @returns(StringTypes)
    def __str__(self):
        return repr(self) 


import os
import xml.etree.cElementTree as ElementTree

from manifold.core.table import Row
from manifold.core.metadata.method import Method, Methods
from manifold.core.metadata.field import Field, Fields
from manifold.core.metadata.gateway import Gateway, Gateways
from manifold.core.metadata.platform import Platform, Platforms

import json

class XmlListConfig(list):
    def __init__(self, aList):
        for element in aList:
            if element:
                # treat like dict
                if len(element) == 1 or element[0].tag != element[1].tag:
                    self.append(XmlDictConfig(element))
                # treat like list
                elif element[0].tag == element[1].tag:
                    self.append(XmlListConfig(element))
            elif element.text:
                text = element.text.strip()
                if text:
                    self.append(text)


class XmlDictConfig(dict):
    '''
    Example usage:

    >>> tree = ElementTree.parse('your_file.xml')
    >>> root = tree.getroot()
    >>> xmldict = XmlDictConfig(root)

    Or, if you want to use an XML string:

    >>> root = ElementTree.XML(xml_string)
    >>> xmldict = XmlDictConfig(root)

    And then use xmldict for what it is... a dict.
    '''
    def __init__(self, parent_element):
        childrenNames = [child.tag for child in parent_element.getchildren()]

        if parent_element.items(): #attributes
            self.update(dict(parent_element.items()))
        for element in parent_element:
            if element:
                # treat like dict - we assume that if the first two tags
                # in a series are different, then they are all different.
                if len(element) == 1 or element[0].tag != element[1].tag:
                    aDict = XmlDictConfig(element)
                # treat like list - we assume that if the first two tags
                # in a series are the same, then the rest are the same.
                else:
                    # here, we put the list in dictionary; the key is the
                    # tag name the list elements all share in common, and
                    # the value is the list itself
                    aDict = {element[0].tag: XmlListConfig(element)}
                # if the tag has attributes, add those to the dict
                if element.items():
                    aDict.update(dict(element.items()))

                if childrenNames.count(element.tag) > 1:
                    try:
                        currentValue = self[element.tag]
                        currentValue.append(aDict)
                        self.update({element.tag: currentValue})
                    except: #the first of its kind, an empty list must be created
                        self.update({element.tag: [aDict]}) #aDict is written in [], i.e. it will be a list

                else:
                     self.update({element.tag: aDict})
            # this assumes that if you've got an attribute in a tag,
            # you won't be having any text. This may or may not be a
            # good idea -- time will tell. It works for the way we are
            # currently doing XML configuration files...
            elif element.items():
                self.update({element.tag: dict(element.items())})
            # finally, if there are no child tags and no attributes, extract
            # the text
            else:
                self.update({element.tag: element.text})

class Metadata(Row):

    DIRECTORY='/usr/share/manifold/metadata'

    # None means : get everything
    @staticmethod
    def get_default_fields(method, subfields = False):
        if method == 'slices':
            output = ['slice_hrn', 'slice_url', 'slice_description', 'slice_expires', 'authority_hrn']
            if subfields:
                output.append('users.hrn')
                res = Metadata.get_default_fields('resources')
                res = ['resources.%s' % r for r in res]
                output.extend(res)
        elif method == 'resources':
            output = ['resources.hostname', 'resources.network_hrn', 'resources.associated']
        else:
            output = None
        return output

    @staticmethod
    def expand_output_fields(method, output_fields, subfields = False):
        if not output_fields:
            return Metadata.get_default_fields(method, subfields)
        if output_fields == '*':
            return []
        # TODO
        # # = default fields (just like None)
        # * = all fields
        return output_fields

    def get_methods(self):
        return MetadataMethods(self.api, {}, ['platform', 'method', 'fields'])

    def import_file(self, metadata):
        print "I: Processing %s" % metadata
        tree = ElementTree.parse(metadata)
        root = tree.getroot()
        md = XmlDictConfig(root)
        
        # Checking the presence of a platform section
        if not 'platform' in md:
            print "Error importing metadata file '%s': no platform specified" % metadata
            return
        p_dict = md['platform']
        
        # Upserting the platform (this should be handled by TableNew XXX)
        platforms = Platforms(self.api, {'platform': p_dict['platform']})
        if not platforms:
            p_dict['platform_longname'] = p_dict['platform']
            print "I: Inserting new platform '%(platform)s'..." % p_dict
            p = Platform(self.api, p_dict)
            p.sync()
        else:
            print "I: Existing platform '%(platform)s'. Updating..." % p_dict
            p = platforms[0]
            p.update(p_dict)
            p.sync()

        # Let's store gateway related information into the database too
        gateways = MetadataGateways(self.api, {'platform': p['platform']})

        # We add the platform name to the configuration
        config = md['gateway']
        config['platform'] = p['platform']

        if not gateways:
            print "I: Inserting new gateway for platform '%(platform)s'..." % p
            g_dict = {
                'config': json.dumps(config),
                'platform_id': p['platform_id']
            }
            g = MetadataGateway(self.api, g_dict)
            g.sync()
        else:
            print "I: Existing gateway for platform  '%(platform)s'. Updating..." % p
            g = gateways[0]
            g['config'] = json.dumps(config)
            g.sync()

        # Checking the presence of a method section
        if not 'methods' in md:
            print "Error importing metadata file '%s': no method section specified" % metadata
            return
        methods = md['methods']

        # Checking the presence of at least a method
        if not 'method' in methods:
            print "Error importing metadata file '%s': no method specified" % metadata
            return
        methods = methods['method']

        if not isinstance(methods, list):
            methods = [methods]

        # Looping through the methods
        for method in methods:
            
            aliases = method['name'].split('|')

            #base = ['%s::%s' % (p_dict['platform'], aliases[0])]
            #base.extend(aliases)

            # XXX we currently restrict ourselves to the main alias 'nodes'
            tmp = [a for a in aliases if a == 'nodes']
            method_main = tmp[0] if tmp else aliases[0]
            method_dict = {'method': method_main, 'platform_id': p['platform_id']}

            # Upserting the method
            methods = MetadataMethods(self.api, method_dict)
            if not methods:
                print "    I: Inserting new method '%(method)s'..." % method_dict
                m = MetadataMethod(self.api, method_dict)
                m.sync()
            else:
                print "    I: Existing method '%(method)s'. Updating..." % method_dict
                m = methods[0]
                m.update(method_dict)
                m.sync()

            #self.insert_method(base, method['fields']['field'])

            # Checking the presence of a field section
            if not 'fields' in method:
                print "Error importing metadata file '%s': no field section" % metadata
                return
            fields = method['fields']
            
            # Checking the presence of at least a field
            if not 'field' in fields:
                print "Error importing metadata file '%s': no field specified" % metadata
                return
            fields = fields['field']

            # Upserting the fields
            for field_dict in fields:
                field_dict['method_id'] = m['method_id']
                fields = MetadataFields(self.api, {'field': field_dict['field'], 'method_id': m['method_id']})
                if not fields:
                    print "        I: Inserting new field '%(field)s'..." % field_dict
                    f = MetadataField(self.api, field_dict)
                    f.sync()
                else:
                    print "        I: Existing field '%(field)s'. Updating..." % field_dict
                    f = fields[0]
                    f.update(field_dict)
                    f.sync()

        #for k, v in method.items():
        #    print "%s -> %s" % (k,v)

    def import_directory(self):
        for root, dirs, files in os.walk(self.DIRECTORY):
            for d in dirs[:]:
                if d[0] == '.':
                    dirs.remove(d)
            metadata = [f for f in files if f[-3:] == 'xml']
            for m in metadata:
                self.import_file(os.path.join(root, m))

    def clear_platforms(self):
        # Delete all platforms
        pass

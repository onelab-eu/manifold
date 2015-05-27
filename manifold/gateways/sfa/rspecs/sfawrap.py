# RSpec middleware that is in charge of handling rspec parsing and creation through SFAWrap

from sfa.util.xrn                       import Xrn, get_leaf, get_authority, hrn_to_urn, urn_to_hrn
from manifold.gateways.sfa.rspecs       import RSpecParser
from manifold.util.log                  import Log
from sfa.rspecs.rspec                   import RSpec
from types                              import StringTypes

from repoze.lru import lru_cache

import dateutil.parser
import calendar

def set_status(node):
    if 'boot_state' in node:
        #Log.tmp('1 - boot_state = %s' % node['boot_state'])
        if node['boot_state'] == 'disabled':
            node['available'] = 'false'
        elif node['boot_state'] == 'boot':
            node['available'] = 'true'
        else:
            if 'available' in node:
                #Log.tmp('2 - available = %s' % node['available'])
                if node['available'] == 'true':
                    node['boot_state'] = 'available'
                else:
                    node['boot_state'] = 'disabled'
            else:
                #Log.tmp('3 - No available')
                node['boot_state'] = 'available'
            node['available'] = 'true'
    else:
        if 'available' in node:
            #Log.tmp('4 - available = %s' % node['available'])
            if node['available'] == 'true':
                node['boot_state'] = 'available'
            else:
                node['boot_state'] = 'disabled'
        else:
            #Log.tmp('5 - available = %s' % node['available'])
            node['boot_state'] = 'available'
            node['available'] = 'true'

    return node

class SFAWrapParser(RSpecParser):

    #---------------------------------------------------------------------------
    # RSpec parsing
    #---------------------------------------------------------------------------

    @classmethod
    def make_dict_rec(cls, obj):
        if not obj or isinstance(obj, (StringTypes, bool)):
            return obj
        if isinstance(obj, list):
            objcopy = []
            for x in obj:
                objcopy.append(cls.make_dict_rec(x))
            return objcopy
        # We thus suppose we have a child of dict
        objcopy = {}
        for k, v in obj.items():
            objcopy[k] = cls.make_dict_rec(v)
        return objcopy


    @classmethod
    def parse(cls, rspec, rspec_version = 'GENI 3', slice_urn = None):

        rspec = RSpec(rspec, version=rspec_version)

        _resources   = cls._get_resources(rspec)
        _nodes       = cls._get_nodes(rspec)
        _channels    = cls._get_channels(rspec)
        _links       = cls._get_links(rspec)
        _leases      = cls._get_leases(rspec)

        resources = list()
        resources.extend(cls._process_resources(_resources))
        resources.extend(cls._process_nodes(_nodes))
        resources.extend(cls._process_channels(_channels))
        resources.extend(cls._process_links(_links))
        leases = cls._process_leases(_leases)
        return {'resource': resources, 'lease': leases } 

    #---------------------------------------------------------------------------
    # RSpec construction
    #---------------------------------------------------------------------------

    @classmethod
    def build_rspec(cls, slice_urn, resources, leases, flowspace, vms, rspec_version='GENI 3 request'):
        """
        Builds a RSpecs based on the specified resources and leases.

        Args:
            slice_urn (string) : the urn of the slice [UNUSED] [HRN in NITOS ?]

        Returns:
            string : the string version of the created RSpec.
        """
        rspec = RSpec(version=rspec_version)

        nodes = []
        channels = []
        links = []

        # XXX Here it is only about mappings and hooks between ontologies

        for urn in resources:
            # XXX TO BE CORRECTED, this handles None values
            if not urn:
                continue
            resource = dict()
            # TODO: take into account the case where we send a dict of URNs without keys
            #resource['component_id'] = resource.pop('urn')
            resource['component_id'] = urn
            resource_hrn, resource_type = urn_to_hrn(urn) # resource['component_id'])
            # build component_manager_id
            top_auth = resource_hrn.split('.')[0]
            cm = urn.split("+")
            resource['component_manager_id'] = "%s+%s+authority+cm" % (cm[0],top_auth)

            #print "resource_type", resource_type
            if resource_type == 'node':
                #print "NODE", resource, cls
                resource = cls.on_build_resource_hook(resource)
                nodes.append(resource)
            elif resource_type == 'link':
                links.append(resource)
            elif resource_type == 'channel':
                channels.append(resource)
            else:
                raise Exception, "Not supported type of resource" 

        #for node in nodes:
        #    print "NODE:", node

        rspec.version.add_nodes(nodes, rspec_content_type="request")

        #rspec.version.add_links(links)
        #rspec.version.add_channels(channels)

        sfa_leases = cls.manifold_to_sfa_leases(leases, slice_urn)
        #print "sfa_leases", sfa_leases
        if sfa_leases:
            # SFAWRAP BUG ???
            # rspec.version.add_leases bugs with an empty set of leases
            # slice_id = leases[0]['slice_id']
            # TypeError: list indices must be integers, not str
            rspec.version.add_leases(sfa_leases, []) # XXX Empty channels for now
   
        return rspec.toxml()


    #---------------------------------------------------------------------------
    # RSpec parsing helpers
    #---------------------------------------------------------------------------

    # We split those basic functions so that they can be easily overloaded in children classes

    @classmethod
    def _get_resources(cls, rspec):
        # These are all resources 
        # get_resources function can return all resources or a specific type of resource
        try:
            return rspec.version.get_resources()
        except Exception, e:
            return list()

    @classmethod
    def _get_nodes(cls, rspec):
        # XXX does not scale... we need get_resources and that's all
        try:
            return rspec.version.get_nodes()
        except Exception, e:
            #Log.warning("Could not retrieve nodes in RSpec: %s" % e)
            return list()

    @classmethod
    def _get_leases(cls, rspec):
        try:
            return rspec.version.get_leases()
        except Exception, e:
            #Log.warning("Could not retrieve leases in RSpec: %s" % e)
            return list()

    @classmethod
    def _get_links(cls, rspec):
        try:
            return rspec.version.get_links()
        except Exception, e:
            #Log.warning("Could not retrieve links in RSpec: %s" % e)
            return list()

    @classmethod
    def _get_channels(cls, rspec):
        try:
            return rspec.version.get_channels()
        except Exception, e:
            #Log.warning("Could not retrieve channels in RSpec: %s" % e)
            return list()

    @classmethod
    def get_resource_facility_name(cls, urn):
        """
        Returns the resource facility name.
        This is useful for filtering resources in the portal.
        Should be overloaded in child classes to be effective.
        """
        t_urn = urn.split("+")
        # urn:publicid:IDN+fuseco.fokus.fraunhofer.de+node+epc_measurement_server
        # urn:publicid:IDN+wall2.ilabt.iminds.be+node+n097-10b
        return t_urn[1]

    @classmethod
    def get_resource_testbed_name(cls, urn):
        """
        Returns the resource testbed name.
        This is useful for filtering resources in the portal.
        Should be overloaded in child classes to be effective.
        """
        t_urn = urn.split("+")
        # urn:publicid:IDN+fuseco.fokus.fraunhofer.de+node+epc_measurement_server
        # urn:publicid:IDN+wall2.ilabt.iminds.be+node+n097-10b
        return t_urn[1]

    @classmethod
    def _process_resource(cls, resource):
        """
        Postprocess resources read from the RSpec. This applies to nodes, channels and links.
        In particular, it sets the urn, hrn, network_hrn, facility_name and testbed_name fields.
        """
        urn = resource['component_id']
        hrn, type = urn_to_hrn(resource['component_id'])

        resource['urn'] = urn
        resource['hrn'] = hrn

        resource['network_hrn'] = Xrn(resource['component_id']).authority[0] # network ? XXX

        # We also add 'facility' and 'testbed' fields
        resource['facility_name'] = cls.get_resource_facility_name(urn)
        resource['testbed_name']  = cls.get_resource_testbed_name(urn)

        return resource

    @classmethod
    def _process_resources(cls, resources):
        ret = list()

        for resource in resources:
            Log.tmp("LOIC - SFAWrap parser type = %s , resource = %r" % (type(resource),resource))
            new_resource = cls._process_resource(resource)
            if not new_resource:
                continue
            # We suppose we have children of dict that cannot be serialized
            # with xmlrpc, let's make dict
            ret.append(cls.make_dict_rec(new_resource))
        return ret
            
    @classmethod
    def _process_node(cls, node):
        node['type'] = 'node'
        #Log.tmp("node component_id = ",Xrn(node['component_id']))
        #Log.tmp("node authority = ", Xrn(node['component_id']).authority)

        # hostname is used in MySlice display and should never be None
        # some RSpecs like VTAM don't have component_name so we replace it by component_id
        if 'component_name' in node and node['component_name'] is not None:
            node['hostname'] = node['component_name']
        else:
            node['hostname'] = node['component_id']
        node['initscripts'] = node.pop('pl_initscripts')
        if 'exclusive' in node and node['exclusive']:
            node['exclusive'] = node['exclusive'].lower() == 'true'
        if 'granularity' in node:
            node['granularity'] = node['granularity']['grain']

        # XXX This should use a MAP as before
        if 'position' in node: # iotlab
            node['x'] = node['position']['posx']
            node['y'] = node['position']['posy']
            node['z'] = node['position']['posz']
            del node['position']

        if 'location' in node:
            if node['location']:
                node['latitude'] = node['location']['latitude']
                node['longitude'] = node['location']['longitude']
            del node['location']

        # Flatten tags
        if 'tags' in node:
            if node['tags']:
                for tag in node['tags']:
                    node[tag['tagname']] = tag['value']
            del node['tags']
        
        if 'services' in node:
            if node['services']:
                node['login'] = {}
                node['login']['username'] = node['services'][0]['login'][0]['username']
                node['login']['hostname'] = node['services'][0]['login'][0]['hostname']
                del node['services']

        # boot_state and available = true/false
        node = set_status(node)
        return node

    @classmethod
    def _process_nodes(cls, nodes):
        ret = list()

        for node in nodes:
            node = cls._process_node(node)
            if not node: continue

            node = cls._process_resource(node)
            if not node: continue
            
            # We suppose we have children of dict that cannot be serialized
            # with xmlrpc, let's make dict
            ret.append(cls.make_dict_rec(node))
        return ret

    @classmethod
    def _process_channel(cls, channel):
        channel['type'] = 'channel'
        return channel

    @classmethod
    def _process_channels(cls, channels):
        ret = list()
        for channel in channels:
            channel = cls._process_channel(channel)
            if not channel: continue

            channel = cls._process_resource(channel)
            if not channel: continue

            ret.append(channel)

        return ret

    @classmethod
    def _process_link(cls, link):
        link['type'] = 'link'
        return link

    @classmethod
    def _process_links(cls, links):
        ret = list()
        for link in links:
            link = cls._process_link(link)
            if not link: continue

            link = cls._process_resource(link)
            if not link: continue

            ret.append(link)
        return ret

    @classmethod
    def _process_lease(cls, lease):
        lease['resource'] = lease.pop('component_id')
        lease['slice']    = lease.pop('slice_id')
        lease['start_time'] = int(lease['start_time'])
        lease['duration'] = int(lease['duration'])
        if 'end_time' in lease:
            lease['end_time'] = int(lease['end_time'])
        if not 'end_time' in lease and set(['start_time', 'duration']) <= set(lease.keys()):
            lease['end_time'] = lease['start_time'] + lease['duration'] * cls.get_grain()
        elif not 'duration' in lease and  set(lease.keys()) <= set(['start_time', 'end_time']):
            lease['duration'] = (lease['end_time'] - lease['start_time']) / cls.get_grain()

        # XXX GRANULARITY Hardcoded for the moment
        if 'granularity' not in lease:
            lease['granularity'] = cls.get_grain() 
        return lease

    @classmethod
    def _process_leases(cls, leases):
        ret = list()
        for lease in leases:
            new_lease = cls._process_lease(lease)
            if not new_lease:
                continue
            ret.append(new_lease)
        return ret

    #---------------------------------------------------------------------------
    # RSpec construction helpers
    #---------------------------------------------------------------------------

    @classmethod
    def on_build_resource_hook(cls, resource):
        return resource

    @classmethod
    def manifold_to_sfa_leases(cls, leases, slice_urn):
        sfa_leases = []
        for lease in leases:
            sfa_lease = dict()
            # sfa_lease_id = 
            sfa_lease['component_id'] = lease['resource']
            sfa_lease['slice_id']     = slice_urn
            sfa_lease['start_time']   = lease['start_time']
            
            grain = cls.get_grain() # in seconds
            min_duration = cls.get_min_duration() # in seconds
            
            # We either need end_time or duration
            # end_time is choosen if both are specified !
            if 'end_time' in lease:
                sfa_lease['end_time'] = lease['end_time']
# XXX XXX
                duration =  (int(lease['end_time']) - int(lease['start_time'])) / grain
                if duration < min_duration:
                    raise Exception, 'duration < min_duration'
                sfa_lease['duration'] = duration
            elif 'duration' in lease:
                sfa_lease['duration'] = lease['duration']
                sfa_lease['end_time'] = lease['start_time'] + lease['duration']
            else:
                raise Exception, 'Lease not specifying neither end_time nor duration'
            sfa_leases.append(sfa_lease)
        return sfa_leases

    @classmethod
    def get_grain(cls):
        """
        in seconds
        """
        return 1800

    @classmethod
    def get_min_duration(cls):
        return 0
        
class OfeliaVTAMParser(SFAWrapParser):
    @classmethod
    def get_resource_facility_name(cls, urn):
        return "openflow"

    @classmethod
    def get_resource_testbed_name(cls, urn):
        t_urn = urn.split("+")
        # example: urn:publicid:IDN+vtam.univbris+node+cseedurham
        return t_urn[1]

class PLEParser(SFAWrapParser):

    @classmethod
    def get_grain(cls):
        """
        in seconds
        """
        # On PLE the granularity is 1h
        return 3600 # s

    @classmethod
    def get_resource_facility_name(cls, urn):
        return "PlanetLab"

    @classmethod
    def get_resource_testbed_name(cls, urn):
        return "PLE"

    @classmethod
    def on_build_resource_mook(cls, resource):
        # PlanetLab now requires a node to have a list of slivers
        resource['slivers'] = [{'type': 'plab-vnode'}]
        return resource

    @classmethod
    def _process_leases(cls, leases):
        ret = list()
        try:
            for lease in leases:
                lease['resource'] = lease.pop('component_id')
                lease['slice']    = lease.pop('slice_id')
                lease['start_time'] = int(lease['start_time']) - 7200 # BUG GMT+2
                lease['duration'] = int(lease['duration'])
                if 'end_time' in lease:
                    lease['end_time'] = int(lease['end_time']) - 7200
                if not 'end_time' in lease and set(['start_time', 'duration']) <= set(lease.keys()):
                    lease['end_time'] = lease['start_time'] + lease['duration'] * cls.get_grain()
                elif not 'duration' in lease and  set(lease.keys()) <= set(['start_time', 'end_time']):
                    lease['duration'] = (lease['end_time'] - lease['start_time']) / cls.get_grain()

                # XXX GRANULARITY Hardcoded for the moment
                if 'granularity' not in lease:
                    lease['granularity'] = cls.get_grain() 

                ret.append(lease)
        except Exception, e:
            print "EEE::", e
            import traceback
            traceback.print_exc()
        return ret

    @classmethod
    def manifold_to_sfa_leases(cls, leases, slice_urn):
        sfa_leases = []
        for lease in leases:
            sfa_lease = dict()
            # sfa_lease_id = 
            sfa_lease['component_id'] = lease['resource']
            sfa_lease['slice_id']     = slice_urn
            sfa_lease['start_time']   = lease['start_time'] + 7200
            
            grain = cls.get_grain() # in seconds
            min_duration = cls.get_min_duration() # in seconds
            
            # We either need end_time or duration
            # end_time is choosen if both are specified !
            if 'end_time' in lease:
                sfa_lease['end_time'] = lease['end_time']  + 7200
# XXX XXX
                duration =  (int(lease['end_time']) - int(lease['start_time'])) / grain
                if duration < min_duration:
                    raise Exception, 'duration < min_duration'
                sfa_lease['duration'] = duration
            elif 'duration' in lease:
                sfa_lease['duration'] = lease['duration']
                sfa_lease['end_time'] = lease['start_time'] + lease['duration']
            else:
                raise Exception, 'Lease not specifying neither end_time nor duration'
            sfa_leases.append(sfa_lease)
        return sfa_leases

class LaboraParser(SFAWrapParser):

    @classmethod
    def get_resource_facility_name(cls, urn):
        return "Labora"

    @classmethod
    def get_resource_testbed_name(cls, urn):
        if not urn:
            return "Labora"
        elements = urn.split('+')
        if len(elements) > 1:
            authority = elements[1]
            authorities = authority.split(':')
            testbed = authorities[len(authorities)-1]
            return testbed
        else:
            return "Labora"


    # The only change for Labora is the tag exclusive which is a tag inside the node tag and not a property
    @classmethod
    def _process_node(cls, node):
        node['type'] = 'node'
        node['network_hrn'] = Xrn(node['component_id']).authority[0] # network ? XXX
        node['hrn'] = urn_to_hrn(node['component_id'])[0]
        node['urn'] = node['component_id']
        node['hostname'] = node['component_name']
        node['initscripts'] = node.pop('pl_initscripts')

        # All Labora nodes are exclusive = true
        node['exclusive'] = 'true'

        if 'granularity' in node:
            node['granularity'] = node['granularity']['grain']

        # XXX This should use a MAP as before
        if 'position' in node: # iotlab
            node['x'] = node['position']['posx']
            node['y'] = node['position']['posy']
            node['z'] = node['position']['posz']
            del node['position']

        if 'location' in node:
            if node['location']:
                node['latitude'] = node['location']['latitude']
                node['longitude'] = node['location']['longitude']
            del node['location']

        # Flatten tags
        if 'tags' in node:
            if node['tags']:
                for tag in node['tags']:
                    node[tag['tagname']] = tag['value']
            del node['tags']

        return node


    # The only change for Labora is the date format which is "yyyy-mm-dd hh:mm:ss" and not a timestamp
    @classmethod
    def manifold_to_sfa_leases(cls, leases, slice_urn):
        from datetime import datetime
        sfa_leases = []
        for lease in leases:
            sfa_lease = dict()
            # sfa_lease_id = 
            sfa_lease['component_id'] = lease['resource']
            sfa_lease['slice_id']     = slice_urn
            sfa_lease['start_time']   = lease['start_time']
            
            grain = cls.get_grain() # in seconds
            min_duration = cls.get_min_duration() # in seconds
            
            # We either need end_time or duration
            # end_time is choosen if both are specified !
            if 'end_time' in lease:
                sfa_lease['end_time'] = lease['end_time']

                duration =  (int(lease['end_time']) - int(lease['start_time'])) / grain
                if duration < min_duration:
                    raise Exception, 'duration < min_duration'
                sfa_lease['duration'] = duration
            elif 'duration' in lease:
                sfa_lease['duration'] = lease['duration']
                sfa_lease['end_time'] = lease['start_time'] + lease['duration']
            else:
                raise Exception, 'Lease not specifying neither end_time nor duration'
            # timestamp -> UTC YYYY-MM-DD hh:mm:ss
            Log.tmp("manifold to sfa - convert timestamp %s to UTC", sfa_lease['start_time'])
            sfa_lease['start_time'] = datetime.utcfromtimestamp(int(sfa_lease['start_time'])).strftime('%Y-%m-%d %H:%M:%S')
            Log.tmp("manifold to sfa - convert timestamp to UTC %s", sfa_lease['start_time'])
            sfa_lease['end_time'] = datetime.utcfromtimestamp(int(sfa_lease['end_time'])).strftime('%Y-%m-%d %H:%M:%S')
            sfa_leases.append(sfa_lease)
        return sfa_leases

    @classmethod
    def _process_leases(cls, leases):
        from datetime import datetime
        import time
        import dateutil.parser 
        import calendar
        ret = list()
        try:
            for lease in leases:
                lease['resource'] = lease.pop('component_id')
                lease['slice']    = lease.pop('slice_id')

                # UTC YYYY-MM-DD hh:mm:ss -> timestamp
                Log.tmp("PARSING - convert UTC %s to timestamp", lease['start_time'])
                lease['start_time'] = calendar.timegm(dateutil.parser.parse(lease['start_time']).utctimetuple())
                Log.tmp("PARSING - convert UTC to timestamp %s", lease['start_time'])
                lease['duration'] = int(lease['duration'])
                if 'end_time' in lease:
                    lease['end_time'] = int(lease['end_time'])
                if not 'end_time' in lease and set(['start_time', 'duration']) <= set(lease.keys()):
                    lease['end_time'] = lease['start_time'] + lease['duration'] * cls.get_grain()
                elif not 'duration' in lease and  set(lease.keys()) <= set(['start_time', 'end_time']):
                    lease['duration'] = (lease['end_time'] - lease['start_time']) / cls.get_grain()

                # XXX GRANULARITY Hardcoded for the moment
                if 'granularity' not in lease:
                    lease['granularity'] = cls.get_grain() 

                ret.append(lease)
        except Exception, e:
            print "EEE::", e
            import traceback
            traceback.print_exc()
        return ret


class NITOSParser(SFAWrapParser):

    @classmethod
    def get_resource_facility_name(cls, urn):
        return "NITOS"

    @classmethod
    def get_resource_testbed_name(cls, urn):
        return "NITOS"

    @classmethod
    def on_build_resource_hook(cls, resource):
        resource['granularity'] = {'grain': 1800}
        return resource


class WiLabtParser(SFAWrapParser):

    # XXX Hack: using a different parsing for Manifest
    # XXX As long as WiLab doesn't support Leases in RSpecs
    # XXX We will generate leases based on the resources in the Manifest
    @classmethod
    def parse_manifest(cls, rspec, rspec_version = 'GENI 3', slice_urn = None, start_time = None):
        rspec = RSpec(rspec, version=rspec_version)

        _resources   = cls._get_resources(rspec)
        _nodes       = cls._get_nodes(rspec)
        # XXX Not supported yet
        #_channels    = cls._get_channels(rspec)
        #_links       = cls._get_links(rspec)
        _leases      = cls._get_leases(rspec)

        # XXX Until WiLab supports Leases
        end_time     = cls._get_expiration(rspec)
        if start_time is None:
            start_time = 1388530800


        resources = list()
        resources.extend(cls._process_resources(_resources))
        resources.extend(cls._process_nodes(_nodes))
        #resources.extend(cls._process_channels(_channels))
        #resources.extend(cls._process_links(_links))

        Log.warning("XXX Until WiLab supports Leases")
        # XXX Generate Leases based on the Resources instead of Leases
        leases = cls._process_leases(resources, slice_urn, start_time, end_time)
        return {'resource': resources, 'lease': leases }

    # Returns a timestamp
    @classmethod
    def _get_expiration(cls, rspec):
        # get the expires tag in the header of the RSpec
        # convert it to timestamp

        # XXX Until WiLab supports Leases
        # this will be used as lease['end_time']
        try:
            rspec_string = rspec.toxml()
            import xml.etree.ElementTree as ET
            rspec = ET.fromstring(rspec_string)
            expiration = rspec.get("expires")
            import time
            from datetime import datetime
            ret = int(time.mktime(datetime.strptime(expiration, "%Y-%m-%dT%H:%M:%SZ").timetuple()))
            return ret
        # XXX To be removed in Router-v2
        except Exception, e:
            import traceback
            Log.warning("Exception in _get_expiration: %s" % e)
            traceback.print_exc()
            return None

    @classmethod
    def _process_leases(cls, leases, slice_urn, start_time, end_time):
        ret = list()
        try:
            for lease in leases:
                lease['slice_urn'] = slice_urn
                lease['start_time'] = start_time
                lease['end_time'] = end_time
                # duration in seconds from now till end_time
                duration = end_time - start_time
                # duration in minutes
                duration = duration / 60
                lease['duration'] = int(duration)
                new_lease = cls._process_lease(lease)
                if not new_lease:
                    continue
                ret.append(new_lease)
        # XXX To be removed in Router-v2
        except Exception, e:
            import traceback
            Log.warning("Exception in _process_leases: %s" % e)
            traceback.print_exc()
        return ret

    @classmethod
    def get_resource_facility_name(cls, urn):
        return "Wireless"

    @classmethod
    def get_resource_testbed_name(cls, urn):
        # TODO w-iLab.t (a facility with two individual testbeds).
        return "w-iLab.t"

    # WiLab AM returns a Manifest with nodes that are reserved in WiLab
    # But also the nodes that are not under its Authority
    #
    # Ex: A request for nodes 1 & 2 of WiLab, but also some nitos nodes, channels and leases
    #     WiLab answer that node 1 has been reserved 
    #
    # Request RSpec  = <node5 nitos> <channel nitos> <lease nitos> <node1 wilab> <node2 wilab>
    # Manifest RSpec = <node5 nitos> <channel nitos> <lease nitos> <node1 wilab> 
    #
    # Therefore, we have to filter out the resources that are not managed by WiLab authority
    # But that are announced in the WiLab Manifest


    # XXX Why nodes have component_manager_id and links have component_manager ???

    # node uses component_manager_id

    @classmethod
    def _process_node(cls, node):
        authority = 'urn:publicid:IDN+wilab2.ilabt.iminds.be+authority+cm'
        if (not 'component_manager_id' in node) or (node['component_manager_id'] != authority):
            Log.warning("Authority is not WiLab - Ignore node = ",node)
            #return None
        return super(WiLabtParser, cls)._process_node(node) 

    # link uses component_manager
    
    @classmethod
    def _process_link(cls, link):
        authority = 'urn:publicid:IDN+wilab2.ilabt.iminds.be+authority+cm'
        if (not 'component_manager' in link) or (link['component_manager'] != authority):
            Log.warning("Authority is not WiLab - Ignore link = ",link)
            #return None
        return super(WiLabtParser, cls)._process_link(link) 

    @classmethod
    def _process_channel(cls, channel):
        authority = 'urn:publicid:IDN+wilab2.ilabt.iminds.be+authority+cm'
        if (not 'component_manager_id' in channel) or (channel['component_manager_id'] != authority):
            Log.warning("Authority is not WiLab - Ignore channel = ",channel)
            #return None
        return super(WiLabtParser, cls)._process_channel(channel) 

    @classmethod
    def _process_lease(cls, lease):
        # Keep only necessary information in leases
        new_lease = dict()
        authority = 'urn:publicid:IDN+wilab2.ilabt.iminds.be+authority+cm'
        if (not 'component_manager_id' in lease) or (lease['component_manager_id'] != authority):
            Log.warning("Authority is not WiLab - Ignore lease = ",lease)
            #return None
        new_lease['resource'] = lease.pop('component_id')
        new_lease['lease_id'] = None
        new_lease['slice']    = lease.pop('slice_urn')
        new_lease['start_time'] = int(lease['start_time'])
        new_lease['duration'] = int(lease['duration'])
        if 'end_time' in lease:
            new_lease['end_time'] = int(lease['end_time'])
        if not 'end_time' in lease and set(['start_time', 'duration']) <= set(lease.keys()):
            new_lease['end_time'] = lease['start_time'] + lease['duration'] * cls.get_grain()
        elif not 'duration' in lease and  set(lease.keys()) <= set(['start_time', 'end_time']):
            new_lease['duration'] = (lease['end_time'] - lease['start_time']) / cls.get_grain()

        # XXX GRANULARITY Hardcoded for the moment
        if 'granularity' not in lease:
            new_lease['granularity'] = cls.get_grain()
        else:
            new_lease['granularity'] = lease['granularity']

        return new_lease

    @classmethod
    def on_build_resource_hook(cls, resource):
        resource['sliver_type'] = "raw-pc"
        return resource

    @classmethod
    def build_rspec(cls, slice_urn, resources, leases, flowspace, vms, rspec_version='GENI 3 request'):
        """
        Builds a RSpecs based on the specified resources and leases.

        Args:
            slice_urn (string) : the urn of the slice [UNUSED] [HRN in NITOS ?]

        Returns:
            string : the string version of the created RSpec.
        """
        import time
        start_time = None
        end_time = None

        # Default duration for WiLab is 2 hours
        duration_default = 120
        for lease in leases:
            if 'end_time' in lease:
                end_time = lease['end_time']
                start_time = lease['start_time']
                break

        if start_time is None:
            # start_time = Now
            start_time = time.time()

        if end_time is None:
            end_time = int(start_time + duration_default*60)
            #raise Exception, "end_time is mandatory in leases"

        # duration in seconds from now till end_time
        duration = end_time - start_time
        # duration in minutes
        duration = duration / 60
        duration = int(duration)
        if duration < duration_default:
            duration = duration_default
        Log.tmp("start_time = ",start_time)
        Log.tmp("end_time = ",end_time)
        Log.tmp("duration = ",duration)
        # RSpec will have expires date = now + duration
        rspec = RSpec(version=rspec_version, ttl=duration, expires=end_time)

        nodes = []
        channels = []
        links = []

        # XXX Here it is only about mappings and hooks between ontologies
        i = 0
        for urn in resources:
            # XXX TO BE CORRECTED, this handles None values
            if not urn:
                continue
            resource = dict()
            # TODO: take into account the case where we send a dict of URNs without keys
            #resource['component_id'] = resource.pop('urn')
            resource['component_id'] = urn
            resource_hrn, resource_type = urn_to_hrn(urn) # resource['component_id'])
            # build component_manager_id

            # The only change for WiLab compared to Generic SFAWrapParser
            cm = urn.split("+")
            resource['component_manager_id'] = "%s+%s+authority+cm" % (cm[0],cm[1])

            #print "resource_type", resource_type
            if resource_type == 'node':
                #print "NODE", resource, cls
                resource['client_id'] = "PC" + str(i)
                resource = cls.on_build_resource_hook(resource)
                nodes.append(resource)
            elif resource_type == 'link':
                links.append(resource)
            elif resource_type == 'channel':
                channels.append(resource)
            else:
                raise Exception, "Not supported type of resource" 

            i = i + 1
        #for node in nodes:
        #    print "NODE:", node

        rspec.version.add_nodes(nodes, rspec_content_type="request")
        #rspec.version.add_links(links)
        #rspec.version.add_channels(channels)

        #sfa_leases = cls.manifold_to_sfa_leases(leases, slice_urn)
        ##print "sfa_leases", sfa_leases
        #if sfa_leases:
        #    # SFAWRAP BUG ???
        #    # rspec.version.add_leases bugs with an empty set of leases
        #    # slice_id = leases[0]['slice_id']
        #    # TypeError: list indices must be integers, not str
        #    rspec.version.add_leases(sfa_leases, []) # XXX Empty channels for now
        return rspec.toxml()

class VirtualWallParser(WiLabtParser):

    @classmethod
    def get_resource_facility_name(cls, urn):
        return "Emulab"

    @classmethod
    def get_resource_testbed_name(cls, urn):
        t_urn = urn.split("+")
        # urn:publicid:IDN+wall2.ilabt.iminds.be+node+n097-01a
        return t_urn[1]

    @classmethod
    def _process_node(cls, node):
        authority = 'urn:publicid:IDN+wall2.ilabt.iminds.be+authority+cm'
        if (not 'component_manager_id' in node) or (node['component_manager_id'] != authority):
            Log.warning("Authority is not wall2 - Ignore node = ",node)
            #return None
        return super(WiLabtParser, cls)._process_node(node) 

    # link uses component_manager
    
    @classmethod
    def _process_link(cls, link):
        authority = 'urn:publicid:IDN+wall2.ilabt.iminds.be+authority+cm'
        if (not 'component_manager' in link) or (link['component_manager'] != authority):
            Log.warning("Authority is not wall2 - Ignore link = ",link)
            #return None
        return super(WiLabtParser, cls)._process_link(link) 

    @classmethod
    def _process_channel(cls, channel):
        authority = 'urn:publicid:IDN+wall2.ilabt.iminds.be+authority+cm'
        if (not 'component_manager_id' in channel) or (channel['component_manager_id'] != authority):
            Log.warning("Authority is not wall2 - Ignore channel = ",channel)
            #return None
        return super(WiLabtParser, cls)._process_channel(channel) 

    @classmethod
    def _process_lease(cls, lease):
        # Keep only necessary information in leases
        new_lease = dict()
        authority = 'urn:publicid:IDN+wall2.ilabt.iminds.be+authority+cm'
        if (not 'component_manager_id' in lease) or (lease['component_manager_id'] != authority):
            Log.warning("Authority is not wall2 - Ignore lease = ",lease)
            #return None

class IoTLABParser(SFAWrapParser):

    @classmethod
    def get_resource_facility_name(cls, urn):
        return "IoTLAB"

    @classmethod
    def get_resource_testbed_name(cls, urn):
        t_urn = urn.split('+')
        testbed = t_urn[3].split('.')[1]
        # XXX Ugly hack for the moment
        if testbed == 'iii':
            return 'iii.org.tw'
        else:
            return testbed.title()

    @classmethod
    def _process_resource(cls, resource):
        """
        Postprocess resources read from the RSpec. This applies to nodes, channels and links.
        In particular, it sets the urn, hrn, network_hrn, facility_name and testbed_name fields.
        """
        urn = resource['component_id']
        hrn, type = urn_to_hrn(resource['component_id'])

        resource['urn'] = urn
        resource['hrn'] = hrn

        resource['network_hrn'] = Xrn(resource['component_id']).authority[0] # network ? XXX

        # We also add 'facility' and 'testbed' fields
        resource['facility_name'] = cls.get_resource_facility_name(urn)
        resource['testbed_name']  = cls.get_resource_testbed_name(urn)

        #if 'location' in node:
        #    if node['location']:
        #        node['latitude'] = node['location']['latitude']
        #        node['longitude'] = node['location']['longitude']
        #    del node['location']
        #else:
        # if the location is not provided, aproximate it from the city
        t_urn = resource['urn'].split('+')
        city = t_urn[3].split('.')[1]
        if city == 'iii':
            city = 'Institute for Information Industry, Taïwan 106'
            resource['country'] = 'Taiwan'
        else:
            resource['country'] = 'France'
        location = cls.get_location(city)
        if location is not None:
            resource['latitude'] = str(location.latitude)
            resource['longitude'] = str(location.longitude)

        return resource

    @classmethod
    @lru_cache(100)
    def get_location(cls, city):
        location = None
        try:
            #from geopy.geocoders import Nominatim
            #geolocator = Nominatim()
            #from geopy.geocoders import GeoNames
            #geolocator = GeoNames()
            from geopy.geocoders import GoogleV3
            geolocator = GoogleV3()
          
            location = geolocator.geocode(city)
        except Exception, e:
            Log.warning("geopy.geocoders failed to get coordinates for city = ",city)
            Log.warning(e)
        return location

    @classmethod
    def on_build_resource_hook(cls, resource):
        # XXX How do we know valid sliver types ?
        resource['slivers'] = [{'type': 'iotlab-node'}]
        return resource

    @classmethod
    def get_grain(cls):
        return 60 # s

    @classmethod
    def get_min_duration(cls):
        return 10 # * cls.get_grain()
        # XXX BTW, can we do duration = 61 s, or shall it be a multiple of min_duration ???

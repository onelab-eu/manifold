# RSpec middleware that is in charge of handling rspec parsing and creation through SFAWrap

from sfa.util.xrn                       import Xrn, get_leaf, get_authority, hrn_to_urn, urn_to_hrn
from manifold.gateways.sfa.rspecs       import RSpecParser
from manifold.util.log                  import Log
from sfa.rspecs.rspec                   import RSpec
from types                              import StringTypes

import dateutil.parser
import calendar

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
        Log.tmp("_nodes = ",_nodes)       
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
    def build_rspec(cls, slice_urn, resources, leases, rspec_version='GENI 3 request'):
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
    def _process_resource(cls, resource):
        resource['urn'] = resource['component_id']
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
        node['network_hrn'] = Xrn(node['component_id']).authority[0] # network ? XXX
        node['hrn'] = urn_to_hrn(node['component_id'])[0]
        node['urn'] = node['component_id']
        node['hostname'] = node['component_name']
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
        return node

    @classmethod
    def _process_nodes(cls, nodes):
        ret = list()

        for node in nodes:
            new_node = cls._process_node(node)
            if not new_node:
                continue
            
            # We suppose we have children of dict that cannot be serialized
            # with xmlrpc, let's make dict
            ret.append(cls.make_dict_rec(new_node))
        return ret

    @classmethod
    def _process_channel(cls, channel):
        channel['type'] = 'channel'
        return channel

    @classmethod
    def _process_channels(cls, channels):
        ret = list()
        for channel in channels:
            new_channel = cls._process_channel(channel)
            if not new_channel:
                continue
            ret.append(new_channel)
        return ret

    @classmethod
    def _process_link(cls, link):
        link['urn'] = link['component_id']
        #if not 'type' in link: # XXX Overrides
        link['type'] = 'link'
        return link

    @classmethod
    def _process_links(cls, links):
        ret = list()
        for link in links:
            new_link = cls._process_link(link)
            if not new_link:
                continue
            ret.append(new_link)
        return ret

    @classmethod
    def _process_lease(cls, lease):
        print "new lease", lease
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
        
    
class PLEParser(SFAWrapParser):

    @classmethod
    def get_grain(cls):
        """
        in seconds
        """
        # On PLE the granularity is 1h
        return 3600 # s

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
    def on_build_resource_hook(cls, resource):
        resource['granularity'] = {'grain': 1800}
        return resource

class WiLabtParser(SFAWrapParser):

    @classmethod
    def _process_node(cls, node):
        if node['component_id'] != 'urn:publicid:IDN+wilab2.ilabt.iminds.be+authority+cm':
            return None
        return SFAWrapParser._process_node(cls, node)

    @classmethod
    def _process_link(cls, link):
        if link['component_id'] != 'urn:publicid:IDN+wilab2.ilabt.iminds.be+authority+cm':
            return None
        return SFAWrapParser._process_link(cls, link)

    @classmethod
    def _process_channel(cls, channel):
        if channel['component_id'] != 'urn:publicid:IDN+wilab2.ilabt.iminds.be+authority+cm':
            return None
        return SFAWrapParser._process_channel(cls, channel)

    @classmethod
    def _process_lease(cls, lease):
        if lease['component_id'] != 'urn:publicid:IDN+wilab2.ilabt.iminds.be+authority+cm':
            print "ignored wilab lease"
            return None
        return SFAWrapParser._process_lease(cls, lease)

    @classmethod
    def on_build_resource_hook(cls, resource):
        resource['client_id'] = "PC"
        resource['sliver_type'] = "raw-pc"
        return resource

    @classmethod
    def build_rspec(cls, slice_urn, resources, leases, rspec_version='GENI 3 request'):
        """
        Builds a RSpecs based on the specified resources and leases.

        Args:
            slice_urn (string) : the urn of the slice [UNUSED] [HRN in NITOS ?]

        Returns:
            string : the string version of the created RSpec.
        """
        import time
        end_time = time.time()
        start_time = None

        # Default duration for WiLab is 2 hours
        duration = 120
        for lease in leases:
            if 'end_time' in lease:
                if lease['end_time'] > end_time:
                    end_time = lease['end_time']
                    start_time = lease['start_time']
        if start_time is not None:
            # duration in seconds from now till end_time
            duration = end_time - time.time()
            # duration in minutes
            duration = duration / 60
        Log.tmp("end_time = ",end_time)
        Log.tmp("duration = ",duration)
        # RSpec will have expires date = now + duration
        rspec = RSpec(version=rspec_version, ttl=duration)

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

            # The only change for WiLab compared to Generic SFAWrapParser
            cm = urn.split("+")
            resource['component_manager_id'] = "%s+%s+authority+cm" % (cm[0],cm[1])

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

        #sfa_leases = cls.manifold_to_sfa_leases(leases, slice_urn)
        ##print "sfa_leases", sfa_leases
        #if sfa_leases:
        #    # SFAWRAP BUG ???
        #    # rspec.version.add_leases bugs with an empty set of leases
        #    # slice_id = leases[0]['slice_id']
        #    # TypeError: list indices must be integers, not str
        #    rspec.version.add_leases(sfa_leases, []) # XXX Empty channels for now
   
        return rspec.toxml()


class IoTLABParser(SFAWrapParser):

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

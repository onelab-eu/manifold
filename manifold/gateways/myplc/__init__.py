#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Gateway managing MyPLC / PLE
#   http://www.planet-lab.eu
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC

from types                              import StringTypes

from manifold.core.announce             import Announces, announces_from_docstring
from manifold.gateways                  import Gateway
from manifold.util.log                  import Log
from manifold.util.predicate            import eq, included
from manifold.util.reactor_thread       import ReactorThread
from manifold.util.type                 import accepts, returns

from manifold.gateways.object           import ManifoldObject, ManifoldCollection

DEFAULT_API_URL = "https://www.planet-lab.eu:443/PLCAPI/"

MAP_METHOD = {
    "node" : "GetNodes",
    "site" : "GetSites",
    "myplcuser" : "GetPersons",
}

class MyPLCCollection(ManifoldCollection):

    def __init__(self, cls = None):
        ManifoldCollection.__init__(self, cls)

        self._proxy = None

    def callback_records(self, rows, packet):
        """
        (Internal usage) See ManifoldGateway::receive_impl.
        Args:
            packet: A QUERY Packet.
            rows: The corresponding list of dict or Record instances.
        """
        self.get_gateway().records(rows, packet)

    def callback_error(self, error, packet):
        """
        (Internal usage) See ManifoldGateway::receive_impl.
        Args:
            packet: A QUERY Packet.
            error: The corresponding error message.
        """
        Log.error("Error during Manifold call: %r" % error)
        self.get_gateway().last(packet) # XXX Should be error

    @staticmethod
    def manifold_to_myplc_filter(filter):
        myplc_filter = dict()
        for predicate in filter:
            key, op, value = predicate.get_tuple()
            if op in [eq, included]:
                op_str = ''
            else:
                op_str = predicate.get_str_op()

            if isinstance(key, tuple):
                if len(key) == 1:
                    key = key[0]
                    print "VALUE=", value
                    value = value[0]
                else:
                    raise NotImplemented
            # tuples ?

            myplc_filter["%(op_str)s%(key)s" % locals()] = value
        if not myplc_filter:
            return None
        return myplc_filter

    def _get_auth(self):
        config = self.get_gateway().get_config()
        return {
            'AuthMethod': 'password',
            'Username'  : config['username'],
            'AuthString': config['password'],
        }

    
    def _get_proxy(self):
        if self._proxy:
            return self._proxy

        # The default xmlrpc.Proxy does not work, we need to use ReactorThread()...
        # XXX Share this code among manifold
        from twisted.web import xmlrpc
        class Proxy(xmlrpc.Proxy):
            ''' See: http://twistedmatrix.com/projects/web/documentation/howto/xmlrpc.html
                this is eacly like the xmlrpc.Proxy included in twisted but you can
                give it a SSLContext object insted of just accepting the defaults..
            '''
            def setSSLClientContext(self,SSLClientContext):
                self.SSLClientContext = SSLClientContext
            def callRemote(self, method, *args):
                def cancel(d):
                    factory.deferred = None
                    connector.disconnect()
                factory = self.queryFactory(
                    self.path, self.host, method, self.user,
                    self.password, self.allowNone, args, cancel, self.useDateTime)
                #factory = xmlrpc._QueryFactory(
                #    self.path, self.host, method, self.user,
                #    self.password, self.allowNone, args)

                if self.secure:
                    try:
                        self.SSLClientContext
                    except NameError:
                        print "Must Set a SSL Context"
                        print "use self.setSSLClientContext() first"
                        # Its very bad to connect to ssl without some kind of
                        # verfication of who your talking to
                        # Using the default sslcontext without verification
                        # Can lead to man in the middle attacks
                    ReactorThread().connectSSL(self.host, self.port or 443,
                                       factory, self.SSLClientContext,
                                       timeout=self.connectTimeout)

                else:
                    ReactorThread().connectTCP(self.host, self.port or 80, factory, timeout=self.connectTimeout)
                return factory.deferred

        config  = self.get_gateway().get_config()
        api_url = config.get('api_url', DEFAULT_API_URL)
        api_url = api_url.encode('latin-1')
        self._proxy = Proxy(api_url, allowNone = True)

        # XXX We should provide a default SSL Context...
        from twisted.internet import ssl
        self._proxy.setSSLClientContext(ssl.ClientContextFactory())
        return self._proxy

    def get(self, packet):
        destination = packet.get_destination()

        # Need to transform manifold fields into query fields, and then back in
        # callback_records

        ## Insert a RENAME Node above this FROM Node if necessary.
        #instance = self.get_object(query.get_from())
        #aliases  = instance.get_aliases()
        #if aliases:
        #    Log.warning("I don't think this properly recables everything")
        #    try:
        #        Rename(self, aliases)
        #    except Exception, e:
        #        print "EEE:", e

        myplc_filter      = self.manifold_to_myplc_filter(destination.get_filter())
        myplc_field_names = destination.get_field_names()
        if myplc_field_names:
            myplc_field_names = list(myplc_field_names)

        args = (self.__myplc_method__, self._get_auth())
        if myplc_filter or myplc_field_names:
            args += (myplc_filter, )
        if myplc_field_names:
            args += (myplc_field_names, )
        print "ARGS", args
        d = self._get_proxy().callRemote(*args)
        d.addCallback(self.callback_records, packet)
        d.addErrback(self.callback_error, packet)


class MyPLCNodeCollection(MyPLCCollection):
    """
    class node {
        unsigned node_id;
        unsigned last_updated;      /** Date and time when node entry was created */
        string key;                 /** (Admin only) Node key */
        string boot_state;          /** Boot state */
        site site_id;           /** Site at which this node is located */
        unsigned pcu_ids[];         /** List of PCUs that control this node */
        string node_type;           /** Node type */
        string session;             /** (Admin only) Node session value */
        string ssh_rsa_key;         /** Last known SSH host key */
        unsigned last_pcu_reboot;   /** Date and time when PCU reboot was attempted */
        unsigned node_tag_ids[];    /** List of tags attached to this node */
        boolean verified;           /** Whether the node configuration is verified correct */
        unsigned last_contact;      /** Date and time when node last contacted plc */
        unsigned peer_node_id;      /** Foreign node identifier at peer */
        hostname hostname;            /** Fully qualified hostname */
        int last_time_spent_offline;/** Length of time the node was last offline after failure and before reboot */
        unsigned conf_file_ids[];   /** List of configuration files specific to this node */
        unsigned last_time_spent_online; /** Length of time the node was last online before shutdown/failure */
        unsigned slice_ids[];       /** List of slices on this node */
        string boot_nonce;          /** (Admin only) Random value generated by the node at last boot */
        string version;             /** Apparent Boot CD version */
        unsigned last_pcu_confirmation; /** Date and time when PCU reboot was confirmed */
        unsigned last_download;     /** Date and time when node boot image was created */
        unsigned date_created;      /** Date and time when node entry was created */
        string model;               /** Make and model of the actual machine */
        unsigned peer_id;           /** Peer to which this node belongs */
        unsigned ports[];           /** List of PCU ports that this node is connected to */

        CAPABILITY(retrieve,join,selection,projection);
        KEY(node_id);
    };
    """
    __myplc_method__ = 'GetNodes'

class MyPLCSiteCollection(MyPLCCollection):
    """
    class site {
        unsigned last_updated;
        unsigned node_ids[];
        unsigned site_id;

        string name;
        float latitude;
        float longitude;

        CAPABILITY(retrieve,join,selection,projection);
        KEY(site_id);
    };
    """
    __myplc_method__ = 'GetSites'

class MyPLCUserCollection(MyPLCCollection):
    """
    class myplcuser {
        string email;
        boolean enabled;
        CAPABILITY(retrieve,join,selection,projection);
        KEY(email);
    };
    """
    __myplc_method__ = 'GetPersons'

class MyPLCGateway(Gateway):

    __gateway_name__ = "myplc"

    def __init__(self, router, platform, platform_config):
        """
        Constructor
        Args:
            router: The Router on which this Gateway is running.
            platform: A String storing name of the platform related to this Gateway or None.
            platform_config: A dictionnary containing the configuration related to this Gateway.

                In pratice this dictionnary is built as follows:

                    {
                        "table_name" : {
                            "filename" : "/absolute/path/file/for/this/table.csv",
                            "fields"   : [
                                ["field_name1", "type1"],
                                ...
                            ],
                            "key" : "field_name_i, field_name_j, ..."
                        },
                        ...
                    }
        """
        super(MyPLCGateway, self).__init__(router, platform, platform_config)

        self.register_collection(MyPLCNodeCollection())
        self.register_collection(MyPLCSiteCollection())
        self.register_collection(MyPLCUserCollection())

        ReactorThread().start_reactor()

    def terminate(self):
        ReactorThread().stop_reactor()

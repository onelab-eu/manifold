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

API_URL = "https://www.planet-lab.eu:443/PLCAPI/"

# Account management in Shell, just like in MySlice, and transport of the l/p in
# annotations
try:
    from .auth import AUTH
except ImportError:
    AUTH = dict()
    Log.warning("PlanetLab authentification set to defaults: %s" % AUTH)

MAP_METHOD = {
    "node" : "GetNodes",
    "site" : "GetSites",
}

# XXX This could inherit from XMLRPC, like manifold_xmlrpc


class PLEGateway(Gateway):

    __gateway_name__ = "ple"

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, interface, platform_name, platform_config):
        """
        Constructor

        Args:
            interface: The Manifold Interface on which this Gateway is running.
            platform_name: A String storing name of the platform related to this Gateway or None.
            platform_config: A dictionnary containing the configuration related to this Gateway.
        """
        super(PLEGateway, self).__init__(interface, platform_name, platform_config)

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

        self._proxy = Proxy(API_URL.encode('latin-1'), allowNone = True)

        # XXX We should provide a default SSL Context...
        from twisted.internet import ssl
        self._proxy.setSSLClientContext(ssl.ClientContextFactory())

        ReactorThread().start_reactor()

    def terminate(self):
        ReactorThread().stop_reactor()

    #---------------------------------------------------------------------------
    # Internal methods
    #---------------------------------------------------------------------------

    @returns(StringTypes)
    def __str__(self):
        """
        Returns:
            The '%s' representation of this gateway.
        """
        return "<PLEGateway %s>" % (API_URL,) #(self._platform_config['url'])

    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    # XXX        nodes = srv.GetNodes(AUTH, {}, ['hostname', 'site_id'])
    def callback_records(self, rows, packet):
        """
        (Internal usage) See ManifoldGateway::receive_impl.
        Args:
            packet: A QUERY Packet.
            rows: The corresponding list of dict or Record instances.
        """
        self.records(rows, packet)

    def callback_error(self, error, packet):
        """
        (Internal usage) See ManifoldGateway::receive_impl.
        Args:
            packet: A QUERY Packet.
            error: The corresponding error message.
        """
        self.error(packet, "Error during Manifold call: %r" % error)

    def manifold_to_myplc_filter(self, filter):
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

    def receive_impl(self, packet):
        """
        Handle a incoming QUERY Packet.
        Args:
            packet: A QUERY Packet instance.
        """
        query = packet.get_query()
        table_name = query.get_table_name()

        # Need to transform manifold fields into query fields, and then back in
        # callback_records

        ## Insert a RENAME Node above this FROM Node if necessary.
        #instance = self.get_object(query.get_table_name())
        #aliases  = instance.get_aliases()
        #if aliases:
        #    Log.warning("I don't think this properly recables everything")
        #    try:
        #        Rename(self, aliases)
        #    except Exception, e:
        #        print "EEE:", e

        method = MAP_METHOD[query.get_object()]

        filters = self.manifold_to_myplc_filter(query.get_filter())
        fields = query.get_fields()
        if fields.is_star():
            fields = None
        else:
            fields = list(fields)

        d = self._proxy.callRemote(method, AUTH, filters, fields)
        d.addCallback(self.callback_records, packet)
        d.addErrback(self.callback_error, packet)

    #---------------------------------------------------------------------------
    # Metadata
    #---------------------------------------------------------------------------

    @returns(Announces)
    def make_announces(self):
        """
        Returns:
            The Announce related to this object.
        """
        platform_name = self.get_platform_name()

        @returns(list)
        @announces_from_docstring(platform_name)
        def make_announces_impl():
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
        announces = make_announces_impl()
        return announces

#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This class implements callback handled while querying a
# Slice object exposed by a RM or an AM SFA Gateway. 
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Loic Baron        <loic.baron@lip6.fr>
# Amine Larabi      <mohamed.larabi@inria.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC-INRIA

import re
from twisted.internet                           import defer

from sfa.storage.record                         import Record
from sfa.util.xrn                               import Xrn, get_authority

from manifold.gateways.sfa.rm.methods.rm_object import RM_Object
from manifold.util.log                          import Log
from manifold.util.type                         import accepts, returns 

@returns(bool)
def check_ssh_key(key):
    """
    Minimally check a key argument
    """
    good_ssh_key = r"^.*(?:ssh-dss|ssh-rsa)[ ]+[A-Za-z0-9+/=]+(?: .*)?$"
    return re.match(good_ssh_key, key, re.IGNORECASE)

@returns(Record)
def create_record_from_params(type, params):
    """
    Args:
        type: A String instance.
        params: A dictionnary storing the Query parameters
            (see Query::get_params())
    Returns:
        A Record.
    """
    record_dict = dict() 

    if type == "slice":
        # This should be handled beforehand
        if "slice_hrn" not in params or not params["slice_hrn"]:
            raise Exception, "Must specify slice_hrn to create a slice"
        xrn = Xrn(params["slice_hrn"], type)
        record_dict["urn"]  = xrn.get_urn()
        record_dict["hrn"]  = xrn.get_hrn()
        record_dict["type"] = xrn.get_type()

    if "key" in params and params["key"]:
        #try:
        #    pubkey = open(params["key"], "r").read()
        #except IOError:
        pubkey = params["key"]
        if not check_ssh_key(pubkey):
            raise ValueError("Wrong key format")
        record_dict["keys"] = [pubkey]

    if "slices" in params and params["slices"]:
        record_dict["slices"] = params["slices"]

    if "researchers" in params and params["researchers"]:
        # for slice: expecting a list of hrn
        record_dict["researcher"] = params["researchers"]

    if "email" in params and params["email"]:
        record_dict["email"] = params["email"]

    if "pis" in params and params["pis"]:
        record_dict["pi"] = params["pis"]

    #slice: description

    # handle extra settings
    #record_dict.update(options.extras)

    return Record(dict = record_dict)


class Slice(RM_Object):
    aliases = {
        "last_updated"       : "slice_last_updated",         # last_updated != last == checked,
        "geni_creator"       : "slice_geni_creator",
        "node_ids"           : "slice_node_ids",             # X This should be "nodes.id" but we do not want IDs
        "reg-researchers"    : "user.user_hrn",              # This should be "users.hrn"
        "reg-urn"            : "slice_urn",                  # slice_geni_urn ???
        "site_id"            : "slice_site_id",              # X ID 
        "site"               : "slice_site",                 # authority.hrn
        "authority"          : "authority_hrn",              # isn"t it the same ???
        "pointer"            : "slice_pointer",              # X
        "instantiation"      : "slice_instantiation",        # instanciation
        "max_nodes"          : "slice_max_nodes",            # max nodes
        "person_ids"         : "slice_person_ids",           # X users.ids
        "hrn"                : "slice_hrn",                  # hrn
        "record_id"          : "slice_record_id",            # X
        "gid"                : "slice_gid",                  # gid
        "nodes"              : "nodes",                      # nodes.hrn
        "peer_id"            : "slice_peer_id",              # X
        "type"               : "slice_type",                 # type ?
        "peer_authority"     : "slice_peer_authority",       # ??
        "description"        : "slice_description",          # description
        "expires"            : "slice_expires",              # expires
        "persons"            : "slice_persons",              # users.hrn
        "creator_person_id"  : "slice_creator_person_id",    # users.creator ?
        "PI"                 : "slice_pi",                   # users.pi ?
        "name"               : "slice_name",                 # hrn
        #"slice_id"          : "slice_id",
        "created"            : "created",                    # first ?
        "url"                : "slice_url",                  # url
        "peer_slice_id"      : "slice_peer_slice_id",        # ?
        "geni_urn"           : "slice_geni_urn",             # urn/hrn
        "slice_tag_ids"      : "slice_tag_ids",              # tags
        "date_created"       : "slice_date_created"          # first ?
    }

    @defer.inlineCallbacks
    @returns(list)
    def create(self, user, account_config, query):
        """
        This method must be overloaded if supported in the children class.
        Args:
            user: a dictionnary describing the User performing the Query.
            account_config: a dictionnary containing the User's Account
                (see "config" field of the Account table defined in the Manifold Storage)
            query: The Query issued by the User.
        Returns:
            The list of updated Objects.
        """
        gateway = self.get_gateway()
        filters = query.get_where()
        params  = query.get_params()
        fields  = query.get_select()

        # Get the slice name
        if not "slice_hrn" in params:
            raise Exception("Create slice requires a slice name")
        slice_hrn = params["slice_hrn"]

        # Are we creating the slice on the right authority
        slice_auth = get_authority(slice_hrn)
        sfa_proxy = yield gateway.get_sfa_proxy_admin()
        server_version = sfa_proxy.get_cached_version()
        server_auth = server_version["hrn"]

        if not slice_auth.startswith("%s." % server_auth):
            Log.info("Not requesting slice creation on %s for %s" % (server_auth, slice_hrn))
            defer.returnValue(list())

        Log.info("Requesting slice creation on %s for %s" % (server_auth, slice_hrn))
        Log.warning("Need to check slice is created under user authority")
        credential = gateway.get_credential(user, account_config, "authority")
        record_dict = create_record_from_params("slice", params)

        try:
            slice_gid = sfa_proxy.Register(record_dict, credential)
        except Exception, e:
            Log.error("%s" % e)

        defer.returnValue(list())


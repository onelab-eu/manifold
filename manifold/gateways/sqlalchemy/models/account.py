#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Account class used in the Manifold Storage.
#
# For the moment, this is an python object used by
# SQLAlchemy, which is used to interact with the
# Manifold Storage. 
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
# Loic Baron        <loic.baron@lip6.fr>
#
# Copyright (C) 2013 UPMC

import json
from sqlalchemy                     import Column, ForeignKey, Integer, String, Enum
from sqlalchemy.orm                 import relationship, backref

# TODO move the SFA specific part in manifold/gateways/sfa
try:
    from sfa.trust.credential       import Credential
except:
    pass

from ..models                       import Base
from ..models.user                  import ModelUser
from ..models.platform              import ModelPlatform
#from ..models.get_session           import get_session
from manifold.util.log              import Log 
from manifold.util.predicate        import Predicate
from manifold.util.type             import accepts, returns 

class ModelAccount(Base):

    restrict_to_self = True

    platform_id = Column(Integer, ForeignKey("platform.platform_id"), primary_key = True, doc = "Platform identifier")
    user_id     = Column(Integer, ForeignKey("user.user_id"),         primary_key = True, doc = "User identifier")
    auth_type   = Column(Enum("none", "default", "user", "reference", "managed"), default = "default")
    config      = Column(String, doc = "Default configuration (serialized in JSON)")

    user        = relationship("ModelUser",     backref = "accounts",  uselist = False)
    platform    = relationship("ModelPlatform", backref = "platforms", uselist = False)

#UNUSED|    def update_config(self, gateway = None):
#UNUSED|        """
#UNUSED|        Update the "config" field of this Account in the Manifold Storage.
#UNUSED|        Args:
#UNUSED|            gateway: The Gateway related to this Account (see manifold.gateways)
#UNUSED|        """
#UNUSED|        if not gateway: gateway = Account.get_gateway()
#UNUSED|        assert gateway,        "Invalid gateway"
#UNUSED|        assert gateway.manage, "No manage method defined in %s" % type(gateway)
#UNUSED|
#UNUSED|        # Retrieve the (new) JSON String
#UNUSED|        config = json.dumps(gateway.manage(self.user, self.platform, json.loads(self.config)))
#UNUSED|
#UNUSED|        # Update the Manifold Storage if required
#UNUSED|        if self.config != config:
#UNUSED|            self.config = config
#UNUSED|            db.commit()

    @staticmethod
    #@returns(Filter)
    def process_filters(filters):
        """
        Process "WHERE" clause carried by a Query related to the
        local:account object.
        Args:
            filters: A Filter instance made of "==" Predicate instances.
        Returns:
            The updated Filter instance. 
        """
        # Update predicates involving "user"
        user_filters = filters.get("user")
        filters.delete("user")
        if user_filters:
            for user_filter in user_filters:
                assert user_filter.op.__name__ == "eq", "Only == is supported for convenience filter 'user'" 
                user_email = user_filter.value
                filters.add(Predicate("user_id", "=", ModelUser.get_user_id(user_email)))
            
        # Update predicates involving "platform"
        platform_filters = filters.get("platform")
        filters.delete("platform")
        if platform_filters:
            for platform_filter in platform_filters:
                assert platform_filter.op.__name__ == "eq", "Only == is supported for convenience filter 'platform'"
                platform_name = platform_filter.value
                filters.add(Predicate("platform_id", "=", Platform.get_platform_id(platform_name)))

        return filters
        
    @staticmethod
    @returns(dict)
    def process_params(params, filters, user, interface, session):
        """
        Process "params" clause carried by a Query to abstract Manifold from
        considerations related to the Manifold Storage (for instance json
        encoding and so on) regarding the local:account object.
        Args:
            params: A dictionnary instance.
            filters: A list of Predicate instances.
            user: a ModelUser instance
        """
        user_params = params.get("user")
        if user_params:
            del params["user"]
            user_email = user_params
            params["user_id"] = ModelUser.get_user_id(user_email, interface)

        platform_params = params.get("platform")
        if platform_params:
            del params["platform"]
            platform_name = platform_params
            params["platform_id"] = Platform.get_platform_id(platform_name) 

        # JSON ENCODED FIELDS are constructed into the json_fields variable
        given = set(params.keys())
        accepted = set([c.name for c in ModelAccount.__table__.columns])
        given_json_fields = given - accepted
        #Log.tmp("given_json_fields = %s given = %s accepted = %s" % (given_json_fields, given, accepted))
        
        if given_json_fields:
            if 'config' in given_json_fields:
                raise Exception, "Cannot mix full JSON specification & JSON encoded fields"

            r = session.query(ModelAccount.config)
            for filter in filters:
                r = r.filter(filter)
            if user:
                r = r.filter(ModelAccount.user_id == user['user_id'])
            #r = r.filter(filters) #Account.platform_id == platform_id)
            r = r.one()
            try:
                json_fields = json.loads(r.config)
            except Exception, e:
                json_fields = dict() 

            # We First look at convenience fields
            for field in given_json_fields:
                if field == "credential":
                    # We'll determine the type of credential
                    # XXX NOTE This is SFA specific... it should be hooked by SFA gateway
                    # @loic modified according to the SFA Gateway, to handle delegation
                    # XXX TODO need to be improved...
                    c = Credential(string=params[field])
                    c_type = c.get_gid_object().get_type()
                    if c_type == "user":
                        new_field = "delegated_%s_credential" % c_type
                        json_fields[new_field] = params[field]
                    else: 
                        cred_name="delegated_%s_credentials" % c_type
                        if not cred_name in json_fields:
                            json_fields[cred_name] = {}
                        c_target = c.get_gid_object().get_hrn()
                        json_fields[cred_name][c_target] = params[field]
                else:
                    json_fields[field] = params[field]
                del params[field]

            params["config"] = json.dumps(json_fields)

        return params

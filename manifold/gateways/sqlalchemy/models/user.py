#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# User class used in the Manifold Storage.
#
# For the moment, this is an python object used by
# SQLAlchemy, which is used to interact with the
# Manifold Storage. 
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC

import json
from sqlalchemy                 import Column, Integer, String

from ..models                   import Base
from ..models.get_session       import get_session
from manifold.util.type         import accepts, returns 

class ModelUser(Base):
    restrict_to_self = True
    user_id  = Column(Integer, primary_key = True, doc = "User identifier")
    email    = Column(String,                      doc = "User email")
    password = Column(String,                      doc = "User password")
    config   = Column(String,                      doc = "User config (serialized in JSON)")

#UNUSED|    def config_set(self, value):
#UNUSED|        #Log.deprecated()
#UNUSED|        return self.set_config(value)
        
#    def set_config(self, value, session):
#        """
#        Update the "config" field of this ModelUser in the
#        Manifold Storage.
#        Args:
#            value: A String encoded in JSON containing
#                the new "config" related to this ModelUser.
#        """
#        session = get_session(self)
#        self.config = json.dumps(value)
#        session.add(self)
#        session.commit()
#        
    @returns(dict)
    def get_config(self):
        """
        Returns:
            The dictionnary corresponding to the JSON content
            related to the "config" field of this ModelUser.
        """
        if not self.config:
            return dict() 
        return json.loads(self.config)

    @staticmethod
    @returns(int)
    def get_user_id(user_email, interface):
        """
#        (CRAPPY)
        This crappy method is used since we do not exploit Manifold
        to perform queries on the Manifold storage, so we manually
        retrieve user_id.
        Args:
            user_email: a String instance. 
        Returns:
            The user ID related to an ModelUser.
        """
        try:
            user, = interface.execute_local_query(Query\
                .get("user").filter_by("email", "=", user_email))
        except Exception, e:
            raise ValueError("No Account found for User %s, Platform %s ignored: %s" % (user_email, platform_name, traceback.format_exc()))
        return user['user_id']
#        ret = db.query(ModelUser.user_id).filter(ModelUser.email == user_params).one()
#        return ret[0]

    @staticmethod
    @returns(dict)
    def process_params(params, filters, user, interface, session):

        # JSON ENCODED FIELDS are constructed into the json_fields variable
        given = set(params.keys())
        accepted = set([c.name for c in ModelUser.__table__.columns])
        given_json_fields = given - accepted
        
        if given_json_fields:
            if "config" in given_json_fields:
                raise Exception, "Cannot mix full JSON specification & JSON encoded fields"

            r = session.query(ModelUser.config).filter(filters)
            if user:
                r = r.filter(ModelUser.user_id == user["user_id"])
            r = r.filter(filters) #ModelUser.platform_id == platform_id)
            r = r.one()
            try:
                json_fields = json.loads(r.config)
            except Exception, e:
                json_fields = dict() 

            # We First look at convenience fields
            for field in given_json_fields:
                json_fields[field] = params[field]
                del params[field]

            params["config"] = json.dumps(json_fields)

        return params

#    @classmethod
#    def params_ensure_user(cls, params, user, session):
#
#        # A user can only create its own objects
#        if cls.restrict_to_self:
#            params["user_id"] = user["user_id"]
#            return
#
#        if "user_id" in params: return
#        if "user" in params:
#            user_params = params["user"]
#            print "user_params", user_params
#            del params["user"]
#            ret = session.query(ModelUser.user_id)
#            ret = ret.filter(ModelUser.email == user_params)
#            ret = ret.one()
#            params["user_id"] = ret[0]
#            return
#        raise ValueError("User should be specified")
 

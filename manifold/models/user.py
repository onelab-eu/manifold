#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# User class used in the Manifold Storage.
#
# For the moment, this is an python object used by
# SQLAlchemy, which is used to interact with the
# sqlite database /var/myslice/db.sqlite.
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) 2013 UPMC

import json
from sqlalchemy                 import Column, Integer, String
from sqlalchemy                 import inspect
from manifold.models            import Base #, db
from manifold.util.type         import accepts, returns 


class User(Base):
    restrict_to_self = True
    user_id  = Column(Integer, primary_key = True, doc = "User identifier")
    email    = Column(String,                      doc = "User email")
    password = Column(String,                      doc = "User password")
    config   = Column(String,                      doc = "User config (serialized in JSON)")

#UNUSED|    def config_set(self, value):
#UNUSED|        #Log.deprecated()
#UNUSED|        return self.set_config(value)
        
    def set_config(self, value):
        """
        Update the "config" field of this User in the
        Manifold Storage.
        Args:
            value: A String encoded in JSON containing
                the new "config" related to this User.
        """
        db = inspect(self).session
        self.config = json.dumps(value)
        db.add(self)
        db.commit()
        
    @returns(dict)
    def get_config(self):
        """
        Returns:
            The dictionnary corresponding to the JSON content
            related to the "config" field of this User.
        """
        if not self.config:
            return dict() 
        return json.loads(self.config)

    @staticmethod
    @returns(int)
    def get_user_id(user_email):
        """
        (CRAPPY)
        This crappy method is used since we do not exploit Manifold
        to perform queries on the Manifold storage, so we manually
        retrieve user_id.
        Args:
            user_email: a String instance. 
        Returns:
            The user ID related to an User.
        """
        db = inspect(self).session
        ret = db.query(User.user_id).filter(User.email == user_params).one()
        return ret[0]

    @staticmethod
    def process_params(params, filters, user):
        db = inspect(self).session

        # JSON ENCODED FIELDS are constructed into the json_fields variable
        given = set(params.keys())
        accepted = set([c.name for c in User.__table__.columns])
        given_json_fields = given - accepted
        
        if given_json_fields:
            if 'config' in given_json_fields:
                raise Exception, "Cannot mix full JSON specification & JSON encoded fields"

            r = db.query(User.config).filter(filters)
            if user:
                r = r.filter(User.user_id == user['user_id'])
            r = r.filter(filters) #User.platform_id == platform_id)
            r = r.one()
            try:
                json_fields = json.loads(r.config)
            except Exception, e:
                json_fields = dict() 

            # We First look at convenience fields
            for field in given_json_fields:
                json_fields[field] = params[field]
                del params[field]

            params['config'] = json.dumps(json_fields)

        return params

    @classmethod
    def params_ensure_user(cls, params, user):
        db = inspect(self).session

        # A user can only create its own objects
        if cls.restrict_to_self:
            params['user_id'] = user['user_id']
            return

        if 'user_id' in params: return
        if 'user' in params:
            user_params = params['user']
            del params['user']
            ret = db.query(User.user_id)
            ret = ret.filter(User.email == user_params)
            ret = ret.one()
            params['user_id'] = ret[0]
            return
        raise Exception, 'User should be specified'
 

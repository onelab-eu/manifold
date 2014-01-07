#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Base class (internal usage).
# It must only used in __init__.py.
# Otherwise please use:
#
#   from manifold.models import Base
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
# Loic Baron        <loic.baron@lip6.fr>
#
# Copyright (C) 2013 UPMC

from sqlalchemy.ext.declarative import declared_attr

class Base(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    # By default, we do not filter the content of the table according to the
    # authenticated user
    restrict_to_self = False

    __mapper_args__= {'always_refresh': True}

    #id =  Column(Integer, primary_key=True)

    #def to_dict(self):
    #    return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @classmethod
    def process_filters(cls, filters):
        return filters

    @classmethod
    def process_params(cls, params, filters, user):
        return params

    # This should be implemented in manifold.models.user
    @classmethod
    def params_ensure_user(cls, params, user):
        # A user can only create its own objects
        if cls.restrict_to_self:
            params['user_id'] = user.user_id
            return

        if 'user_id' in params: return
        if 'user' in params:
            user_params = params['user']
            del params['user']
            ret = db.query(User.user_id)
            ret = ret.filter(User.email == user_params)
            ret = ret.one()
            params['user_id']=ret[0]
            return
        raise Exception, 'User should be specified'


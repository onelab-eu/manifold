#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Parent class of every Model* class defined in this
# directory.
#
# Jordan Auge       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
# Loic Baron        <loic.baron@lip6.fr>
#
# Copyright (C) 2013 UPMC

from types                          import StringTypes
from sqlalchemy.ext.declarative     import declared_attr

from manifold.core.filter           import Filter
from manifold.util.type             import returns 

class Base(object):
    # By default, we do not filter the content of the table according to the
    # authenticated user
    restrict_to_self = False

    __mapper_args__= {"always_refresh" : True}

    #id =  Column(Integer, primary_key=True)

    #def to_dict(self):
    #    return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @declared_attr
    @returns(StringTypes)
    def __tablename__(cls):
        """
        Returns:
            A String containing the name of the sqlalchemy
            wrapped by this Base class.
        """
        return cls.__name__[len("Model"):].lower()

    @classmethod
    @returns(Filter)
    def process_filters(cls, filters):
        return filters

    @classmethod
    @returns(dict)
    def process_params(cls, params, filters, user):
        return params

    # This should be implemented in manifold.gateways.sqlalchemy.models.user
    @classmethod
    def params_ensure_user(cls, params, user):
        """
        Check whether the "user" value is properly set in a
        dictionnary and initialize it according to the database
        content if required.
        Args:
            params: A dict instance.
            user: A ModelUser instance.
        Raises:
            ValueError: If 'user' is not set properly.
        """
        # A user can only create its own objects
        if cls.restrict_to_self:
            params["user_id"] = user.user_id
            return

        if "user_id" in params:
            return

        if "user" in params:
            user_params = params["user"]
            del params["user"]
            ret = db.query(ModelUser.user_id)
            ret = ret.filter(ModelUser.email == user_params)
            ret = ret.one()
            params["user_id"] = ret[0]
            return

        raise ValueError("User should be specified")


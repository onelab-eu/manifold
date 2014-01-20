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
from sqlalchemy.sql                     import ClauseElement

from manifold.core.filter           import Filter
from manifold.util.predicate            import included
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

    #---------------------------------------------------------------------
    # Internal usage 
    #---------------------------------------------------------------------

    @classmethod
    @returns(ClauseElement)
    def make_sqla_single_filter_impl(cls, key, op, value):
        """
        (Internal usage)
        Args:
            key: A String containing the field name involved in the Predicate
            op: A function taking (key, value) parameter and returning
                a value.
            value: The value involved in the Predicate
        """
        key_attr = getattr(cls, key)
        return key_attr.in_(value) if op == included else op(key_attr, value)

    @classmethod
    @returns(ClauseElement)
    def make_sqla_single_filter(cls, predicate):
        """
        (Internal usage) Convert a Predicate involving a single Field. 
            Example:
                x INCLUDED [1, 2, 4]
        Note:
            You should use make_sqla_filters instead.
        Args:
            cls: Any Model* class.
            predicate: A Predicate instance.
        Returns:
            The corresponding ClauseElement instance. 
        """
        key, op, value = predicate.get_tuple()
        return cls.make_sqla_single_filter_impl(key, op, value) 
        
    @classmethod
    @returns(ClauseElement)
    def make_sqla_composite_filter(cls, predicate):
        """
        (Internal usage) Convert a Predicate involving several Fields
        (e.g "(foo, bar) == (1, 2)") into the corresponding ClauseElement.

        Each induced single-filter is connected using OR operator.
            Example: 
                (x, y) INCLUDED [(1, 2), (2, 3), (4, 6)]
            <=> (x INCLUDED [1, 2, 4]) AND (y INCLUDED [2, 3, 6])

        Note:
            You should use make_sqla_filters instead.
        Args:
            cls: Any Model* class.
            predicate: A Predicate instance.
        Returns:
            The corresponding ClauseElement instance. 
        """
        key, op, values = predicate.get_tuple()
        ret = None 
        for i, key_i in enumerate(key):
            values_i = [value[i] for value in values]
            sqla_filter_i = cls.make_sqla_single_filter_impl(key_i, op, values_i)
            ret = sqla_filter_i if not ret else (ret and sqla_filter_i)
        return ret

    @classmethod
    @returns(ClauseElement)
    def make_sqla_filter(cls, predicate):
        """
        (Internal usage) Convert a Predicate into a sqlalchemy filter.
        involving several Fields, (e.g "(foo, bar) == (1, 2)").

        Note:
            You should use make_sqla_filters instead.
        Args:
            cls: Any Model* class.
            predicate: A Predicate instance.
        Returns:
            The corresponding ClauseElement instance. 
        """
        if predicate.is_composite():
            sqla_filter = cls.make_sqla_composite_filter(predicate)
        else:
            sqla_filter = cls.make_sqla_single_filter(predicate)
        return sqla_filter

    @classmethod
    @returns(list)
    def make_sqla_filters(cls, predicates):
        """
        Convert a Filter into a list of sqlalchemy filters.
        Args:
            cls: Any Model* class
            predicates: A Filter instance or None.
        Returns:
            The corresponding list of ClauseElement instances.
        """
        assert isinstance(predicates, Filter),\
            "Invalid predicates = %s (%s)" % (predicates, Filter)

        sqla_filters = None
        if predicates:
            sqla_filters = list() 
            for predicate in predicates:
                sqla_filter = cls.make_sqla_filter(predicate)
                sqla_filters.append(sqla_filter)
        return sqla_filters 



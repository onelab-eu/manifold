#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ConfGateway perform queries on the filesystem where
# file hierarchy describes the database schema according
# to the following convention:
#
# base_dir/table_name/field_name/value
#
# Jordan Augé       <jordan.auge@lip6.fr>
# Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#
# Copyright (C) UPMC

import os

from manifold.core.announce                 import Announces, announces_from_docstring
from manifold.core.query_factory            import QueryFactory
from manifold.gateways                      import Gateway
from manifold.gateways.conf.pattern_parser  import PatternParser
from manifold.util.type                     import accepts, returns

BASEDIR = "/tmp/test"

class ConfGateway(Gateway):
    """
    Configuration elements stored in the filesystem.
    """
    __gateway_name__ = "conf"

    MAP_PATTERN = {
        "account"  : "%(account)t/%(email)f/%(platform)f/%(accounts)F",
        "platform" : "%(platform)t/%(platform)f/%(platform.conf)F"
    }

    # user:
    # account : %(account)t/%(user)f/%(platform)f/%f
    # key must be encoded in the filesystem

    def receive_impl(self, packet):
        """
        Executes a Query on the Manifold Storage.
        Args:
            packet: A QUERY Packet.
        """
        query = QueryFactory.from_packet()
        # XXX ensure query parameters are non empty for create
        parser = PatternParser(query, BASEDIR)
        table_name = query.get_table_name()
        rows = parser.parse(self.MAP_PATTERN[table_name])
        self.records(rows, packet)

    
    @returns(Announces)
    def make_announces(self):
        """
        Returns:
            The Announces related to this Gateway.
        """
        platform_name = self.get_platform_name()

        @returns(Announces)
        @announces_from_docstring(platform_name)
        def make_announces_impl(self):
            """
            class account {
                user     user;
                platform platform;

                CAPABILITY(fullquery);
                KEY(user,platform);
            };
            """
            # XXX infinite number of fields can be stored
            # XXX local table / all local fields, no normalization

        announces = make_announces_impl()
        return announces

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

from manifold.core.announce                 import announces_from_docstring
from manifold.gateways                      import Gateway
from manifold.gateways.conf.pattern_parser  import PatternParser
from manifold.core.local                    import LOCAL_NAMESPACE

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
        query = packet.get_query()
        # XXX ensure query parameters are non empty for create
        parser = PatternParser(query, BASEDIR)
        rows = parser.parse(self.MAP_PATTERN[query.get_from()])
        self.records(rows, packet)

    @announces_from_docstring(LOCAL_NAMESPACE)
    def make_announces(self):
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

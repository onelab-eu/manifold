#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# The Manifold Storage stores the Manifold configuration, including
# the Manifold users, accounts, and platforms.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>
#   Jordan Augé       <jordan.auge@lip6.f>

from manifold.gateways              import Gateway
from manifold.core.announce         import announces_from_docstring
from manifold.core.annotation       import Annotation
from manifold.core.packet           import QueryPacket
from manifold.core.sync_receiver    import SyncReceiver
from manifold.util.filesystem       import ensure_writable_directory
from manifold.util.type             import accepts, returns

STORAGE_GATEWAY   = "sqlalchemy"
STORAGE_DIRECTORY = "/var/myslice"
STORAGE_URL       = "sqlite:///%s/db.sqlite?check_same_thread=False" % STORAGE_DIRECTORY
STORAGE_CONFIG    = {"url" : STORAGE_URL}
STORAGE_NAMESPACE = "local"

@returns(Gateway)
def make_storage(interface):
    """
    Create a Gateway instance allowing to query the Manifold Storage
    Args:
        interface: The Interface instance on which the Storage Gateway is running.
    Returns:
        A Gateway instance allowing to query the Manifold Storage
    """

    ensure_writable_directory(STORAGE_DIRECTORY)

    cls_storage = Gateway.get(STORAGE_GATEWAY)
    if not cls_storage:
        raise Exception, "Cannot find %s Gateway, required to access Manifold Storage " % STORAGE_GATEWAY
    return cls_storage(interface, STORAGE_NAMESPACE, STORAGE_CONFIG)

@returns(list)
def storage_execute(gateway, query, annotation = None, error_message = None):
    """
    Execute a Query related to the Manifold Storage
    (ie any "local:*" object).
    Args:
        gateway: A Gateway instance allowing to query the Manifold Storage.
        query: A Query. query.get_from() should start with "local:".
        annotation: An Annotation instance related to Query or None.
        error_message: A String containing the error_message that must
            be written in case of failure.
    Raises:
        Exception: if the Query does not succeed.
    Returns:
        A list of Records.            
    """
    assert isinstance(gateway, Gateway),\
        "Invalid gateway = %s (%s)" % (gateway, type(gateway))
    assert not annotation or isinstance(annotation, Annotation),\
        "Invalid annotation = %s (%s)" % (annotation, type(annotation))
    assert not ':' in query.get_from() or query.get_from().startswith(STORAGE_NAMESPACE),\
        "Invalid namespace: '%s' != '%s'" % (query.get_from(), STORAGE_NAMESPACE)

    receiver = SyncReceiver()
    packet   = QueryPacket(query, annotation, receiver)

    gateway.add_flow(query, receiver)
    gateway.receive(packet)
    result_value = receiver.get_result_value()

    if not result_value.is_success():
        if not error_message:
            error_message = "Error executing local query: %s" % query
        raise Exception, error_message

    return [record.to_dict() for record in result_value['value']]
        
@returns(list)
def storage_make_virtual_announces(platform_name = STORAGE_NAMESPACE):
    """
    Craft a list of Announces used to feed the Storage's namespace.
    Args:
        platform_name: A name of the Storage platform.
    Returns:
        The corresponding list of Announces.
    """
    @announces_from_docstring(platform_name)
    def _get_metadata_tables():
        """
        class object {
            string  table;           /**< The name of the object/table.     */
            column  columns[];       /**< The corresponding fields/columns. */
            string  capabilities[];  /**< The supported capabilities        */

            CAPABILITY(retrieve);
            KEY(table);
        }; 

        class column {
            string qualifier;
            string name;
            string type;
            string description;
            bool   is_array;

            KEY(name);
        };

        class gateway {
            string type;

            CAPABILITY(retrieve);
            KEY(type);
        };
        """
    announces = _get_metadata_tables(platform_name)
    return announces

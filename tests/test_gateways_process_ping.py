#!/usr/bin/env python
# -*- coding: utf-8 -*-

from manifold.core.annotation               import Annotation
from manifold.core.packet                   import QueryPacket
from manifold.core.query                    import Query
from manifold.core.sync_receiver            import SyncReceiver

from manifold.gateways.process.ping         import PingGateway


def main():

    ping_gw = PingGateway()

    print ping_gw.make_announce()

    query       = Query.get('ping').filter_by('destination', '==', '8.8.8.8')
    annotation  = Annotation()
    receiver    = SyncReceiver()

    annotation['count'] = 3

    packet      = QueryPacket(query, annotation, receiver)
    
    ping_gw.receive(packet)
    print receiver.get_result_value()

if __name__ == '__main__':
    main()

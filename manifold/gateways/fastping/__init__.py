# -*- coding: utf-8 -*-

# We need a base fastping class to import as a dependency, that does not
# initialize anything
import sys, urllib, threading

from fastping                   import Fastping

from manifold.gateways          import Gateway
from manifold.gateways.object   import ManifoldCollection
from manifold.util.filesystem   import hostname
from manifold.util.misc         import url_exists

TARGET_SITE = 'http://www.top-hat.info'
TARGET_PATH = '/download/anycast-census/'
TARGET_FILES = ['iplist-%(source)s.dat', 'iplist.dat']

BLACKLIST_URL = 'http://www.top-hat.info/download/anycast-census/blacklist.fastping'

class FastPingCollection(ManifoldCollection):
    """
    class fastping {
        string target_id;
        hostname source;
        ip destination;
        string sequence;
        string cycle;
        string rtt;
        string time_sent;
        string time_received;
        CAPABILITY(join);
        KEY(source, destination);
        PARTITIONBY(source == $HOSTNAME);
    };
    """

    def on_new_measurement(self, measurement, packet, source):
        measurement['source'] = source
        self.get_gateway().record(measurement, packet)

    def on_done(self, packet):
        self.get_gateway().last(packet)

    def get_target_url(self):
        for filename in TARGET_FILES:
            if url_exists(TARGET_SITE, TARGET_PATH + filename):
                return "%s%s%s" % (TARGET_SITE, TARGET_PATH, filename)
        raise Exception, "No IP list found."

    def get(self, packet):
        source = hostname()
        target_url = self.get_target_url() % locals()

        # Get the necessary parameters from the packet destination
        #filter = packet.get_destination().get_filter()
        #target = filter.get_eq('destination')

        # Initialize a fastping instance
        opt = {
            linkFile    : target_url,
            deltaM      : 0,
            numberCycle : 1,
            saveRW      : True,
            saveQD      : True,
            saveSM      : True,
            saveST      : True,
            blacklist   : BLACKLIST_URL,
            upload      : ['clitos.ipv6.lip6.fr', 'guest@top-hat.info', 'guest', 21, 'anycast', 'False'],
        }
        fastping = Fastping(**opt)

        fastping.set_raw_callback(self.on_new_measurement, packet, source)
        fastping.set_done_callback(self.on_done, packet)

        # Return results as they come by, we need to overload some methods from
        # fastping eventually, until callbacks are available
        # XXX blocking
        thread = threading.Thread(target=fastping.run, args=())
        thread.daemon = True # will make fastping fail otherwise...
        thread.start()
        # Needs:
        # - min of 10 pings
        # - no cycle limit
        # - interrupt

class FastPingGateway(Gateway):
    __gateway_name__ = 'fastping'

    def __init__(self, router = None, platform_name = None, platform_config = None):
        Gateway.__init__(self, router, platform_name, platform_config)

        self.register_collection(FastPingCollection())

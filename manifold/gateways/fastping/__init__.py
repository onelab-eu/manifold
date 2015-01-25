# -*- coding: utf-8 -*-

# We need a base fastping class to import as a dependency, that does not
# initialize anything
import os, sys, urllib, threading

from fastping                   import Fastping

from manifold.core.packet       import Record, Records
from manifold.gateways          import Gateway
from manifold.gateways.object   import ManifoldCollection
from manifold.util.log          import Log
from manifold.util.misc         import url_exists
from manifold.util.filesystem   import hostname, ensure_writable_directory

TARGET_SITE = 'www.top-hat.info'
TARGET_PATH = '/download/anycast-census/'
TARGET_FILES = ['iplist-%(source)s.dat', 'iplist.dat']

DATASET_URLBASE = 'http://www.top-hat.info/download/manifold/'
CACHE_DIRECTORY = '/var/cache/manifold/'

BLACKLIST_URL = 'http://www.top-hat.info/download/anycast-census/blacklist.fastping'

def ensure_dataset(dataset):
    if os.path.exists(CACHE_DIRECTORY + dataset):
        return
    
    # Let's download dataset
    ensure_writable_directory(CACHE_DIRECTORY)

    f = urllib.urlopen(DATASET_URLBASE + dataset)
    g = open(CACHE_DIRECTORY + dataset, 'w')
    g.write(f.read())
    f.close()
    g.close()

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

    def get(self, packet):

        # Get the necessary parameters from the packet destination
        #filter = packet.get_destination().get_filter()
        #target = filter.get_eq('destination')

        FTP_FIELDS = ['ftp_hostname', 'ftp_port', 'ftp_username', 'ftp_password', 'ftp_directory', 'ftp_passive']
        FTP_FIELDS_MANDATORY = ['ftp_hostname', 'ftp_username', 'ftp_password']
        FTP_DEFAULT_PORT = 21
        FTP_DEFAULT_PASSIVE = False
        FTP_DEFAULT_DIRECTORY = '.'

        # Initialize a fastping instance
        opt = {
            'deltaM'     : 0,
            'numberCycle': 1,
            'saveRW'     : True,
            'saveQD'     : True,
            'saveSM'     : True,
            'saveST'     : True,
            'shuffle'    : True,
        }

        annotation = packet.get_annotation()

        # IP list: we are given a url
        if not 'destination' in annotation:
            self.get_gateway().error('Missing target', packet)
            return

        target = annotation.get('destination')
        if target.startswith('dataset://'):
            target = target[10:]
            try:
                ensure_dataset(target)
            except Exception, e:
                self.get_gateway().error('Cannot retrieve dataset: %s' % e, packet)
                return
            target = CACHE_DIRECTORY + target

        target.replace('$HOSTNAME', hostname())
        opt['target'] = target

        # Blacklist
        blacklist = annotation.get('blacklist', None)
        if blacklist:
            if blacklist.startswith('dataset://'):
                blacklist = blacklist[10:]
                try:
                    ensure_dataset(blacklist)
                except Exception, e:
                    self.get_gateway().error('Cannot retrieve dataset: %s' % e, packet)
                    return
                blacklist = CACHE_DIRECTORY + blacklist
            opt['blacklist'] = blacklist

        # FTP
        do_ftp = set(FTP_FIELDS) & set(annotation.keys())
        if do_ftp:
            try:
                ftp_hostname  = annotation.get('ftp_hostname')
                ftp_username  = annotation.get('ftp_username')
                ftp_password  = annotation.get('ftp_password')
                ftp_port      = annotation.get('ftp_port', FTP_DEFAULT_PORT)
                ftp_directory = annotation.get('ftp_directory', FTP_DEFAULT_DIRECTORY)
                ftp_passive   = annotation.get('ftp_passive', FTP_DEFAULT_PASSIVE)
            except Exception, e:
                self.get_gateway().error('Missing mandatory field in annotation for FTP: %s' % e, packet)
                return
            opt['upload'] = [ftp_hostname, ftp_username, ftp_password, ftp_port, ftp_directory, ftp_passive]
            
        Log.info("Initializing fastping with options: %r" % opt)
        fastping = Fastping(**opt)

        if not do_ftp:
            source = hostname()
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

        if do_ftp:
            self.get_gateway().records(Record(last=True), packet)

class FastPingGateway(Gateway):
    __gateway_name__ = 'fastping'

    def __init__(self, router = None, platform_name = None, platform_config = None):
        Gateway.__init__(self, router, platform_name, platform_config)

        self.register_collection(FastPingCollection())

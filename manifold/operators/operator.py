# -*- coding: utf-8 -*-

from manifold.core.packet import Packet
from manifold.core.relay  import Relay

# NOTES: it seem we don't need the query anymore in the operators expect From
# maybe ? Selection, projection ??

class Operator(Relay):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, producers = None, consumers = None, max_producers = None, max_consumers = None, has_parent_producer = False):
        """
        Constructor.
        """
        Relay.__init__(self, \
            producers = producers, consumers = consumers, \
            max_consumers = max_consumers, max_producers = max_producers, \
            has_parent_producer = has_parent_producer)


    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def receive(self, packet):
        raise Exception, "Operator::receive() should be implemented in children classes"
        
    def tab(self, indent):
        """
        \brief print _indent_ tabs
        """
        print "         P: ", self._pool_producers
        print "         C: ", self._pool_consumers
        print "[%04d ]" % self._identifier, ' ' * 4 * indent,
        #        sys.stdout.write(' ' * indent * 4)

    def dump(self, indent = 0):
        """
        \brief Dump the current node
        \param indent current indentation
        """
        self.tab(indent)
        print "%r" % self
        #print "%r (%r)" % (self, self.query)
        #print "%r (%r)" % (self, self.callback)

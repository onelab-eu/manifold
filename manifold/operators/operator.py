from manifold.core.producer import Producer
from manifold.core.consumer import Consumer
from manifold.core.packet import Packet

# NOTES: it seem we don't need the query anymore in the operators expect From
# maybe ? Selection, projection ??

class Operator(Producer, Consumer):

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self, max_producers = None, max_consumers = None, has_parent_producer = False):
        """
        Constructor.
        """

        Producer.__init__(self, max_consumers = max_consumers)
        Consumer.__init__(self, max_producers = max_producers, has_parent_producer = has_parent_producer)


    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def send(self, packet):
        if packet.get_type() in [Packet.TYPE_QUERY]:
            Producer.send(self, packet)
        elif packet.get_type() in [Packet.TYPE_RECORD, Packet.TYPE_ERROR]:
            Consumer.send(self, packet)

    def receive(self, packet):
        raise Exception, "Operator::receive() should be implemented in children classes"
        
    def tab(self, indent):
        """
        \brief print _indent_ tabs
        """
        print "[%04d]" % self.identifier, ' ' * 4 * indent,
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

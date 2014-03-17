#!/usr/bin/env python


class Client(asynchat.async_chat):

    def __init__(self):
        asynchat.async_chat.__init__(self)

        self.data = ""

        self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._request_fifo = [] # UNUSED ?
        self._receive_buffer = []

        # Prepare packet reception (DUP)
        self._pstate = self.STATE_LENGTH
        self.set_terminator (8)

        self.connect(self.path)

    def handle_connect (self):
        # Push a query
        query = Query.get('ping').filter_by('destination', '==', '8.8.8.8')
        annotation = Annotation()
        
        packet = QueryPacket(query, annotation, None)

        packet_str = packet.serialize()
        self.push ('%08x%s' % (len(packet_str), packet_str))


    def close(self):
        asynchat.async_chat.close(self)


    def collect_incoming_data (self, data):
        self._receive_buffer.append (data)

    def found_terminator (self):
        self._receive_buffer, data = [], ''.join (self._receive_buffer)

        if self._pstate is self.STATE_LENGTH:
            packet_length = int(data, 16)
            self.set_terminator(packet_length)
            self._pstate = self.STATE_PACKET
        else:
            # We shoud wait until last record
            packet = Packet.deserialize(data)

            print "PACKET=", packet
            if isinstance(packet, ErrorPacket):
                print packet._traceback

            if packet.is_last():
                self.close()
            else:
                # Prepare for next packet
                self.set_terminator (8)
                self._pstate = self.STATE_LENGTH

c = Client()
asyncore.loop()

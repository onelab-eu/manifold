import threading, asynchat, os, os.path, socket, traceback

from types                          import StringTypes

# Bugfix http://hg.python.org/cpython/rev/16bc59d37866 (FC14 or less)
# On recent linux distros we can directly "import asyncore"
from platform   import dist
distribution, _, _ = dist()
if distribution == "fedora":
    import manifold.util.asyncore as asyncore
else:
    import asyncore

from manifold.core.interface        import Interface
from manifold.core.operator_slot    import ChildSlotMixin
from manifold.core.packet           import Packet
from manifold.util.constants        import SOCKET_PATH
from manifold.util.filesystem       import ensure_writable_directory
from manifold.util.log              import Log
from manifold.util.type             import accepts, returns

class State(object):
    pass

class QueryHandler(asynchat.async_chat, ChildSlotMixin):

    STATE_LENGTH = State()
    STATE_PACKET = State()

    def __init__(self, conn, addr, callback):
        asynchat.async_chat.__init__ (self, conn)
        ChildSlotMixin.__init__(self) # XXX
        self.addr = addr

        # Reading socket data is done in two steps: first get the length of the
        # packet, then read the packet of a given length
        self.pstate = self.STATE_LENGTH
        self.set_terminator(8)
        self._receive_buffer = []
        self.callback = callback

    def collect_incoming_data(self, data):
        self._receive_buffer.append (data)

    def found_terminator(self):
        self._receive_buffer, data = [], ''.join(self._receive_buffer)

        if self.pstate is self.STATE_LENGTH:
            packet_length = int(data, 16)
            self.set_terminator(packet_length)
            self.pstate = self.STATE_PACKET
        else:
            self.set_terminator (8)
            self.pstate = self.STATE_LENGTH

            packet = Packet.deserialize(data)

            self.callback(self.addr, packet, receiver = self) or ""

    def receive(self, packet):
        packet_str = packet.serialize()
        self.push(("%08x" % len(packet_str)) + packet_str)


class UNIXSocketInterface(Interface, asyncore.dispatcher):

    __interface_name__ = 'unix'

    def __init__(self, router, socket_path = SOCKET_PATH):
        Interface.__init__(self, router)
        self._socket_path = socket_path
        asyncore.dispatcher.__init__(self)

        Log.info("Binding to %s" % self._socket_path)
        ensure_writable_directory(os.path.dirname(self._socket_path))
        if os.path.exists(self._socket_path):
            raise RuntimeError("%s is already in use" % self._socket_path)
        self.create_socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind(self._socket_path)
        self.listen(128)

        self._thread = threading.Thread(target=asyncore.loop)
        self._thread.start()     

    def terminate(self):
        # Closes the current asyncore channel. As it should be the only asyncore
        # channel open, this should terminate the asyncore loop
        # XXX If clients are still in, they prevent the close() call to complete
        self.close()
        self._thread.join()

        # Remove the socket file
        if os.path.exists(self._socket_path):
            Log.info("Unlinking %s" % self._socket_path)
            os.unlink(self._socket_path)

    #DEFAULTS = {
    #    "socket_path"     : SOCKET_PATH,
    #}
    #
    #@staticmethod
    #def init_options():
    #    """
    #    """
    #    options = Options()
    #
    #    options.add_argument(
    #        "-S", "--socket", dest = "socket_path",
    #        help = "Socket that will read the Manifold router.",
    #        default = RouterDaemon.DEFAULTS["socket_path"]
    #    )

    @returns(StringTypes)
    def get_socket_path(self):
        """
        Returns:
            The absolute path of the socjet used by this ManifoldServer.
        """
        return self._socket_path

    def handle_accept(self):
        conn, addr = self.accept()
        return QueryHandler(conn, addr, self.on_received)

    def on_received(self, addr, packet, receiver):
        packet.set_receiver(receiver)
        self._router.receive(packet)

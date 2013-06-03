# NOTE
# http://stackoverflow.com/questions/319279/how-to-validate-ip-address-in-python
# The IPy module (a module designed for dealing with IP addresses) will throw a ValueError exception for invalid addresses.
# Python >=3.3 includes the ipaddress module
# - http://hg.python.org/cpython/file/default/Lib/ipaddress.py
# Python 2.7 compatible version obtained from:
# https://ipaddr-py.googlecode.com/files/ipaddr-2.1.10.tar.gz

from manifold.util.ipaddr import IPv4Address, IPv6Address

class inet(str):
    __typename__ = 'inet'

    def __init__(self, address, version=None):
        self.address = address
        # Code borrowed from IPAddress() function in ipaddr module
        if version:
            if version == 4:
                self._ip = IPv4Address(address)
                return
            elif version == 6:
                self._ip = IPv6Address(address)
                return

        try:
            self._ip = IPv4Address(address)
            return
        except Exception, e:#(AddressValueError, NetmaskValueError):
            pass

        try:
            self._ip = IPv6Address(address)
            return
        except Exception:#(AddressValueError, NetmaskValueError):
            pass

        raise ValueError('%r does not appear to be an IPv4 or IPv6 address' %
                         address)

    def __repr__(self):
        return "<inet: %s>" % self.address

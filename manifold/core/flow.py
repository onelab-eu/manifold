# -*- coding: utf-8 -*-
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé         <jordan.auge@lip6.fr>
#   Marc-Olivier Buob   <marc-olivier.buob@lip6.fr>
#   Loïc Baron          <loic.baron@lip6.fr>

from types                      import StringTypes

from manifold.core.address      import Address
from manifold.util.type         import accepts, returns

class Flow(object):
    def __init__(self, source, destination):
        """
        Constructor.
        Args:
            source: The source Address of this Flow.
            destination: The destination Address of this Flow.
        """
        self._source = source
        self._destination = destination

    @returns(Address)
    def get_source(self):
        """
        Returns:
            The source Address of this Flow.
        """
        return self._source

    @returns(Address)
    def get_destination(self):
        """
        Returns:
            The destination Address of this Flow.
        """
        return self._destination

    #@returns(Flow)
    def get_reverse(self):
        """
        Make the reverse Flow of this Flow.
        Returns:
            The reverse Flow.
        """
        return Flow(self._destination, self._source)

    @returns(bool)
    def __eq__(self, other):
        """
        Tests whether two Flows are equal or not.
        Args:
            other: A Flow instance.
        Returns:
            True iif self == other.
        """
        is_direct  = self._source == other._source and \
                     self._destination == other._destination
        #is_reverse = self._source == other._destination and \
        #             self._destination == other._source
        return is_direct #or is_reverse
        #return self._data == other._record and self._last == other._last

    def __hash__(self):
        # ORDER IS IMPORTANT
        return hash((self._source, self._destination))
        #return hash(frozenset([self._source, self._destination]))

    @returns(StringTypes)
    def __repr__(self):
        return "<Flow %s -> %s>" % (self.get_source(), self.get_destination())



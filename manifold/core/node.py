# -*- coding: utf-8 -*-

import random

class Node(object):
    """
    A processing node. Base object
    """

    #---------------------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------------------

    def __init__(self):
        self._identifier =  random.randint(0, 9999)


    #---------------------------------------------------------------------------
    # Accessors
    #---------------------------------------------------------------------------

    def get_identifier(self):
        return self._identifier


    #---------------------------------------------------------------------------
    # Methods
    #---------------------------------------------------------------------------

    def tab(self, indent):
        """
        \brief print _indent_ tabs
        """
        print "[%04d]" % self._identifier, ' ' * 4 * indent,
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

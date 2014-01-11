#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Constants used by Packet and ResultValue classes.
#
# Copyright (C) UPMC Paris Universitas
# Authors:
#   Jordan Augé       <jordan.auge@lip6.fr>
#   Marc-Olivier Buob <marc-olivier.buob@lip6.fr>

# type
SUCCESS     = 0
WARNING     = 1
ERROR       = 2

# origin
CORE        = 0
GATEWAY     = 1

# code
SUCCESS     = 0
SERVERBUSY  = 32001
BADARGS     = 1
ERROR       = 2
FORBIDDEN   = 3
BADVERSION  = 4
SERVERERROR = 5
TOOBIG      = 6
REFUSED     = 7
TIMEDOUT    = 8
DBERROR     = 9
RPCERROR    = 10

# description
ERRSTR = {
    SUCCESS     : 'Success',
    SERVERBUSY  : 'Server is (temporarily) too busy; try again later',
    BADARGS     : 'Bad Arguments: malformed',
    ERROR       : 'Error (other)',
    FORBIDDEN   : 'Operation Forbidden: eg supplied credentials do not provide sufficient privileges (on the given slice)',
    BADVERSION  : 'Bad Version (eg of RSpec)',
    SERVERERROR : 'Server Error',
    TOOBIG      : 'Too Big (eg request RSpec)',
    REFUSED     : 'Operation Refused',
    TIMEDOUT    : 'Operation Timed Out',
    DBERROR     : 'Database Error',
    RPCERROR    : ''
}


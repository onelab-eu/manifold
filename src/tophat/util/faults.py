#
# PLCAPI XML-RPC faults
#
# Aaron Klingaman <alk@absarokasoft.com>
# Mark Huang <mlhuang@cs.princeton.edu>
#
# Copyright (C) 2004-2006 The Trustees of Princeton University
# $Id: Faults.py 14587 2009-07-19 13:18:50Z thierry $
# $URL: http://svn.planet-lab.org/svn/PLCAPI/tags/PLCAPI-4.3-29/PLC/Faults.py $
#

import xmlrpclib

class PLCFault(xmlrpclib.Fault):
    def __init__(self, faultCode, faultString, extra = None):
        if extra:
            faultString += ": " + extra
        xmlrpclib.Fault.__init__(self, faultCode, faultString)

class PLCInvalidAPIMethod(PLCFault):
    def __init__(self, method, role = None, extra = None):
        faultString = "Invalid method " + method
        if role:
            faultString += " for role " + role
        PLCFault.__init__(self, 100, faultString, extra)

class PLCInvalidArgumentCount(PLCFault):
    def __init__(self, got, min, max = min, extra = None):
        if min != max:
            expected = "%d-%d" % (min, max)
        else:
            expected = "%d" % min
        faultString = "Expected %s arguments, got %d" % \
                      (expected, got)
        PLCFault.__init__(self, 101, faultString, extra)

class PLCInvalidArgument(PLCFault):
    def __init__(self, extra = None, name = None):
        if name is not None:
            faultString = "Invalid %s value" % name
        else:
            faultString = "Invalid argument"
        PLCFault.__init__(self, 102, faultString, extra)

class PLCAuthenticationFailure(PLCFault):
    def __init__(self, extra = None):
        faultString = "Failed to authenticate call"
        PLCFault.__init__(self, 103, faultString, extra)

class PLCDBError(PLCFault):
    def __init__(self, extra = None):
        faultString = "Database error"
        PLCFault.__init__(self, 106, faultString, extra)

class PLCPermissionDenied(PLCFault):
    def __init__(self, extra = None):
        faultString = "Permission denied"
        PLCFault.__init__(self, 108, faultString, extra)

class PLCNotImplemented(PLCFault):
    def __init__(self, extra = None):
        faultString = "Not fully implemented"
        PLCFault.__init__(self, 109, faultString, extra)

class PLCAPIError(PLCFault):
    def __init__(self, extra = None):
        faultString = "Internal API error"
        PLCFault.__init__(self, 111, faultString, extra)

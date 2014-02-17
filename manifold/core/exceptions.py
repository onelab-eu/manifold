# -*- coding: utf-8 -*-

# In this file we will declare all the exceptions that should be handled by
# synchronous code
# The mapping with ICMP codes and types will also be done

#-------------------------------------------------------------------------------
# 0. Base class for errors
#-------------------------------------------------------------------------------

# NOTE: RuntimeError is not the right one:
# http://docs.python.org/2/library/exceptions.html#exceptions.RuntimeError
class ManifoldException(Exception):
    """
    Base class for all exceptions raised in Manifold.
    
    We expect two levels of hierarchy corresponding to the ICMP type and code
    fields.
    """
    TYPE = 0
    # Note: we always set the 0 value for parent classes
    CODE = 0

#-------------------------------------------------------------------------------
# 1. Internal error
#-------------------------------------------------------------------------------

class InternalException(ManifoldException):
    """
    This class regroups all exceptions that are due to the Manifold codebase
    itself: bugs, etc. We use it for example in assertions, or to catch
    situations that should never occur.  As such, it should be properly
    monitored.
    """
    TYPE = 1

# 1.1. Dummy internal error

class DummyInternalException(InternalException):
    CODE = 1

#-------------------------------------------------------------------------------
# 2. Client configuration errors / Authorization errors
#-------------------------------------------------------------------------------

class ClientException(ManifoldException):
    """
    """
    TYPE = 2

class MissingCredentialException(ClientException):
    """
    Shall we refine ?
        # delegation ?
    Note : we have an infinite number of credential types (= number of objects)
    and object names.
    """
    CODE = 1

#-------------------------------------------------------------------------------
# 3. Gateway error
#-------------------------------------------------------------------------------

class GatewayException(ManifoldException):
    TYPE = 3

class ManagementException(GatewayException):
    CODE = 1

#-------------------------------------------------------------------------------
# 4. Gateway error
#-------------------------------------------------------------------------------

class StorageException(ManifoldException):
    TYPE = 4

class NoUserException(StorageException):
    CODE = 1

class NoPlatformException(StorageException):
    CODE = 2

class NoAccountException(StorageException):
    CODE = 3

class NoAdminUserException(StorageException):
    CODE = 4

class NoAdminAccountException(StorageException):
    CODE = 5

#-------------------------------------------------------------------------------
# 5. Other types of errors
#-------------------------------------------------------------------------------

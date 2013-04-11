from manifold.models import *
import time

import crypt, base64, random
from hashlib import md5

from manifold.gateways.sfa import ADMIN_USER

#-------------------------------------------------------------------------------
# Helper functions
#-------------------------------------------------------------------------------

row2dict = lambda r: {c.name: getattr(r, c.name) for c in r.__table__.columns}

# XXX This should be replaced by User and Platform object classes
def make_account_dict(account):
    account_dict = row2dict(account)
    account_dict['platform'] = account.platform.platform
    #del account_dict['platform_id']
    #del account_dict['config'] # XXX
    return account_dict

def make_user_dict(user):
    user_dict = row2dict(user)
    user_dict['accounts'] = [make_account_dict(a) for a in user.accounts]
    #del user_dict['user_id']
    del user_dict['password']
    return user_dict

# TODO Shall we track origin of newly created users ?

#-------------------------------------------------------------------------------
# Exceptions
#-------------------------------------------------------------------------------

class AuthenticationFailure(Exception): pass

#-------------------------------------------------------------------------------
# Auth class
#-------------------------------------------------------------------------------

# http://code.activestate.com/recipes/86900/

class AuthMethod(object):
    """
    """
    def __init__(self, auth):
        self.auth = auth

class PasswordAuth(AuthMethod):
    """
    """
    def check(self):
        # Method.type_check() should have checked that all of the
        # mandatory fields were present.
        assert self.auth.has_key('Username')
        
        # Get record (must be enabled)
        try:
            user = db.query(User).filter(User.email == self.auth['Username'].lower()).one()
        except Exception, e:
            raise AuthenticationFailure, "No such account (PW): %s" % e

        # Compare encrypted plaintext against encrypted password stored in the DB
        plaintext = self.auth['AuthString'].encode('latin1') # XXX method.api.encoding)
        password = user.password

        # Protect against blank passwords in the DB
        if password is None or password[:12] == "" or \
            crypt.crypt(plaintext, password[:12]) != password:
            raise AuthenticationFailure, "Password verification failed %s" % crypt.crypt(plaintext, password[:12])

        return user

class AnonymousAuth(AuthMethod):
    def check(self):
        return None

class SessionAuth(AuthMethod):
    """
    Secondary authentication method. After authenticating with a
    primary authentication method, call GetSession() to generate a
    session key that may be used for subsequent calls.
    """

    def check(self):
        assert self.auth.has_key('session')

        try:
            sess = db.query(Session).filter(Session.session == self.auth['session']).one()
        except Exception, e:
            raise AuthenticationFailure, "No such session: %s" % e

        user = sess.user
        if user and sess.expires > time.time():
            return user
        else:
            db.delete(sess)
            raise AuthenticationFailure, "Invalid session"

    def get_session(self, user):
        assert user, "A user associated to a session should not be NULL"
        # Before a new session is added, delete expired sessions
        db.query(Session).filter(Session.expires < int(time.time())).delete()

        s = Session()
        # Generate 32 random bytes
        bytes = random.sample(xrange(0, 256), 32)
        # Base64 encode their string representation
        s.session = base64.b64encode("".join(map(chr, bytes)))
        s.user = user #self.authenticate(self.auth)
        s.expires = int(time.time()) + (24 * 60 * 60)
        db.add(s)
        db.commit()
        return s.session


class PLEAuth(AuthMethod):
    """
    Authentication towards PLE
    """

    def check(self):
        pleauthmethod = self.auth['RemoteAuthMethod'] if self.auth.has_key('RemoteAuthMethod') else 'password'

        # Check PLE authentication
        import xmlrpclib
        ple = xmlrpclib.ServerProxy('https://www.planet-lab.eu/PLCAPI/', allow_none=True)
        auth = self.auth.copy()

        # We support all PLE authentication methods, default one is password
        if auth.has_key('RemoteAuthMethod'):
           auth['AuthMethod'] = auth['RemoteAuthMethod']
           del auth['RemoteAuthMethod']
        else:
            auth['AuthMethod'] = 'password' 
        if not ple.AuthCheck(auth):
            raise AuthenticationFailure, "No such PlanetLab Europe account"

        # Eventually returns local user based on email, or return newly created user 
        # NOTE: we trust PLE for email validation
        user_email = auth['Username'].lower()
        try:
            return db.query(User).filter(User.email == user_email).one()
        except:
            user = User(email=auth['Username'].lower())
            db.add(user)
            db.commit()
            return user

class PLCAuth(AuthMethod):
    """
    Authentication towards PLC
    """

    def check(self):
        pleauthmethod = self.auth['RemoteAuthMethod'] if self.auth.has_key('RemoteAuthMethod') else 'password'

        # Check PLC authentication
        import xmlrpclib
        ple = xmlrpclib.ServerProxy('https://www.planet-lab.org/PLCAPI/', allow_none=True)
        auth = self.auth.copy()

        # We support all PLC authentication methods, default one is password
        if auth.has_key('RemoteAuthMethod'):
           auth['AuthMethod'] = auth['RemoteAuthMethod']
           del auth['RemoteAuthMethod']
        else:
            auth['AuthMethod'] = 'password' 
        if not ple.AuthCheck(auth):
            raise AuthenticationFailure, "No such PlanetLab Europe account"

        # Eventually returns local user based on email, or return newly created user 
        # NOTE: we trust PLC for email validation
        user_email = auth['Username'].lower()
        try:
            return db.query(User).filter(User.email == user_email).one()
        except:
            user = User(email=auth['Username'].lower())
            db.add(user)
            db.commit()
            return user

class ManagedAuth(AuthMethod):
    """
    """

    def check(self):
        assert auth.has_key('AuthAdmin')
        assert auth.has_key('Username')

        if auth['AuthAdmin'] != ADMIN_USER:
            raise AuthenticationFailure, "Failed authentication as administrator"

        admin_auth = self.auth.copy()
        admin_auth['Username'] = auth['AuthAdmin']
        del admin_auth['AuthAdmin']

        try:
            Auth(admin_auth).check()
        except Exception, e:
            raise AuthenticationFailure, "Failed authentication as administrator"

        # We might either get email or HRN (from certificate authentication)
        # If we get the HRN, we need to query the registry, but we are not even
        # sure to get access to the data...
        if '@' not in self.auth['Username']:
            # FIXME We get HRN, how to get email ?
            user_email = "%s@sfa" % self.auth['Username'].lower()
        else:
            user_email = auth['Username'].lower()

        try:
            return db.query(User).filter(User.email == user_email).one()
        except:
            user = User(email=auth['Username'].lower())
            db.add(user)
            db.commit()
            return user

class Auth(object):

    auth_map = {
        'anonymous': AnonymousAuth,
        'password': PasswordAuth,
        'session': SessionAuth,
        'ple': PLEAuth,
        'plc': PLCAuth,
        'managed': ManagedAuth
    }

    def __init__(self, auth):
        if not 'AuthMethod' in auth:
            raise AuthenticationFailure, "AuthMethod should be specified"

        try:
            self.auth_method = self.auth_map[auth['AuthMethod']](auth)
        except Exception, e:
            raise AuthenticationFailure, "Unsupported authentication method: %s" % auth['AuthMethod']

    def check(self):
        return self.auth_method.check()

    def AuthCheck(self):
        return 1

    # These are temporary functions...

    def GetSession(self, *args):
        auth = args[0]
        user = Auth(auth).check()
        return SessionAuth(auth).get_session(user)

    def GetPersons(self, *args):
        auth = args[0]
        print "getpersons auth=", auth
        user = Auth(auth).check()
        dic = make_user_dict(user)
        dic.update({'first_name': 'FIRST', 'last_name': 'LAST'})
        return [dic]
        #return [{'email': user.email, 'first_name': user.email, 'last_name': '', 'user_hrn': 'TODO'}]



import time, crypt, base64, random
from hashlib                    import md5

try:
    from manifold.gateways.sfa  import ADMIN_USER
except:
    ADMIN_USER = 'admin' # XXX

from manifold.core.query        import Query
from manifold.util.log          import Log

#-------------------------------------------------------------------------------
# Helper functions
#-------------------------------------------------------------------------------

#UNUSED|row2dict = lambda r: {c.name: getattr(r, c.name) for c in r.__table__.columns}
#UNUSED|
#UNUSED|# XXX This should be replaced by User and Platform object classes
#UNUSED|def make_account_dict(account):
#UNUSED|    account_dict = row2dict(account)
#UNUSED|    account_dict['platform'] = account.platform.platform
#UNUSED|    #del account_dict['platform_id']
#UNUSED|    #del account_dict['config'] # XXX
#UNUSED|    return account_dict
#UNUSED|
#UNUSED|def make_user_dict(user):
#UNUSED|    user_dict = row2dict(user)
#UNUSED|    user_dict['accounts'] = [make_account_dict(a) for a in user.accounts]
#UNUSED|    #del user_dict['user_id']
#UNUSED|    del user_dict['password']
#UNUSED|    return user_dict
#UNUSED|
# TODO Shall we track origin of newly created users ?

#-------------------------------------------------------------------------------
# Exceptions
#-------------------------------------------------------------------------------

class AuthenticationFailure(Exception):
    pass

#-------------------------------------------------------------------------------
# AuthMethod class
#-------------------------------------------------------------------------------

# http://code.activestate.com/recipes/86900/

class AuthMethod(object):
    """
    """
    def __init__(self, auth, router):
        self.router = router
        self.auth = auth

class PasswordAuth(AuthMethod):
    """
    """
    def check(self):
        # Method.type_check() should have checked that all of the
        # mandatory fields were present.
        assert self.auth.has_key('Username')
        
        # Get record (must be enabled)
        query_users = Query.get("local:user").filter_by("email", "==", self.auth["Username"].lower())
        users = self.router.execute_local_query(query_users)
        if not users:
            raise AuthenticationFailure, "No such account (PW): %s" % self.auth["Username"]
        user = users[0]

        # Compare encrypted plaintext against encrypted password stored in the DB
        plaintext = self.auth['AuthString'].encode('latin1') # XXX method.api.encoding)
        password = user['password']

        # Protect against blank passwords in the DB
        if password is None or password[:12] == "" or \
            crypt.crypt(plaintext, password[:12]) != password:
            raise AuthenticationFailure, "Password verification failed"

        return user

class AnonymousAuth(AuthMethod):
    def check(self):
        return {}

class GIDAuth(AuthMethod):
    def check(self):
        request = self.auth.request
        # Have we been authenticated by the ssl layer ?
        peer_certificate = request.channel.transport.getPeerCertificate()
        user_hrn = peer_certificate.get_subject().commonName if peer_certificate else None

        if not user_hrn:
            raise AuthenticationFailure, "GID verification failed"

        # We need to map the SFA user to the Manifold user... let's search into his accounts

        query_user_id = Query.get('local:linked_account').filter_by('identifier', '==', user_hrn).select('user_id')
        ret_user_ids = self.router.execute_local_query(query_user_id)
        if ret_user_ids['code'] != 0:
            raise Exception, "Failure requesting linked accounts for identifier '%s'" % user_hrn
        user_ids = ret_user_ids['value']
        if not user_ids:
            raise Exception, "No linked account found with identifier '%s'" % user_hrn
        print "user_ids", user_ids
        user_id = user_ids[0]['user_id']

        query_user = Query.get('local:user').filter_by('user_id', '==', user_id)
        users = self.router.execute_local_query(query_user)
        #if ret_users['code'] != 0:
        #    raise Exception, "Failure requesting linked accounts for identifier '%s'" % user_hrn
        #users = ret_users['value']
        if not users:
            raise Exception, "Internal error: no user found with user_id = '%d'" % user_id
        user = users[0]

        print "Linked SFA account '%s' for user: %r" % (user_hrn, user)


class SessionAuth(AuthMethod):
    """
    Secondary authentication method. After authenticating with a
    primary authentication method, call GetSession() to generate a
    session key that may be used for subsequent calls.
    """

    def check(self):
        assert self.auth.has_key('session')

        query_sessions = Query.get('local:session').filter_by('session', '==', self.auth['session'])
        sessions = self.router.execute_local_query(query_sessions)
        if not sessions:
            raise AuthenticationFailure, "No such session: %s" % e
        session = sessions[0]

        user_id = session['user_id']
        query_users = Query.get('local:user').filter_by('user_id', '==', user_id)
        users = self.router.execute_local_query(query_users)
        if not users:
            raise AuthenticationFailure, "No such user_id: %s" % e
        user = users[0]
        if user and session['expires'] > time.time():
            return user
        else:
            query_sessions = Query.delete('local:session').filter_by('session', '==', session['session'])
            try:
                self.router.execute_local_query(query_sessions)
            except: pass
            raise AuthenticationFailure, "Invalid session"

    def get_session(self, user):
        assert user, "A user associated to a session should not be NULL"
        # Before a new session is added, delete expired sessions
        query_sessions = Query.delete('local:session').filter_by('expires', '<', int(time.time()))
        try:
            self.router.execute_local_query(query_sessions)
        except: pass

        # Generate 32 random bytes
        bytes = random.sample(xrange(0, 256), 32)
        # Base64 encode their string representation

        session_params = {
            'session': base64.b64encode("".join(map(chr, bytes))),
            'user_id': user['user_id'],
            'expires': int(time.time()) + (24 * 60 * 60)
        }

        query_session = Query.create('local:session').set(session_params).select('session')
        try:
            session, = self.router.execute_local_query(query_sessions)
        except: pass

        return session['session']


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
        assert self.auth.has_key('AuthAdmin')
        assert self.auth.has_key('Username')
        assert self.auth.has_key('AuthString')

        # Only the ADMIN_USER can perform such authentication at the moment
        if self.auth['AuthAdmin'] != ADMIN_USER:
            raise AuthenticationFailure, "Failed authentication as administrator"

        # We authenticate the ADMIN_USER thanks to PasswordAuth 
        admin_auth = {
            'AuthMethod': 'password',
            'Username'  : self.auth['AuthAdmin'],
            'AuthString': self.auth['AuthString']
        }
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
        'managed': ManagedAuth,
        'gid': GIDAuth
    }

    def __init__(self, auth, router):
        if not 'AuthMethod' in auth:
            raise AuthenticationFailure, "AuthMethod should be specified"

        try:
            self.auth_method = self.auth_map[auth['AuthMethod']](auth, router)
        except Exception, e:
            raise AuthenticationFailure, "Unsupported authentication method: %s, %s" % (auth['AuthMethod'], e)

    def check(self):
        return self.auth_method.check()

# deprecated #     # These are temporary functions...
# deprecated # 
# deprecated #     def GetSession(self, *args):
# deprecated #         auth = args[0]
# deprecated #         user = Auth(auth).check()
# deprecated #         return SessionAuth(auth).get_session(user)
# deprecated # 
# deprecated #     def GetPersons(self, *args):
# deprecated #         auth = args[0]
# deprecated #         print "getpersons auth=", auth
# deprecated #         user = Auth(auth).check()
# deprecated #         dic = make_user_dict(user)
# deprecated #         dic.update({'first_name': 'FIRST', 'last_name': 'LAST'})
# deprecated #         return [dic]
# deprecated #         #return [{'email': user.email, 'first_name': user.email, 'last_name': '', 'user_hrn': 'TODO'}]
# deprecated # 
# deprecated # 

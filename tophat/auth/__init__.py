from tophat.models import *

class AuthenticationFailure(Exception): pass

class Auth(object):
    def __new__(cls, auth):
        if not 'AuthMethod' in auth:
            raise AuthenticationFailure, "AuthMethod should be specified"
        if auth['AuthMethod'] == 'anonymous':
            return super(Auth, cls).__new__(Anonymous)
        elif auth['AuthMethod'] == 'password':
            return super(Auth, cls).__new__(Password)
        else:
            raise AuthenticationFailure, "Unsupported authentication method: %s" % auth['AuthMethod']
    def __init__(self, auth):
        self.auth = auth

class Password(Auth):
    """
    """
    def check(self):
        # Method.type_check() should have checked that all of the
        # mandatory fields were present.
        assert self.auth.has_key('Username')
        
        # Get record (must be enabled)
        users = session.query(User).filter(User.email == self.auth['Username'].lower()).all()
        if len(users) != 1:
            raise AuthenticationFailure, "No such account (PW)"

        user = users[0]

        print "W: skipped password verification"
        return user

        # Compare encrypted plaintext against encrypted password stored in the DB
        plaintext = self.auth['AuthString'].encode('latin1') # XXX method.api.encoding)
        password = user['password']

        # Protect against blank passwords in the DB
        if password is None or password[:12] == "" or \
            crypt.crypt(plaintext, password[:12]) != password:
            raise AuthenticationFailure, "Password verification failed"

        return user

class Anonymous(Auth):
    def check(self):
        return None

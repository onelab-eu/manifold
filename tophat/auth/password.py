class Password(Auth):
    """
    """
    def check(self):
        # Compare encrypted plaintext against encrypted password stored in the DB
        plaintext = auth['AuthString'].encode(method.api.encoding)
        password = person['password']


        # Protect against blank passwords in the DB
        if password is None or password[:12] == "" or \
            crypt.crypt(plaintext, password[:12]) != password:
            raise PLCAuthenticationFailure, "Password verification failed"

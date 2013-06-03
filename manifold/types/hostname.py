import re

#ValidHostnameRegex = "^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$";

# Either any 2 char country code, or TLD form list IANA.ORG
VALID_HOSTNAME_STR = "^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([a-z]{2}|AERO|ARPA|ASIA|BIZ|CAT|COM|COOP|EDU|GOV|INFO|INT|JOBS|MIL|MOBI|MUSEUM|NAME|NET|ORG|POST|PRO|TEL|TRAVEL|XXX)$"
VALID_HOSTNAME_RX  = re.compile(VALID_HOSTNAME_STR, re.IGNORECASE)

class hostname(str):
    __typename__ = 'hostname'

    def __init__(self, value):
        if not VALID_HOSTNAME_RX.match(value):
            raise ValueError, "%s does not appear to be a valid hostname" % value
        super(hostname, self).__init__(value)

    def __repr__(self):
        return "<hostname: %s>" % self

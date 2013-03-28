class Platform(object):
    def __init__(self, name, gateway_name, gateway_config, auth_type):
        self.name = name
        self.gateway_name = gateway_name
        self.gateway_config = gateway_config
        self.auth_type = auth_type

    def __str__(self):
        return "<Platform %s (%s [%r])>" % (self.name, self.gateway_name, self.gateway_config)
    def __repr__(self):
        return str(self)

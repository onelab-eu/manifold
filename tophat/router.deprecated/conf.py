class Conf:
    """
    Implements a router configuration.

    - Where static routes are stored
    """

    def __init__(self):
        self.settings = {
            'STATIC_ROUTES_FILE': "/usr/share/manifold/metadata/"
        }

    def __getattr__(self, name):
        return self.settings[name]



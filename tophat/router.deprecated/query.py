class Query:
    """
    Implements a query

    Analogy of a request to connectivity of an IP (!= announces that are prefixes)
    """

    def __init__(self, destination):
        self.destination = destination
        

class FlowTable(dict):
    """
    Implements a flow table.
    """
    def __init__(self, route_cls=object):
        self.route_cls = route_cls
        super(FlowTable, self).__init__(self)

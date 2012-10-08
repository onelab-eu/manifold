class Table:
    """
    Implements a database table schema.
    """

    def __init__(self, platform, name, fields, keys, partition=None, cost=1):
        self.platform = platform
        self.name = name
        self.fields = fields
        self.keys = keys
        self.partition = partition # an instance of a Filter
        # There will also be a list that the platform cannot provide, cf sources[i].fields
        self.cost = cost
        if isinstance(self.keys, (list, tuple)):
            self.keys = frozenset(self.keys)
        if isinstance(self.fields, (list, tuple)):
            self.fields = frozenset(self.fields)

    def __str__(self):
        #return "<Table name='%s' platform='%s' fields='%r' keys='%r'>" % (self.name, self.platform, self.fields, self.keys)
        if self.platform:
            return "%s::%s" % (self.platform, self.name)
        else:
            return self.name

    def get_fields_from_keys(self):
        fields = []
        for k in self.keys:
            if isinstance(k, (list, tuple)):
                fields.extend(list(k))
            else:
                fields.append(k)
        return fields

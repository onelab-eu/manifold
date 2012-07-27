from tophat.core.filter import Filter

class Query:
    def __init__(self, method, ts='latest', filters={}, fields=[]):
        self.method = method
        self.ts = ts
        self.filters = {}
        self.fields = []
        self.sort = None
        self.offset = None
        self.limit = None

        if filters:
            for k, v in filters.items():
                if k[0] == '-':
                    if k[1:] == 'SORT':
                        self.sort = v
                    elif k[1:] == 'OFFSET':
                        self.offset = v
                    elif k[1:] == 'LIMIT':
                        self.limit = v
                    else:
                        raise Exception, "Unknown special field in filter: %s", k[1:]
                else:
                    self.filters[k] = v
            
        self.fields = fields

    def __str__(self):
        return "<Query method='%s' ts='%s' filter='%r' fields='%r'...>" % (self.method, self.ts, self.filters, self.fields)

    def to_tuple(self):
        return (self.method, self.ts, self.filters, self.fields)

    def get_method(self):
        return self.method

    def get_ts(self):
        return self.ts

    def get_filters(self):
        filters = []
        for k,v in self.filters.items():
            if k[0] == '-':
                continue
            elif k[0] in Filter.modifiers:
                filters.append((k[1:], k[0], v))
            else:
                filters.append((k, '=', v))
        return filters

    def get_fields(self):
        result = []
        for f in self.fields:
            if f[0] in Filter.modifiers:
                result.append(f[1:])
            else:
                result.append(f)
        return result

    def get_params(self):
        return [self.method, self.ts, self.filter, self.fields]

    def get_sort(self):
        for k,v in self.filters.items():
            if k == '-SORT':
                return v
        return None


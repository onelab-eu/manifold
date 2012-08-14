def xgetattr(cls, list_attr):
    ret = []
    for a in list_attr:
        ret.append(getattr(cls,a))
    return tuple(ret)

def get_sqla_filters(cls, filters):
    if filters:
        _filters = None
        for p in filters:
            f = p.op(getattr(cls, p.key), p.value)
            if _filters:
                _filters = f and _filters
            else:
                _filters = f
        return _filters
    else:
        return None


# Define the inclusion operator
class contains(type): pass

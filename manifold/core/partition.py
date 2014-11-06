from manifold.core.filter           import Filter

class Partition(Filter):
    pass

class Partitions(set):
    """
    Partitions are a set of filter.
    """

    def to_list(self):
        ret = list()
        for partition in self:
            ret.append(partition.to_dict())
        return ret
    


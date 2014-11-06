from manifold.core.filter           import Filter

class Partition(Filter):
    pass

class Partitions(set):
    """
    Partitions are a set of filter.
    """

    @staticmethod
    def from_list(self):
        ret = Partitions()
        for partition_list in self:
            ret.add(Partition.from_list(partition_list))
        return ret
             
    def to_list(self):
        ret = list()
        for partition in self:
            ret.append(partition.to_list())
        return ret
    


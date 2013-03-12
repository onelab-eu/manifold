#OBSOLETE|from tophat.core.ast  import From
#OBSOLETE|from twisted.internet import reactor
#OBSOLETE|
#OBSOLETE|class SourceManager(list):
#OBSOLETE|
#OBSOLETE|    def __init__(self, reactor):
#OBSOLETE|        self.reactor = reactor
#OBSOLETE|        list.__init__(self)
#OBSOLETE|
#OBSOLETE|    def run(self):
#OBSOLETE|        for item in self:
#OBSOLETE|            if item.do_start and not item.started:
#OBSOLETE|                item.start()
#OBSOLETE|
#OBSOLETE|    def append(self, item):
#OBSOLETE|        if not isinstance(item, From):
#OBSOLETE|            raise TypeError("Item of class From expected in argument. Got %s" % item.__class__.__name__)
#OBSOLETE|        pos = len(self)
#OBSOLETE|        list.append(self, item)
#OBSOLETE|        return pos
#OBSOLETE|
#OBSOLETE|    def start(self, pos):
#OBSOLETE|        self[pos].start()

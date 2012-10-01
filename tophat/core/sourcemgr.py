from tophat.core.ast import FromNode
from twisted.internet import reactor

class SourceManager(list):

    def __init__(self, reactor):
        self.reactor = reactor
        list.__init__(self)

    def run(self):
        for item in self:
            if item.do_start and not item.started:
                item.start()

    def append(self, item):
        if not isinstance(item, FromNode):
            raise TypeError("Item of class FromNode expected in argument. Got %s" % item.__class__.__name__)
        pos = len(self)
        list.append(self, item)
        return pos

    def start(self, pos):
        self[pos].start()

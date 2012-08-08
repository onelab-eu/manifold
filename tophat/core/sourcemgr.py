from tophat.core.nodes import SourceNode
from twisted.internet import reactor

class SourceManager(list):

    def __init__(self, reactor):
        self.reactor = reactor
        list.__init__(self)

    def run(self):
        for item in self:
            if not item.started:
                item.start()

    def append(self, item):
        if not isinstance(item, SourceNode):
            raise TypeError("Item of class SourceNode expected in argument. Got %s" % item.__class__.__name__)
        list.append(self, item)

# Borrowed from Chandler
# http://chandlerproject.org/Projects/ChandlerTwistedInThreadedEnvironment

import threading, time, signal
from twisted.internet           import defer
from twisted.python             import threadable

from manifold.util.singleton    import Singleton
from manifold.util.log          import Log

__author__ = "Brian Kirsch <bkirsch@osafoundation.org>"

#required for using threads with the Reactor
threadable.init()

class ReactorException(Exception):
      def __init__(self, *args):
            Exception.__init__(self, *args)


class ReactorThread(threading.Thread):
    """
    Run the Reactor in a Thread to prevent blocking the
    Main Thread once reactor.run is called
    """

    __metaclass__ = Singleton

    def __init__(self):
        threading.Thread.__init__(self)
        self._reactorRunning = False
        self._reactorStarted = False

        # Be sure the import is done only at runtime, we keep a reference in the
        # class instance
        from twisted.internet import reactor
        self.reactor = reactor

        self._num_instances = 0

    def run(self):
        if self._reactorRunning:
            raise ReactorException("Reactor Already Running")

        self._reactorRunning = True

        #call run passing a False flag indicating to the
        #reactor not to install sig handlers since sig handlers
        #only work on the main thread

        try:
            self.reactor.run(False)
        except Exception, e:
            print "Reactor exception:", e

    def callInReactor(self, callable, *args, **kw):
        if self._reactorRunning:
            self.reactor.callFromThread(callable, *args, **kw)
        else:
            callable(*args, **kw)

    def isReactorRunning(self):
        return self._reactorRunning

    def start_reactor(self):
        self._num_instances += 1

        if self._reactorStarted:
            Log.debug("Reactor already started")
            return
        self._reactorStarted = True

        # Should not occur
        if self._reactorRunning:
            Log.debug("Reactor already running: should not occur")
            return

        threading.Thread.start(self)
        cpt = 0
        while not self._reactorRunning:
            time.sleep(0.1)
            cpt +=1
            if cpt > 5:
                raise ReactorException, "Reactor thread is too long to start... cancelling"
        self.reactor.addSystemEventTrigger('after', 'shutdown', self.__reactorShutDown)

    def stop_reactor(self, force = False):
        """
        may want a way to force thread to join if reactor does not shutdown
        properly. The reactor can get in to a recursive loop condition if reactor.stop
        placed in the threads join method. This will require further investigation.
        """
        if not self._reactorRunning:
            raise ReactorException("Reactor Not Running")

        self._num_instances -= 1
        if not force and self._num_instances > 0:
            return

        print "STOPPING REACTOR !!"

        self._num_instances = 0
        # done here instead of event until shell.client.__del__ issue is solved
        self._reactorRunning = False
        self._reactorStarted = False
        self.reactor.callFromThread(self.reactor.stop)
        ReactorThread._drop()
        #self.reactor.join()

    def addReactorEventTrigger(self, phase, eventType, callable):
        if self._reactorRunning:
            self.reactor.callFromThread(self.reactor.addSystemEventTrigger, phase, eventType, callable)
        else:
            self.reactor.addSystemEventTrigger(phase, eventType, callable)

    def __reactorShuttingDown(self):
        pass

    def __reactorShutDown(self):
        """This method called when the reactor is stopped"""
        self._reactorRunning = False

    def __getattr__(self, name):
        # We transfer missing methods to the reactor
        def _missing(*args, **kwargs):
            self.reactor.callFromThread(getattr(self.reactor, name), *args, **kwargs)
        return _missing

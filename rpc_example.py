from twisted.internet import reactor, defer, task
from twisted.python.util import println
import time
from kontiki.rpc import ReconnectingPBClientFactory


class ExampleClient(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.factory = ReconnectingPBClientFactory()
        self.echoLoop = task.LoopingCall(self.echoTime)

    def callRemote(self, method, *args, **kwargs):
        rootObjectDeferred = self.factory.getRootObject()

        def caller(root):
            return root.callRemote(method, *args, **kwargs)

        rootObjectDeferred.addCallback(caller)
        return rootObjectDeferred

    def echoTime(self):
        t = time.time()
        print '(invoking %d)' % t
        response = self.callRemote('echo', 'hello %d' % t)
        response.addCallback(println)
        return response

    def run(self, interval=1.0):
        reactor.connectTCP(self.host, self.port, self.factory)
        self.echoLoop.start(interval, now=True)


if __name__ == '__main__':
    e = ExampleClient('localhost', 8789)
    e.echoTime()
    e.echoTime()
    e.echoTime()
    e.echoTime()
    e.run()
    reactor.run()

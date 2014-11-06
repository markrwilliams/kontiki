from twisted.spread.pb import PBClientFactory
from twisted.internet.protocol import ReconnectingClientFactory
from heapq import heapify, heappop


def nlargest(n, l):
    if not l:
        return l
    leastL = [-el for el in l]
    heapify(leastL)
    return [-heappop(leastL) for _ in xrange(min(n, len(l)))]


def majorityMedian(l):
    if not l:
        raise ValueError("no median for empty data")
    return nlargest(len(l) / 2 + 1, l)[-1]


class ReconnectingPBClientFactory(PBClientFactory,
                                  ReconnectingClientFactory):

    def clientConnectionLost(self, connector, unused_reason):
        PBClientFactory.clientConnectionLost(self,
                                             connector,
                                             unused_reason,
                                             reconnecting=True)
        ReconnectingPBClientFactory.clientConnectionLost(self,
                                                         connector,
                                                         unused_reason)

    def clientConnectionFailed(self, connector, reason):
        PBClientFactory.clientConnectionFailed(self, connector, reason)

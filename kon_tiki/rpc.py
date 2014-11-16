from twisted.internet.protocol import ReconnectingClientFactory
from twisted.spread import pb


class ReconnectingPBClientFactory(pb.PBClientFactory,
                                  ReconnectingClientFactory):

    def clientConnectionMade(self, broker):
        ReconnectingClientFactory.resetDelay(self)
        pb.PBClientFactory.clientConnectionMade(self, broker)

    clientConnectionFailed = ReconnectingClientFactory.clientConnectionFailed

    def clientConnectionLost(self, connector, reason):
        pb.PBClientFactory.clientConnectionLost(self, connector, reason,
                                                reconnecting=1)
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

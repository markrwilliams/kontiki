from twisted.internet import defer, reactor
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.spread import pb
from kontiki import raft, rpc_objects


rpc_objects.establishObjects()


class ReconnectingPBClientFactory(pb.PBClientFactory,
                                  ReconnectingClientFactory):

    def clientConnectionMade(self, broker):
        ReconnectingClientFactory.resetDelay(self)
        pb.PBClientFactory.clientConnectionMade(self, broker)

    def clientConnectionFailed(self, connector, reason):
        pb.PBClientFactory.clientConnectionFailed(self, connector, reason)
        ReconnectingClientFactory.clientConnectionFailed(self,
                                                         connector,
                                                         reason)

    def clientConnectionLost(self, connector, reason):
        pb.PBClientFactory.clientConnectionLost(self, connector, reason,
                                                reconnecting=1)
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)


class PeerRPC(object):
    _reactor = reactor

    def __init__(self, host, port, identity=None):
        if identity is None:
            identity = '%s:%s' % (host, port)
        self.identity = identity
        self.host, self.port = host, port
        self.perspectiveFactory = ReconnectingPBClientFactory()

    def callRemote(self, method, *args, **kwargs):
        rootObjectDeferred = self.perspectiveFactory.getRootObject()

        def call(root):
            return root.callRemote(method, *args, **kwargs)

        rootObjectDeferred.addCallback(call)
        return rootObjectDeferred

    def connect(self):
        self._reactor.connectTCP(self.host, self.port, self.perspectiveFactory)

    def stopTrying(self):
        self.perspectiveFactory.stopTrying()


class RaftServer(pb.Root):

    def __init__(self, identity, peers, persister, applyCommand,
                 electionTimeoutRange=(.150, .350)):
        self.identity = identity
        self.peers = {peer.identity: peer for peer in peers}
        self.persister = persister
        self.applyCommand = applyCommand
        self.electionTimeoutRange = electionTimeoutRange
        self.lock = defer.DeferredLock()
        self.state = raft.Follower(electionTimeoutRange=electionTimeoutRange,
                                   server=self,
                                   identity=identity,
                                   peers=self.peers,
                                   persister=persister,
                                   applyCommand=applyCommand)

    def begin(self):
        self.lock.run(self.state.begin)

    def changeState(self, newState):
        self.state = newState.fromState(self.electionTimeoutRange,
                                        server=self,
                                        state=self.state)
        return self.state.begin()  # lock should have already been acquired

    def _requestVote(self, *args, **kwargs):
        # This is an extra step of indirection so that self.state gets
        # run against the current state
        return self.state.requestVote(*args, **kwargs)

    def remote_requestVote(self,
                           term, candidateId, lastLogIndex, lastLogTerm):
        return self.lock.run(self._requestVote,
                             term, candidateId, lastLogIndex, lastLogTerm)

    def _appendEntries(self, *args, **kwargs):
        return self.state.appendEntries(*args, **kwargs)

    def remote_appendEntries(self, term, leaderId, prevLogIndex,
                             prevLogTerm, entries, leaderCommit):
        return self.lock.run(self._appendEntries, term, leaderId,
                             prevLogIndex, prevLogTerm, entries,
                             leaderCommit)

    def _command(self, command):
        return self.state.command(command)

    def remote_command(self, command):
        return self.lock.run(self._command, command)

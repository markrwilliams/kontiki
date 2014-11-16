from twisted.internet import defer
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.spread import pb
from kon_tiki import raft


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


class RaftServer(pb.Root):

    def __init__(self, identity, peers, persister, applyCommand,
                 electionTimeoutRange=(.150, .350)):
        self.identity = identity
        self.peers = peers
        self.persister = persister
        self.applyCommand = applyCommand
        self.electionTimeoutRange = electionTimeoutRange
        self.lock = defer.DeferredLock()
        self.state = raft.Follower(electionTimeoutRange=electionTimeoutRange,
                                   server=self,
                                   identity=identity,
                                   peers=peers,
                                   persister=persister,
                                   applyCommand=applyCommand)

    def begin(self):
        self.lock(self.state.begin)

    def changeState(self, newState, begin=True):
        self.state = newState.fromState(self.electionTimeoutRange,
                                        server=self,
                                        state=self.state)
        if begin:
            # lock should have already been acquired
            self.state.begin()

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
        return self.lock.run(self._appendEntries, leaderId, prevLogIndex,
                             prevLogTerm, entries, leaderCommit)

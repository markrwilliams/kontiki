from twisted.internet import task, reactor
from twisted.python import util
from kon_tiki.rpc import PeerRPC


peer = PeerRPC('localhost', 9876)
peer.connect()


def commandALot():
    d = peer.callRemote('command', 'hi')
    d.addBoth(util.println)


l = task.LoopingCall(commandALot)
l.start(1)
reactor.run()

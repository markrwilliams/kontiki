from twisted.internet import task, reactor
from twisted.python import util
from kon_tiki.rpc import PeerRPC
import time


peer = PeerRPC('localhost', 9876)
peer.connect()


def commandALot():
    d = peer.callRemote('command', 'hi @ %s' % time.time())
    d.addBoth(util.println)


l = task.LoopingCall(commandALot)
l.start(1)
reactor.run()

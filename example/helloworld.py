from twisted.python import log
from twisted.internet import reactor
from twisted.spread import pb
from kon_tiki.persist import SQLitePersist
from kon_tiki.rpc import RaftServer, PeerRPC


def applyCommand(command):
    print "I got %s" % command


def make_peers(peer_spec_list):
    peers = []
    for peer_spec in peer_spec_list:
        peer_spec = peer_spec.strip()
        components = peer_spec.split(':')
        if len(components) == 2:
            host, port = components
            peers.append(PeerRPC(host, int(port)))
        elif len(components) == 3:
            host, port, identity = components
            peers.append(PeerRPC(host, int(port), identity))
        else:
            raise ValueError("Could not parse spec %s" % peer_spec)
    return peers


def make_server(host, port, peers, path, identity=None):
    persister = SQLitePersist(path)
    persister.connect()

    for peer in peers:
        peer.factor = 1
        peer.connect()

    if identity is None:
        identity = '%s:%s' % (host, port)
    server = RaftServer(identity, peers, persister, applyCommand,
                        electionTimeoutRange=(1, 5))
    server.begin()
    reactor.listenTCP(port, pb.PBServerFactory(server))
    return server


if __name__ == '__main__':
    import argparse
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument('path')
    parser.add_argument('host')
    parser.add_argument('port', type=int)
    parser.add_argument('peers', nargs='+')
    parser.add_argument('--identity', default=None)
    args = parser.parse_args()

    peers = make_peers(args.peers)
    server = make_server(args.host, args.port, peers, args.path,
                         args.identity)
    log.startLogging(sys.stdout)
    reactor.run()

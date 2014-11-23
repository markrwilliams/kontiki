from collections import namedtuple
from twisted.spread import flavors

_LogEntry = namedtuple('LogEntry', 'term command')


class LogEntry(_LogEntry, flavors.Copyable):

    def getStateToCopy(self):
        return tuple(self)


class RemoteLogEntry(flavors.RemoteCopy):

    def unjellyFor(self, unjellier, jellyList):
        if unjellier.invoker is None:
            return flavors.setInstanceState(self, unjellier, jellyList)
        return LogEntry(*unjellier.unjelly(jellyList[1]))


def establishObjects():
    flavors.setUnjellyableForClass(LogEntry, RemoteLogEntry)

from twisted.internet import defer
from kon_tiki import persist
from twisted.trial import unittest


firstEntries = [persist.LogEntry(term=0, command='first1'),
                persist.LogEntry(term=0, command='first2'),
                persist.LogEntry(term=1, command='first3')]
appendedEntries = [persist.LogEntry(term=2, command='append1'),
                   persist.LogEntry(term=3, command='append2')]


class SQLitePersistTestCase(unittest.TestCase):

    def setUp(self):
        self.persister = persist.SQLitePersist(':memory:')
        self.persister.connect()

    def test_currentTerm(self):
        def assertCurrentTerm(deferred, term):
            deferred.addCallback(lambda ignore:
                                 self.persister.getCurrentTerm())
            deferred.addCallback(self.assertEqual, term)
            return deferred

        d = assertCurrentTerm(defer.succeed(None), 0)
        d.addCallback(lambda ignore:
                      self.persister.setCurrentTerm(None, increment=True))
        d.addCallback(self.assertEqual, second=1)

        d = assertCurrentTerm(d, 1)

        d.addCallback(lambda ignore:
                      self.persister.setCurrentTerm(123))
        d.addCallback(self.assertEqual, second=123)

        d = assertCurrentTerm(d, 123)

        return d

    def test_votedFor(self):
        d = self.persister.votedFor()
        d.addCallback(self.assertIs, second=None)

        d.addCallback(lambda ignore:
                      self.persister.voteFor("me"))
        d.addCallback(lambda ignore:
                      self.persister.votedFor())
        d.addCallback(self.assertEqual, second='me')
        return d

    def test_matchAndAppendNewLogEntries(self):
        d = self.persister.matchAndAppendNewLogEntries(-1, firstEntries)
        d.addCallback(lambda ignore:
                      self.persister.getLastIndex())
        d.addCallback(self.assertEqual, second=len(firstEntries) - 1)

        d.addCallback(lambda ignore:
                      self.persister.getLastIndex())
        d.addCallback(lambda lastIndex:
                      self.persister.committableLogEntries(0, lastIndex))
        d.addCallback(self.assertEqual, firstEntries[1:])

        d.addCallback(lambda ignore:
                      self.persister.getLastIndex())

        d.addCallback(
            lambda lastIndex:
            self.persister.matchAndAppendNewLogEntries(lastIndex,
                                                       appendedEntries))
        d.addCallback(
            lambda ignore:
            self.persister.getLastIndex())
        d.addCallback(self.assertEqual,
                      second=len(firstEntries) + len(appendedEntries) - 1)

        return d

    def test_overlaps_matchAndAppendNewLogEntries(self):
        # this needs to be generalized, the way it was for list persist
        d = self.persister.matchAndAppendNewLogEntries(-1, firstEntries)
        d.addCallback(
            lambda ignore:
            self.persister.matchAndAppendNewLogEntries(0, firstEntries))
        d.addCallback(
            lambda ignore:
            self.persister.getLastIndex())
        d.addCallback(self.assertEqual, len(firstEntries))
        return d

    def test_new_committableLogEntries(self):
        d = self.persister.committableLogEntries(0, 100)
        d.addCallback(self.assertEqual, second=[])
        return d

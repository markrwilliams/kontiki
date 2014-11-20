import itertools
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

    def test_getLastTerm(self):
        d = self.persister.getLastTerm()
        d.addCallback(self.assertEqual, 0)
        d.addCallback(
            lambda ignore:
            self.persister.matchAndAppendNewLogEntries(-1,
                                                       appendedEntries))

        d.addCallback(lambda ignore:
                      self.persister.getLastTerm())
        d.addCallback(self.assertEqual, 3)
        return d

    def test_addNewEntries(self):
        d = self.persister.addNewEntries(firstEntries)
        d.addCallback(lambda ignore:
                      self.persister.logSlice(0, -1))
        d.addCallback(self.assertEqual, firstEntries)

        d.addCallback(lambda ignore:
                      self.persister.addNewEntries(appendedEntries))
        d.addCallback(lambda ignore:
                      self.persister.logSlice(0, -1))
        d.addCallback(self.assertEqual, firstEntries + appendedEntries)

    def test_logSlice(self):
        d = self.persister.logSlice(0, 1000)
        d.addCallback(self.assertFalse)
        d.addCallback(lambda ignore:
                      self.persister.logSlice(0, -1))
        d.addCallback(self.assertFalse)

        d.addCallback(lambda ignore:
                      self.persister.addNewEntries(firstEntries))

        for gte, lt in itertools.combinations(range(len(firstEntries)), 2):
            d.addCallback(
                lambda ignore, lower=gte, upper=lt:
                self.persister.logSlice(lower, upper))
            d.addCallback(
                lambda result, lower=gte, upper=lt:
                self.assertEquals(result, firstEntries[lower:upper],
                                  msg='lower %d, upper %d' % (lower, upper)))

        d.addCallback(lambda ignore:
                      self.persister.logSlice(0, -1))
        d.addCallback(self.assertEquals, firstEntries)

        return d

    def test_indexMatchesTerm(self):
        d = self.persister.indexMatchesTerm(10, 123)
        d.addCallback(self.assertTrue)

        d.addCallback(lambda ignore:
                      self.persister.addNewEntries(firstEntries))

        for idx, entry in enumerate(firstEntries):
            d.addCallback(
                lambda ignore, idx=idx, entry=entry:
                self.persister.indexMatchesTerm(idx, entry.term))
            d.addCallback(self.assertTrue)

        return d

    def test_indexLETerm(self):
        d = self.persister.lastIndexLETerm(123)
        d.addCallback(self.assertTrue)

        for entry in firstEntries + appendedEntries:
            d.addCallback(lambda ignore, entry=entry:
                          self.persister.addNewEntries([entry]))
            d.addCallback(lambda ignore, entry=entry:
                          self.persister.lastIndexLETerm(entry.term))
            d.addCallback(self.assertTrue)

        return d

    def test_appendEntriesView(self):
        d = self.persister.appendEntriesView(123)
        emptyExpected = persist.AppendEntriesView(currentTerm=0,
                                                  lastLogIndex=-1,
                                                  prevLogTerm=None,
                                                  entries=[])
        d.addCallback(self.assertEqual, emptyExpected)

        d.addCallback(lambda ignore:
                      self.persister.addNewEntries(firstEntries))

        for prevLogIndex in xrange(len(firstEntries)):
            d.addCallback(lambda ignore, prevLogIndex=prevLogIndex:
                          self.persister.appendEntriesView(prevLogIndex))

            def verifyView(view, prevLogIndex=prevLogIndex):
                currentTerm = 0
                prevLogTerm = firstEntries[prevLogIndex]
                entries = firstEntries[prevLogIndex + 1:]
                lastLogIndex = len(firstEntries) + 1
                expected = persist.AppendEntriesView(currentTerm,
                                                     lastLogIndex,
                                                     prevLogIndex,
                                                     prevLogTerm,
                                                     entries)
                self.assertEqual(view, expected)

                d.addCallback(verifyView)

        return d

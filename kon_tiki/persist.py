import os
import operator
from collections import namedtuple
from twisted.python import failure
from twisted.internet.defer import Deferred
from twisted.enterprise import adbapi
import sqlite3


LogEntry = namedtuple('LogEntry', 'term command')


class PersistenceError(Exception):
    '''Raised when an error occurs in a Persist class'''


class MatchAfterTooHigh(PersistenceError):
    '''Raised when Persist.matchLogToEntries is given a prevIndex that's
    greater than the current size of the log.  Perhaps you didn't call
    Persist.indexMatchesTerm first?'''


class ListPersist(object):
    '''A Persist implementation that stores its state in-memory.  For
    testing only!
    '''

    def __init__(self, currentTerm=0, votedFor=None, log=None):
        # A real persister would restore the previous values, and
        # these would be properties that, when set, sync'd the values
        # to disk
        self.currentTerm = currentTerm
        self.votedFor = votedFor
        if not log:
            log = []
        self.log = log

    @property
    def lastIndex(self):
        return len(self.log) - 1

    def _compareIndexToTerm(self, index, term, op):
        if index == -1 and not self.log:
            return True
        return -1 < index < len(self.log) and op(self.log[index].term, term)

    def logSlice(self, start, end):
        return self.log[start:end]

    def indexMatchesTerm(self, index, term):
        return self._compareIndexToTerm(index, term, operator.eq)

    def lastIndexLETerm(self, term):
        return self._compareIndexToTerm(self.lastIndex, term, operator.le)

    def matchLogToEntries(self, matchAfter, entries):
        '''Delete existing log entries that conflict with `entries` and return
        only new entries.
        '''
        lastIndex = self.lastIndex
        if matchAfter > lastIndex:
            raise MatchAfterTooHigh('matchAfter = %d, '
                                    'lastIndex = %d' % (matchAfter,
                                                        self.lastIndex))
        elif matchAfter == lastIndex or not entries:
            # no entries can conflict, because entries begins
            # immediately after our last index or there are no entries
            return entries
        else:
            logIndex = matchAfter + 1
            for matchesUpTo, entry in enumerate(entries, 1):
                if logIndex >= len(self.log):
                    break
                elif self.log[logIndex].term != entry.term:
                    del self.log[logIndex:]
                    matchesUpTo -= 1
                    break
                else:
                    logIndex += 1
            return entries[matchesUpTo:]

    def appendNewEntries(self, entries):
        self.log.extend(entries)


def rowToLogEntry(row):
    return LogEntry(term=row['term'], command=row['command'])


class SQLitePersist(object):
    SCHEMA = '''CREATE TABLE IF NOT EXISTS raft_log
                             (logIndex INTEGER PRIMARY KEY,
                              term INTEGER,
                              command TEXT);
                CREATE TABLE IF NOT EXISTS raft_log_match
                             (logIndex INTEGER PRIMARY KEY,
                              term INTEGER,
                              command TEXT);
                CREATE INDEX IF NOT EXISTS raft_log_term_idx
                             ON raft_log(term);

                CREATE TABLE IF NOT EXISTS raft_variables
                             (currentTerm INTEGER,
                              votedFor    TEXT);'''
    _MISSING = object()
    dbPool = None
    _currentTerm = _MISSING
    _votedFor = _MISSING

    def __init__(self, path, poolMin=None, poolMax=None):
        self.path = path
        self.poolMin = poolMin
        self.poolMax = poolMax

    def prepareDatabaseAndConnection(self, connection):
        connection.row_factory = sqlite3.Row
        connection.executescript(self.SCHEMA)
        variablesPresent = '''SELECT COUNT(*)
                              FROM raft_variables'''
        cursor = connection.cursor()
        cursor.execute(variablesPresent)
        (number,) = cursor.fetchone()
        assert number < 2
        if number < 1:
            initializeRaftVariables = '''INSERT INTO raft_variables
                                         (currentTerm, votedFor)
                                         VALUES (0, null)'''
            cursor.execute(initializeRaftVariables)
            connection.commit()

    def connect(self):
        cf_openfun = self.prepareDatabaseAndConnection

        kwargs = {}
        if self.poolMin is not None:
            kwargs['cp_min'] = self.poolMin
        if self.poolMax is not None:
            kwargs['cp_max'] = self.poolMin

        self.dbPool = adbapi.ConnectionPool('sqlite3', self.path,
                                            check_same_thread=False,
                                            cp_openfun=cf_openfun,
                                            **kwargs)
    def getLastIndex(self):
        q = '''SELECT MAX(logIndex) AS lastIndex
               FROM raft_log'''

        def extractIndex(result):
            lastIndex = result[0]['lastIndex']
            if lastIndex is None:
                lastIndex = -1
            return lastIndex

        return self.dbPool.runQuery(q).addCallback(extractIndex)

    def logSlice(self, start, end):
        q = '''SELECT term, command
               FROM raft_log
               WHERE logIndex >= :start AND logIndex < :end'''

        def convertRows(result):
            return [rowToLog(row) for row in result]

        d = self.dbPool.runQuery(q, {'start': start, 'end': end})
        d.addCallback(convertRows)
        return d

    def indexMatchesTerm(self, index, term):
        q = '''SELECT term = :term AS termMatches
               FROM raft_log
               WHERE logIndex = :index'''

        def boolify(result):
            return bool(result[0]['termMatches'])

        d = self.dbPool.runQuery(q, {'index': index, 'term': term})
        d.addCallback(boolify)
        return d

    def lastIndexLETerm(self, term):
        q = '''SELECT term <= :term AS termOK
               FROM raft_log
               WHERE logIndex = (SELECT MAX(logIndex)
                              FROM raft_log)'''

        def boolify(result):
            return bool(result[0]['termOK'])

        d = self.dbPool.runQuery(q, {'term': term})
        d.addCallback(boolify)
        return d

    def getCurrentTerm(self):
        if self._currentTerm is not self._MISSING:
            d = Deferred()
            d.callback(self._currentTerm)
            return d
        else:
            q = '''SELECT currentTerm
                   FROM raft_variables
                   WHERE rowid = 1'''

            def extractCurrentTerm(result):
                return result[0]['currentTerm']

            def setCurrentTerm(currentTerm):
                self._currentTerm = currentTerm
                return currentTerm

            d = self.dbPool.runQuery(q)
            d.addCallback(extractCurrentTerm)
            d.addCallback(setCurrentTerm)
            return d

    def incrementTerm(self):
        def updateReturning(txn):
            update = '''UPDATE raft_variables
                        SET currentTerm = currentTerm + 1'''
            txn.execute(update)
            select = '''SELECT currentTerm
                        FROM raft_variables'''
            txn.execute(select)
            return txn.fetchone()['currentTerm']

        def setCurrentTerm(currentTerm):
            self._currentTerm = currentTerm
            return currentTerm

        d = self.dbPool.runInteraction(updateReturning)
        d.addCallback(setCurrentTerm)
        return d

    def votedFor(self):
        if self._votedFor is not self._MISSING:
            d = Deferred()
            d.callback(self._votedFor)
            return d
        q = '''SELECT votedFor
               FROM raft_variables
               WHERE rowid = 1'''

        def extractVotedFor(result):
            return result[0]['votedFor']

        return self.dbPool.runQuery(q).addCallback(extractVotedFor)

    def voteFor(self, identity):
        def updateReturning(txn):
            update = '''UPDATE raft_variables
                        SET votedFor = :identity'''
            updateParams = {'identity': identity}
            txn.execute(update, updateParams)
            select = '''SELECT votedFor
                        FROM raft_variables'''
            txn.execute(select)
            return txn.fetchone()['votedFor']

        def setVotedFor(votedFor):
            self._votedFor = votedFor
            if self._votedFor is None:
                raise ValueError("HERE")
            return votedFor

        d = self.dbPool.runInteraction(updateReturning)
        d.addCallback(setVotedFor)
        return d

    def matchLogEntries(self, matchAfter, entries):

        def match(txn):
            lastIndex = txn.execute('SELECT MAX(logIndex) FROM raft_log')
            if matchAfter > lastIndex:
                raise MatchAfterTooHigh('matchAfter = %d, '
                                        'lastIndex = %d' % (matchAfter,
                                                            lastIndex))
            elif matchAfter == lastIndex or not entries:
                return entries
            truncateMatchTable = "DELETE FROM raft_log_match"
            txn.execute(truncateMatchTable)

            loadMatchTable = '''INSERT INTO raft_log_match
                                (logIndex, term, command)
                                VALUES (:index, :term, :command)'''
            entriesStartIndex = matchAfter + 1
            loadMatchTableParams = ({'index': i,
                                     'term': entry.term,
                                     'command': entry.command}
                                    for i, entry
                                    in enumerate(entries, entriesStartIndex))

            txn.executemany(loadMatchTable, loadMatchTableParams)

            matchEntries = '''SELECT MAX(raft_log.logIndex) AS myIndex
                              FROM raft_log
                              JOIN raft_log_match
                              ON (raft_log.logIndex = raft_log_match.logIndex
                                  AND raft_log.term = raft_log_match.term)'''

            txn.execute(matchEntries)
            result = txn.fetchone()
            myIndex = result['myIndex']
            if myIndex is None:
                myIndex = matchAfter

            entriesStartIndex -= myIndex

            deleteMismatch = '''DELETE FROM raft_log
                                WHERE logIndex > :myIndex'''

            txn.execute(deleteMismatch, {'myIndex': myIndex})
            return entries[newIndex:]

        return self.dbPool.runInteraction(match)

    def appendNewEntries(self, entries):

        def append(txn):
            insert = '''INSERT INTO raft_log
                        (logIndex, term, command)
                        VALUES (null, :term, :command)'''
            insertParams = ({'term': entry.term,
                             'command': entry.command}
                            for entry in entries)

            txn.executemany(insert, insertParams)
            return len(entries)

        return self.dbPool.runInteraction(append)

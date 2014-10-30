from heapq import heapify, heappop


def nlargest(n, l):
    if not l:
        return l
    leastL = [-el for el in l]
    heapify(leastL)
    return [-heappop(leastL) for _ in xrange(min(n, len(l)))]


def median(l):
    if not l:
        raise ValueError("no median for empty data")
    return nlargest(len(l) / 2 + 1, l)[-1]

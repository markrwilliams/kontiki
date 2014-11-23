from kontiki import fundamentals
from twisted.trial import unittest


class FundamentalsTestCase(unittest.TestCase):

    def test_nlargest(self):
        self.assertEqual(fundamentals.nlargest(0, []), [])
        self.assertEqual(fundamentals.nlargest(9999, []), [])

        listOf5 = [1, 2, 3, 4, 5]
        self.assertEqual(fundamentals.nlargest(5, listOf5), listOf5[::-1])
        self.assertEqual(fundamentals.nlargest(9999, listOf5), listOf5[::-1])
        self.assertEqual(fundamentals.nlargest(0, listOf5), [])

        listOf10 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        self.assertEqual(fundamentals.nlargest(3, listOf10), [10, 9, 8])

    def test_majorityMedian(self):
        self.assertRaises(ValueError, fundamentals.majorityMedian, [])

        self.assertEqual(fundamentals.majorityMedian([1]), 1)
        self.assertEqual(fundamentals.majorityMedian([1, 2]), 1)
        self.assertEqual(fundamentals.majorityMedian([1, 2, 3]), 2)
        self.assertEqual(fundamentals.majorityMedian([4, 4, 4]), 4)
        self.assertEqual(fundamentals.majorityMedian([1, 2, 3, 4, 6, 7]), 3)
        self.assertEqual(fundamentals.majorityMedian([1, 2, 3, 6, 7, 8, 9]), 6)

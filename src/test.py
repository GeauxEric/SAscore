import unittest
from fp import getHexFp, convert2Bits


class TestFingerPrint(unittest.TestCase):
    def test_fp(self):
        ifn = "../data/sample.smi"
        bits = convert2Bits(getHexFp(ifn))
        self.assertEqual(len(bits), 1024)


if __name__ == '__main__':
    unittest.main()

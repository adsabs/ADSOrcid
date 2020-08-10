#!/usr/bin/env python
# -*- coding: utf-8 -*-


import unittest
from ADSOrcid import names 

class Test(unittest.TestCase):
    def test_build_short_forms(self):
        """Get name variants"""
        self.assertEqual(names.build_short_forms('porceddu,'), [])
        self.assertEqual(names.build_short_forms('porceddu, i'), [])
        self.assertEqual(sorted(names.build_short_forms('porceddu, i. enrico pietro')),
                         sorted(['porceddu, i enrico p', 'porceddu, i e pietro', 'porceddu, i e', 'porceddu, i', 'porceddu, i e p']))
        self.assertEqual(sorted(names.build_short_forms('porceddu, ignazio enrico pietro')),
                         sorted(['porceddu, ignazio enrico p', 'porceddu, i e', 'porceddu, i enrico pietro', 'porceddu, i', 'porceddu, ignazio e pietro', 'porceddu, i e p']))
        
        
if __name__ == '__main__':
    unittest.main()         
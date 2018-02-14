#!/usr/bin/env python
# -*- coding: utf-8 -*-


import unittest
from mock import patch
from ADSOrcid import updater, names

class Test(unittest.TestCase):
    
    
    def test_update_record(self):
        """
        Update ADS document with the claim info

        :return: no return
        """
        doc = {
            'bibcode': '2015ApJ...799..123B', 
            'authors': [
              "Barrière, Nicolas M.",
              "Krivonos, Roman",
              "Tomsick, John A.",
              "Bachetti, Matteo",
              "Boggs, Steven E.",
              "Chakrabarty, Deepto",
              "Christensen, Finn E.",
              "Craig, William W.",
              "Hailey, Charles J.",
              "Harrison, Fiona A.",
              "Hong, Jaesub",
              "Mori, Kaya",
              "Stern, Daniel",
              "Yıldız, Umut"
            ],
            'claims': {}
        }
        r = updater.update_record(
          doc,
          {
           'bibcode': '2015ApJ...799..123B', 
           'orcidid': '0000-0003-2686-9241',
           'account_id': '1',
           'orcid_name': [u'Stern, Daniel'],
           'author': [u'Stern, D', u'Stern, D K', u'Stern, Daniel'],
           'author_norm': [u'Stern, D'],
           'name': u'Stern, D K' 
          },
          0.9                     
        )
        self.assertEqual(r, ('verified', 12))
        self.assertEqual(doc['claims']['verified'], 
            ['-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '0000-0003-2686-9241', '-'])
        
        updater.update_record(
          doc,
          {
           'bibcode': '2015ApJ...799..123B', 
           'orcidid': '0000-0003-2686-9241',
           'account_id': '1',
           'orcid_name': [u'Stern, Daniel'],
           'author': [u'Stern, D', u'Stern, D K', u'Stern, Daniel'],
           'author_norm': [u'Stern, D'],
           'name': u'Stern, D K',
           'status': 'removed'
          },
          0.9                        
        )
        self.assertEqual(doc['claims']['verified'], 
            ['-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-'])
        
        # the size differs
        doc['claims']['verified'] = ['-']
        r = updater.update_record(
          doc,
          {
           'bibcode': '2015ApJ...799..123B', 
           'orcidid': '0000-0003-2686-9241',
           'account_id': '1',
           'orcid_name': [u'Stern, Daniel'],
           'author': [u'Stern, D', u'Stern, D K', u'Stern, Daniel'],
           'author_norm': [u'Stern, D'],
           'name': u'Stern, D K' 
          },
          0.9                       
        )
        self.assertEqual(r, ('verified', 12))
        self.assertEqual(doc['claims']['verified'], 
            ['-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '0000-0003-2686-9241', '-'])
        
        self.assertEqual(14, len(doc['claims']['verified']))

        # check transliterated name version
        r = updater.update_record(
            doc,
            {
                'bibcode': '2015ApJ...799..123B',
                'orcidid': '0000-0001-2345-6789',
                'account_id': '2',
                'orcid_name': [u'Yildiz, Umut'],
                'author': [u'Yildiz, U', u'Yildiz, Umut'],
                'author_norm': [u'Yildiz, U'],
                'name': u'Yildiz, Umut'
            },
            0.9
        )
        self.assertEqual(r, ('verified', 13))
        self.assertEqual(doc['claims']['verified'],
                         ['-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '0000-0003-2686-9241', '0000-0001-2345-6789'])

    def test_exact_match(self):
        """
        Given an author with an exact name match, the Levenshtein matching function should not be called.

        :return: None
        """

        with patch.object(updater,'find_orcid_position') as next_task:

            self.assertFalse(next_task.called)
            doc1 = {
                'bibcode': "2001RadR..155..543L",
                "authors": [
                    "Li, Zhongkui",
                    "Xia, Liqun",
                    "Lee, Leo M.",
                    "Khaletskiy, Alexander",
                    "Wang, J.",
                    "Wong, J. Y.",
                    "Li, Jian-Jian"
                ],
                'claims': {}
            }
            orcid_name = u'Wong, Jeffrey Yang'
            r = updater.update_record(
                doc1,
                {
                    'bibcode': '2001RadR..155..543L',
                    'orcidid': '0000-0003-2686-9241',
                    'account_id': '1',
                    'orcid_name': [orcid_name],
                    'author': [u'Wong, J Y'],
                    'author_norm': [u'Wong, J'],
                    'name': u'Wong, J Y',
                    'short_name': names.build_short_forms(orcid_name)
                },
                0.8
            )
            self.assertEqual(r, ('verified', 5))
            self.assertEqual(doc1['claims']['verified'],
                             ['-', '-', '-', '-', '-', '0000-0003-2686-9241', '-'])
            # find_orcid_position should be bypassed by the exact string match
            self.assertFalse(next_task.called)

    def test_find_author_position(self):
        """
        Given the ORCID ID, and information about author name, 
        we have to identify the position of the author from
        the list of supplied names

        :return: no return
        """
        res = updater.find_orcid_position([
              "Barrière, Nicolas M.",
              "Krivonos, Roman",
              "Tomsick, John A.",
              "Bachetti, Matteo",
              "Boggs, Steven E.",
              "Chakrabarty, Deepto",
              "Christensen, Finn E.",
              "Craig, William W.",
              "Hailey, Charles J.",
              "Harrison, Fiona A.",
              "Hong, Jaesub",
              "Mori, Kaya",
              "Stern, Daniel",
              "Zhang, William W."
            ],
          ['Stern, D.', 'Stern, Daniel']                          
        )
        self.assertEqual(res, 12)
        
        # check that the author cannot claim what doesn't look like their 
        # own paper
        
        res = updater.find_orcid_position([
               "Erdmann, Christopher",
               "Frey, Katie"
               ], 
              ["Accomazzi, Alberto"]);
        self.assertEqual(res, -1)
        
        # check boundaries
        res = updater.find_orcid_position([
               "Erdmann, Christopher",
               "Frey, Katie"
               ], 
              ["Erdmann, C"]);
        self.assertEqual(res, 0)
        res = updater.find_orcid_position([
               "Erdmann, Christopher",
               "Cote, Ann",
               "Frey, Katie"
               ], 
              ["Frey, Katie"]);
        self.assertEqual(res, 2)

        res = updater.find_orcid_position([
            u"Goldsmith, P. F.",
            u"Yıldız, U. A.",
            u"Langer, W. D.",
            u"Pineda, J. L."
        ],
            ["Yildiz, U. A."]
        )
        self.assertEqual(res,1)
            
if __name__ == '__main__':
    unittest.main()            
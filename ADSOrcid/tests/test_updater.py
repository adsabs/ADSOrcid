#!/usr/bin/env python
# -*- coding: utf-8 -*-


import unittest
import os
from mock import patch
from ADSOrcid import updater, names, app
from ADSOrcid.models import Base

class Test(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        proj_home = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        self.app = app.ADSOrcidCelery('test', local_config= \
            {
                'SQLALCHEMY_URL': 'sqlite:///',
                'SQLALCHEMY_ECHO': False,
                'PROJ_HOME': proj_home,
                'TEST_DIR': os.path.join(proj_home, 'ADSOrcid/tests'),
            })
        Base.metadata.bind = self.app._session.get_bind()
        Base.metadata.create_all()

    def tearDown(self):
        unittest.TestCase.tearDown(self)
        Base.metadata.drop_all()
        self.app.close_app()
    
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

        doc_lev = {
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
              "Zhang, William W."
            ],
            'claims': {}
        }
        r_lev = updater.update_record(
            doc_lev,
            {
                'bibcode': '2015ApJ...799..123B',
                'orcidid': '0000-0001-2345-6789',
                'account_id': '1',
                'orcid_name': [u'Zhang, Will'],
                'author': [u'Zhang, Will'],
                'author_norm': [u'Zhang, Will'],
                'name': u'Zhang, Will'
            },
            0.75
        )
        self.assertEqual(r_lev, ('verified', 13))
        self.assertEqual(doc_lev['claims']['verified'],
                         ['-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '-', '0000-0001-2345-6789'])

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

    def test_reindex_all_claims(self):
        """
        Given an ORCID ID and a starting date, we should be able to reindex all claims after that date.

        :return: None
        """
        with patch.object(self.app, 'retrieve_orcid') as retrieve_orcid:

            retrieve_orcid.return_value = {'status': None,
                                           'name': u'Payne, Cecilia',
                                           'facts': {u'author': [u'Payne, Cecilia'],
                                                     u'orcid_name': [u'Payne, Cecilia'],
                                                     u'name': u'Payne, Cecilia'},
                                           'orcidid': u'0000-0001-0002-0003',
                                           'id': 1,
                                           'account_id': None,
                                           'updated': '2017-01-01T12:00:00.000000Z'
                                           }

            cdate = '2018-01-01T12:00:00.000000Z'
            sdate = '2018-01-01T01:00:00.000000Z'

            self.app.insert_claims([self.app.create_claim(bibcode='2018Test....123...A',
                                                          orcidid='0000-0001-0002-0003',
                                                          provenance='Test',
                                                          status='claimed',
                                                          date=cdate
                                                          ),
                                    self.app.create_claim(bibcode='2018Test....123...B',
                                                          orcidid='0000-0001-0002-0003',
                                                          provenance='Test',
                                                          status='removed',
                                                          date=cdate
                                                          )])

            self.app.record_claims('2018Test....123...A',
                                   {'verified': ['0000-0001-0002-0003', '-', '-'],
                                    'unverified': ['-', '-', '-']},
                                    authors = [u'Payne, Cecilia', u'Doe, Jane', u'Doe, John'])
            self.app.record_claims('2018Test....123...B',
                                   {'verified': ['0000-0001-0002-0003', '-', '-'],
                                    'unverified': ['-', '-', '-']})

            recs = updater.reindex_all_claims(self.app, orcidid='0000-0001-0002-0003', since=sdate)

            self.assertEqual(len(recs),2)
            self.assertEqual(sorted(recs)[0],'2018Test....123...A')
            self.assertEqual(sorted(recs)[1],'2018Test....123...B')


if __name__ == '__main__':
    unittest.main()            
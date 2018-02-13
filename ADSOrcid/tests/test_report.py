#!/usr/bin/env python
# -*- coding: utf-8 -*-


import unittest
import os
import json
import pdb
from mock import patch, PropertyMock
from ADSOrcid import report, app
from ADSOrcid.models import Base, ClaimsLog

logdir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../logs'))

class TestReport(unittest.TestCase):

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

    def test_claimed_records(self):
        claimed_log = logdir + '/test_claimed.log'
        if os.path.exists(claimed_log):
            os.remove(claimed_log)

        json_1 = {u'response': {u'docs': [{u'bibcode': u'2018Test..001..123A'},
                                          {u'bibcode': u'2018Test..001..123B'}],
                                u'numFound': 2,
                                u'start': 0}
                  }

        json_2 = [{u'bibcode': u'2018Test..001..123A',
                   u'orcid_pub': [u'0000-0001-0002-0003',
                                  u'-']
                  },
                  {u'bibcode': u'2018Test..001..123B',
                   u'orcid_user': [u'0000-0001-0002-0003',
                                   u'0000-0001-0002-0004',
                                   u'-']
                   },
                  {u'bibcode': u'2018Test..001..123C',
                   u'orcid_other': [u'0000-0001-0002-0003',
                                    u'0000-0001-0002-0004',
                                    u'0000-0001-0002-0005',
                                    u'-']
                   }
                  ]

        with patch.object(report, 'query_solr', return_value=json_1) as query_solr, \
             patch.object(report, 'query_records', return_value=json_2) as query_records:

            report.claimed_records(debug=False,test=True)

            log = open(logdir + '/test_claimed.log').read()
            line = 0
            for x in log.strip().split('\n'):
                j = json.loads(x)
                if line == 0:
                    self.assertEqual(j['message'],u'Number of records with an orcid_pub: 2')
                if line == 1:
                    self.assertEqual(j['message'],u'Number of records with an orcid_user: 2')
                if line == 2:
                    self.assertEqual(j['message'],u'Number of records with an orcid_other: 2')
                if line == 3:
                    self.assertEqual(j['message'],u'Total number of orcid_pub claims: 1')
                if line == 4:
                    self.assertEqual(j['message'],u'Total number of orcid_user claims: 2')
                if line == 5:
                    self.assertEqual(j['message'],u'Total number of orcid_other claims: 3')
                if line == 6:
                    self.assertEqual(j['message'],u'Total number of records with any ORCID claims: 3')
                line += 1

    def test_num_claims(self):
        num_claims_log = logdir + '/test_num_claimed.log'
        if os.path.exists(num_claims_log):
            os.remove(num_claims_log)

        r = self.app.insert_claims([
            self.app.create_claim(bibcode='2018Test..001..123D',
                                  orcidid='0000-0001-0002-0006',
                                  provenance='ads test',
                                  status='claimed'),
            self.app.create_claim(bibcode='2018Test..001..123E',
                                  orcidid='0000-0001-0002-0007',
                                  status='updated'),
            self.app.create_claim(bibcode='2018Test..001..123F',
                                  orcidid='0000-0001-0002-0008',
                                  status='removed')
            ])

        self.assertEquals(len(r), 3)
        self.assertTrue(len(self.app._session.query(ClaimsLog)
                            .filter_by(bibcode='2018Test..001..123D').all()) == 1)

        report.num_claims(self.app,n_days=1,test=True)

        log = open(logdir + '/test_num_claimed.log').read()
        line = 0
        for x in log.strip().split('\n'):
            j = json.loads(x)
            if line == 0:
                self.assertEqual(j['message'], u'Number of unique ORCID IDs generating claims of type claimed in last 1 days: 1')
            if line == 1:
                self.assertEqual(j['message'], u'Number of unique ORCID IDs generating claims of type removed in last 1 days: 1')
            if line == 2:
                self.assertEqual(j['message'], u'Number of unique ORCID IDs generating claims of type updated in last 1 days: 1')
            if line == 3:
                self.assertEqual(j['message'], u'Number of unique claims by a unique bibcode+ORCID ID pair that have been claimed in the last 1 days: 1')
            if line == 4:
                self.assertEqual(j['message'], u'Number of unique claims by a unique bibcode+ORCID ID pair that have been removed in the last 1 days: 1')
            if line == 5:
                self.assertEqual(j['message'], u'Number of unique claims by a unique bibcode+ORCID ID pair that have been updated in the last 1 days: 1')
            if line == 6:
                self.assertEqual(j['message'], u"Total number of non-unique claims with status ['claimed', 'removed', 'updated'] in the last 1 days, to compare with logging on rejected claims: 3")
            line += 1

    def test_Kibana(self):
        kibana_log = logdir + '/test_kibana.log'
        if os.path.exists(kibana_log):
            os.remove(kibana_log)

        with patch.object(report.requests,'post') as post:
            data = {u'responses': [{u'hits': {u'total': 5},
                                    u'status': 200}]}
            resp = PropertyMock()
            resp.text = str(data)
            resp.json = lambda: data
            resp.status_code = 200
            post.return_value = resp

            report.num_refused_claims(n_days=1,test=True)
            report.num_missing_profile(n_days=1,test=True)

            log = open(logdir + '/test_kibana.log').read()
            line = 0
            for x in log.strip().split('\n'):
                j = json.loads(x)
                if line == 0:
                    self.assertEqual(j['message'], u'Number of claims rejected in the last 1 days: 5')
                if line == 1:
                    self.assertEqual(j['message'], u'Number of missing profile errors in the last 1 days: 5')
                line += 1

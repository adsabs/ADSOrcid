#!/usr/bin/env python
"""
"""

__author__ = 'rca'
__maintainer__ = 'rca'
__copyright__ = 'Copyright 2015'
__version__ = '1.0'
__email__ = 'ads@cfa.harvard.edu'
__status__ = 'Production'
__credit__ = ['J. Elliott']
__license__ = 'MIT'

import os
import sys
import time
import json
import argparse
import logging
import traceback
import warnings
from urllib3 import exceptions
warnings.simplefilter('ignore', exceptions.InsecurePlatformWarning)

from adsputils import get_date
from adsmsg import OrcidClaims
from ADSOrcid import updater, tasks
from ADSOrcid.models import ClaimsLog, KeyValue, Records, AuthorInfo

# ============================= INITIALIZATION ==================================== #

from adsputils import setup_logging, load_config
proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)
logger = setup_logging('run.py', proj_home=proj_home,
                        level=config.get('LOGGING_LEVEL', 'INFO'),
                        attach_stdout=config.get('LOG_STDOUT', False))

app = tasks.app

# =============================== FUNCTIONS ======================================= #


def reindex_claims(since=None, orcid_ids=None, **kwargs):
    """
    Re-runs all claims, both from the pipeline and
    from the orcid-service storage.

    :param: since - RFC889 formatted string
    :type: str

    :return: no return
    """
    if orcid_ids:
        for oid in orcid_ids:
            tasks.task_index_orcid_profile.delay({'orcidid': oid, 'force': True})
        if not since:
            print('Done (just the supplied orcidids)')
            return

    logging.captureWarnings(True)
    if not since or isinstance(since, str) and since.strip() == "":
        with app.session_scope() as session:
            kv = session.query(KeyValue).filter_by(key='last.reindex').first()
            if kv is not None:
                since = kv.value
            else:
                since = '1974-11-09T22:56:52.518001Z'

    from_date = get_date(since)
    orcidids = set()


    logger.info('Loading records since: {0}'.format(from_date.isoformat()))

    # first re-check our own database (replay the logs)
    with app.session_scope() as session:
        for author in session.query(AuthorInfo.orcidid.distinct().label('orcidid')).all():
            orcidid = author.orcidid
            if orcidid and orcidid.strip() != "":
                try:
                    changed = updater.reindex_all_claims(app, orcidid, since=from_date.isoformat(), ignore_errors=True)
                    if len(changed):
                        orcidids.add(orcidid)
                    tasks.task_index_orcid_profile.delay({'orcidid': orcidid, 'force': True})
                except Exception as e:
                    print('Error processing: {0}'.format(orcidid))
                    traceback.print_exc()
                    continue
                if len(orcidids) % 100 == 0:
                    print('Done replaying {0} profiles'.format(len(orcidids)))

    print('Now harvesting orcid profiles...')

    # then get all new/old orcidids from orcid-service
    all_orcids = set(updater.get_all_touched_profiles(app, from_date.isoformat()))
    orcidids = all_orcids.difference(orcidids)
    from_date = get_date()


    for orcidid in orcidids:
        try:
            tasks.task_index_orcid_profile.delay({'orcidid': orcidid, 'force': True})
        except: # potential backpressure (we are too fast)
            time.sleep(2)
            print('Conn problem, retrying...', orcidid)
            tasks.task_index_orcid_profile.delay({'orcidid': orcidid, 'force': True})

    with app.session_scope() as session:
        kv = session.query(KeyValue).filter_by(key='last.reindex').first()
        if kv is None:
            kv = KeyValue(key='last.reindex', value=from_date.isoformat())
            session.add(kv)
        else:
            kv.value = from_date.isoformat()
        session.commit()

    print('Done')
    logger.info('Done submitting {0} orcid ids.'.format(len(orcidids)))


def repush_claims(since=None, orcid_ids=None, **kwargs):
    """
    Re-pushes all recs that were added since date 'X'
    to the output (i.e. forwards them onto the Solr queue)

    :param: since - RFC889 formatted string
    :type: str

    :return: no return
    """
    if orcid_ids:
        for oid in orcid_ids:
            tasks.task_index_orcid_profile.delay({'orcidid': oid, 'force': False})
        if not since:
            print('Done (just the supplied orcidids)')
            return

    logging.captureWarnings(True)
    if not since or isinstance(since, str) and since.strip() == "":
        with app.session_scope() as session:
            kv = session.query(KeyValue).filter_by(key='last.repush').first()
            if kv is not None:
                since = kv.value
            else:
                since = '1974-11-09T22:56:52.518001Z'

    from_date = get_date(since)

    logger.info('Re-pushing records since: {0}'.format(from_date.isoformat()))


    num_bibcodes = 0
    with app.session_scope() as session:
        for rec in session.query(Records) \
            .filter(Records.updated >= from_date) \
            .order_by(Records.updated.asc()) \
            .all():

            data = rec.toJSON()
            try:
                tasks.task_output_results.delay({'bibcode': data['bibcode'], 'authors': data['authors'], 'claims': data['claims']})
            except Exception as e: # potential backpressure (we are too fast)
                time.sleep(2)
                print('Conn problem, retrying ', data['bibcode'])
                tasks.task_output_results.delay({'bibcode': data['bibcode'], 'authors': data['authors'], 'claims': data['claims']})
            num_bibcodes += 1

    with app.session_scope() as session:
        kv = session.query(KeyValue).filter_by(key='last.repush').first()
        if kv is None:
            kv = KeyValue(key='last.repush', value=get_date())
            session.add(kv)
        else:
            kv.value = get_date()
        session.commit()

    logger.info('Done processing {0} orcid ids.'.format(num_bibcodes))



def refetch_orcidids(since=None, orcid_ids=None, **kwargs):
    """
    Gets all orcidids that were updated since time X.

    :param: since - RFC889 formatted string
    :type: str

    :return: no return
    """
    if orcid_ids:
        for oid in orcid_ids:
            tasks.task_index_orcid_profile({'orcidid': oid, 'force': False})
        if not since:
            print('Done (just the supplied orcidids)')
            return


    logging.captureWarnings(True)
    if not since or isinstance(since, str) and since.strip() == "":
        with app.session_scope() as session:
            kv = session.query(KeyValue).filter_by(key='last.refetch').first()
            if kv is not None:
                since = kv.value
            else:
                since = '1974-11-09T22:56:52.518001Z'

    from_date = get_date(since)
    logger.info('Re-fetching orcidids updated since: {0}'.format(from_date.isoformat()))


    # then get all new/old orcidids from orcid-service
    orcidids = set(updater.get_all_touched_profiles(app, from_date.isoformat()))
    from_date = get_date()


    for orcidid in orcidids:
        try:
            tasks.task_index_orcid_profile.delay({'orcidid': orcidid, 'force': False})
        except Exception as e: # potential backpressure (we are too fast)
            time.sleep(2)
            print('Conn problem, retrying...', orcidid)
            tasks.task_index_orcid_profile.delay({'orcidid': orcidid, 'force': False})

    with app.session_scope() as session:
        kv = session.query(KeyValue).filter_by(key='last.refetch').first()
        if kv is None:
            kv = KeyValue(key='last.refetch', value=from_date.isoformat())
            session.add(kv)
        else:
            kv.value = from_date.isoformat()
        session.commit()

    print('Done')
    logger.info('Done submitting {0} orcid ids.'.format(len(orcidids)))


def reprocess_bibcodes(bibcodes, force=False):
    """
    Checks that the stored length of the claims matches the length of the authors.
    Reprocesses the relevant ORCID IDs if the lengths do not match.

    :param bibcodes: Bibcodes to check (list)
    :param force: Force the length of the ORCID array to be rebuilt (boolean)
    :return: no return
    """

    orcids_to_process = set()
    logging.captureWarnings(True)

    if (type(bibcodes) != list):
        if type(bibcodes) == str:
            bibcodes = [bibcodes]
    elif (len(bibcodes) == 1) and bibcodes[0].startswith('@'):
        tmp_bibcodes = []
        with open(bibcodes[0].replace('@', '')) as fp:
            for line in fp:
                if line.startswith('#'):
                    continue
                b = line.strip()
                tmp_bibcodes.append(b)
        bibcodes = tmp_bibcodes
    else:
        raise TypeError('Bibcodes must be list, string, or filename starting with @')

    for bibcode in bibcodes:
        logger.debug('Reprocessing bibcode {}'.format(bibcode))
        update = False

        metadata = app.retrieve_metadata(bibcode, search_identifiers=True)
        # make sure we're using the canonical bibcode
        bibc = metadata.get('bibcode')
        author_list = metadata.get('author', [])

        # fetch existing record; creates one if necessary
        rec = app.retrieve_record(bibc, author_list)
        claims = rec.get('claims', {})
        # reprocess any ORCID IDs that are in arrays of the wrong length
        fld_names = ['verified', 'unverified']
        for f in fld_names:
            f_claims = claims.get(f, [])
            if f_claims and len(author_list) != f_claims:
                logger.debug('{0} claims length does not match author length for bibcode {1}, reprocessing'.
                             format(f, bibcode))
                orcids = set(f_claims)
                orcids -= {'-'}
                orcids_to_process = orcids_to_process.union(orcids)
                if force or not orcids:
                    # force is on or length is non-zero but wrong but there are no ORCIDs in it to reprocess,
                    # so rebuild manually
                    # note that this is fine to do even if there are valid orcid claims here because those will get
                    # rebuilt in the reindex task at the end of this process
                    claims[f] = ['-'] * len(author_list)
                    update = True

        if update:
            with app.session_scope() as session:
                r = session.query(Records).filter_by(bibcode=bibc).first()
                r.claims = json.dumps(claims)
                r.updated = get_date()

                session.commit()

            # if there are no claims, we need to push the update to master manually
            msg = OrcidClaims(authors=rec.get('authors'), bibcode=rec['bibcode'],
                              verified=claims.get('verified', []),
                              unverified=claims.get('unverified', [])
                              )
            tasks.task_output_results.delay(msg)

    for orcidid in orcids_to_process:
        try:
            tasks.task_index_orcid_profile.delay({'orcidid': orcidid, 'force': True})
        except Exception as e: # potential backpressure (we are too fast)
            time.sleep(2)
            logger.info('Connection problem when trying to process {}, retrying...'.format(orcidid))
            tasks.task_index_orcid_profile.delay({'orcidid': orcidid, 'force': True})

    logger.info('Done processing the given bibcodes')


def print_kvs():
    """Prints the values stored in the KeyValue table."""
    print('Key, Value from the storage:')
    print('-' * 80)
    with app.session_scope() as session:
        for kv in session.query(KeyValue).order_by('key').all():
            print(kv.key, kv.value)

def show_api_diagnostics(orcid_ids=None, bibcodes=None, ):
    """
    Prints various responses that we receive from our API.
    """

    print('API_ENDPOINT', app.conf.get('API_ENDPOINT', None))
    print('API_SOLR_QUERY_ENDPOINT', app.conf.get('API_SOLR_QUERY_ENDPOINT', None))
    print('API_ORCID_EXPORT_PROFILE', app.conf.get('API_ORCID_EXPORT_PROFILE', None))
    print('API_ORCID_UPDATES_ENDPOINT', app.conf.get('API_ORCID_UPDATES_ENDPOINT', None))


    if orcid_ids:
        for o in orcid_ids:
            print(o)
            print('DB Model', app.retrieve_orcid(o))
            print('=' * 80 + '\n')
            print('Author info', app.harvest_author_info(o))
            print('=' * 80 + '\n')
            print('Public orcid profile', app.get_public_orcid_profile(o))
            print('=' * 80 + '\n')
            print('ADS Orcid Profile', app.get_ads_orcid_profile(o))
            print('=' * 80 + '\n')
            print('Harvested Author Info', app.retrieve_orcid(o))
            print('=' * 80 + '\n')
            orcid_present, updated, removed = app.get_claims(o,
                         app.conf.get('API_TOKEN'),
                         app.conf.get('API_ORCID_EXPORT_PROFILE') % o,
                         orcid_identifiers_order=app.conf.get('ORCID_IDENTIFIERS_ORDER', {'bibcode': 9, '*': -1})
                         )
            print('All of orcid', len(orcid_present), orcid_present)
            print('In need of update', len(updated), updated)
            print('In need of removal', len(removed), removed)
            print('=' * 80 + '\n')
    else:
        print('If you want to see what I see for authors, give me some orcid ids')

    if bibcodes:
        for b in bibcodes:
            print(b, app.retrieve_metadata(b))
    else:
        print('If you want to see what I see give me some bibcodes')


    if orcid_ids:
        print('=' * 80 + '\n')
        print('Now submitting ORCiD for processing')
        for o in orcid_ids:
            m = {'orcidid': o}
            print('message=%s, taskid=%s' % (m, tasks.task_index_orcid_profile.delay(m)))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Process user input.')

    parser.add_argument('-r',
                        '--reindex_claims',
                        dest='reindex_claims',
                        action='store_true',
                        help='Reindex claims')

    parser.add_argument('-u',
                        '--repush_claims',
                        dest='repush_claims',
                        action='store_true',
                        help='Re-push claims')

    parser.add_argument('-f',
                        '--refetch_orcidids',
                        dest='refetch_orcidids',
                        action='store_true',
                        help='Gets all orcidids changed since X (as discovered from ads api) and sends them to the queue.')

    parser.add_argument('-e',
                        '--reprocess_bibcodes',
                        dest='reprocess_bibcodes',
                        action='store_true',
                        help='Reprocesses all claims for the given bibcodes, confirms claim array lengths')

    parser.add_argument('-m',
                        '--force',
                        dest='force',
                        action='store_true',
                        default=False,
                        help='Force rebuilding the length of the ORCID array when reprocessing a bibcode')

    parser.add_argument('-s',
                        '--since',
                        dest='since_date',
                        action='store',
                        default=None,
                        help='Starting date for reindexing')

    parser.add_argument('-o',
                        '--oid',
                        dest='orcid_ids',
                        action='store',
                        default=None,
                        help='Comma delimited list of orcid-ids to re-index (use with refetch orcidids)')

    parser.add_argument('-b',
                        '--bibcodes',
                        dest='bibcodes',
                        action='store',
                        default=None,
                        help='Comma delimited list of bibcodes (for diagnostics)')

    parser.add_argument('-k',
                        '--kv',
                        dest='kv',
                        action='store_true',
                        default=False,
                        help='Show current values of KV store')

    parser.add_argument('-d',
                        '--diagnose',
                        dest='diagnose',
                        action='store_true',
                        default=False,
                        help='Show me what you would do with ORCiDs/bibcodes')

    args = parser.parse_args()
    if args.orcid_ids:
        args.orcid_ids = [x.strip() for x in args.orcid_ids.split(',')]
    if args.bibcodes:
        args.bibcodes = [x.strip() for x in args.bibcodes.split(',')]


    if args.kv:
        print_kvs()

    if args.diagnose:
        show_api_diagnostics(args.orcid_ids or ['0000-0003-3041-2092'], args.bibcodes or ['2015arXiv150305881C'])

    if args.reindex_claims:
        reindex_claims(args.since_date, args.orcid_ids)
    elif args.repush_claims:
        repush_claims(args.since_date, args.orcid_ids)
    elif args.refetch_orcidids:
        refetch_orcidids(args.since_date, args.orcid_ids)
    elif args.reprocess_bibcodes:
        reprocess_bibcodes(args.bibcodes, args.force)

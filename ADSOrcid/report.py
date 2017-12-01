from adsputils import load_config, setup_logging
from ADSOrcid import tasks
from ADSOrcid.models import ClaimsLog
from levenshtein_default import query_solr
from sqlalchemy import func, and_, distinct
from dateutil.tz import tzutc
import fnmatch
import datetime

app = tasks.app
logger = setup_logging('reporting')

def claimed_records():
    # the first 7 digits of ORCID IDs are zero padding
    orcid_wild = '000000*'
    resp_pub = query_solr(config['SOLR_URL_OLD'], 'orcid_pub:"' + orcid_wild + '"', rows=10, sort="bibcode desc", fl='bibcode')
    resp_user = query_solr(config['SOLR_URL_OLD'], 'orcid_user:"' + orcid_wild + '"', rows=10, sort="bibcode desc", fl='bibcode')
    resp_other = query_solr(config['SOLR_URL_OLD'], 'orcid_other:"' + orcid_wild + '"', rows=10, sort="bibcode desc", fl='bibcode')

    logger.info('Number of records with an orcid_pub: {}'.format(resp_pub['response']['numFound']))
    logger.info('Number of records with an orcid_user: {}'.format(resp_user['response']['numFound']))
    logger.info('Number of records with an orcid_other: {}'.format(resp_other['response']['numFound']))

    start = 0
    rows = 1000

    results = resp_pub['response']['docs']
    num_orcid_pub = 0
    bibcode_pub = []
    while results:
        resp_pub = query_solr(config['SOLR_URL_OLD'], 'orcid_pub:"' + orcid_wild + '"', start=start, rows=rows,
                              sort="bibcode desc", fl='orcid_pub,bibcode')
        results = resp_pub['response']['docs']
        for i in range(len(results)):
            num = len(fnmatch.filter(results[i].get('orcid_pub'), '0000*'))
            num_orcid_pub += num
            bibcode_pub.append(results[i].get('bibcode'))
        start += rows

    logger.info('Total number of orcid_pub claims: {}'.format(num_orcid_pub))

    start = 0
    results = resp_user['response']['docs']
    num_orcid_user = 0
    bibcode_user = []
    while results:
        resp_user = query_solr(config['SOLR_URL_OLD'], 'orcid_user:"' + orcid_wild + '"', start=start, rows=rows,
                              sort="bibcode desc", fl='orcid_user,bibcode')
        results = resp_user['response']['docs']
        for i in range(len(results)):
            num = len(fnmatch.filter(results[i].get('orcid_user'), '0000*'))
            num_orcid_user += num
            bibcode_user.append(results[i].get('bibcode'))
        start += rows

    logger.info('Total number of orcid_user claims: {}'.format(num_orcid_user))

    start = 0
    results = resp_other['response']['docs']
    num_orcid_other = 0
    bibcode_other = []
    while results:
        resp_other = query_solr(config['SOLR_URL_OLD'], 'orcid_other:"' + orcid_wild + '"', start=start, rows=rows,
                              sort="bibcode desc", fl='orcid_other,bibcode')
        results = resp_other['response']['docs']
        for i in range(len(results)):
            num = len(fnmatch.filter(results[i].get('orcid_other'), '0000*'))
            num_orcid_other += num
            bibcode_other.append(results[i].get('bibcode'))
        start += rows

    logger.info('Total number of orcid_other claims: {}'.format(num_orcid_other))

    orcid_bibcodes = set(bibcode_pub).union(set(bibcode_user)).union(set(bibcode_other))
    logger.info('Total number of records with any ORCID claims: {}'.format(len(orcid_bibcodes)))

def num_claims(n_days=7):
    now = datetime.datetime.now(tzutc())
    beginning = now - datetime.timedelta(days=n_days)

    with app.session_scope() as session:
        status_count = session.query(func.count(distinct(ClaimsLog.orcidid)),ClaimsLog.status).filter(
            and_(ClaimsLog.created >= beginning, ClaimsLog.created <= now)).group_by(ClaimsLog.status).all()

        for i in range(len(status_count)):
            logger.info('Number of unique ORCID IDs generating claims of type {} in last {} days: {}'.
                        format(status_count[i][1],n_days,status_count[i][0]))

        statuses = ['claimed','removed','updated']

        for s in statuses:
            claims = session.query(func.count(distinct(ClaimsLog.bibcode)).
                                   filter(and_(ClaimsLog.created >= beginning, ClaimsLog.created <= now,
                                               ClaimsLog.status == s))).all()
            logger.info('Number of unique bibcodes that have been {} in the last {} days: {}'.
                        format(s,n_days,claims[0][0]))

if __name__ == '__main__':
    # Before running, tunnel into SOLR and postgres and specify localhost URLs for
    # SOLR_URL_OLD and SQLALCHEMY_URL, respectively, in local_config.py

    config = {}
    config.update(load_config())
    claimed_records()
    num_claims(7)
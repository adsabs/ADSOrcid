from adsputils import load_config, setup_logging
from ADSOrcid import tasks
from ADSOrcid.models import ClaimsLog
from levenshtein_default import query_solr
from sqlalchemy import func, and_, distinct
from dateutil.tz import tzutc
import fnmatch
import datetime
import cachetools
import time
import pytz
import urllib3
import requests

app = tasks.app
logger = setup_logging('reporting')

records_cache = cachetools.TTLCache(maxsize=1024, ttl=3600, timer=time.time, missing=None, getsizeof=None)

@cachetools.cached(records_cache)
def query_records(start=0,rows=1000):
    """
    Function to query SOLR for a set of records and return the response.
    Kept as a separate function in order to use a cache.

    :param
        start: Row number to start with; default=0
        rows:  Number of rows to retrieve; default=1000
    :return
        response: Response from the query
    """
    orcid_wild = '000000*'

    resp = query_solr(config['SOLR_URL_OLD'], 'orcid:"' + orcid_wild + '"', start=start, rows=rows,
                      sort="bibcode desc", fl='orcid_pub,orcid_user,orcid_other,bibcode')
    results = resp['response']['docs']

    return results

def query_Kibana(query='"+@log_group:\\"backoffice-orcid_pipeline-daemon\\" +@message:\\"Claim refused\\""',
                 n_days=7,rows=5):
    """
    Function to query Kibana for a given input query and return the response.

    :param query: string query, same as would be entered in the Kibana search input (be sure to escape quotes and wrap
        query in double quotes - see default query for formatting)
    :param n_days: number of days backwards to query, starting now (=0 for all time)
    :param rows: number of results to return. If you just need the total number of hits and not the results
        themselves, can be small.
    :return: JSON results
    """

    config = {}
    config.update(load_config())

    # get start and end timestamps (in milliseconds since 1970 epoch)
    now = datetime.datetime.now(tzutc())
    epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)
    end_time = (now - epoch).total_seconds() * 1000.
    if n_days != 0:
        start_time = (now - datetime.timedelta(days=n_days) - epoch).total_seconds() * 1000.
    else:
        start_time = 0.

    data = ('{"index":["cwl-*"]}\n{"size":%.0f,"sort":[{"@timestamp":{"order":"desc","unmapped_type":"boolean"}}],' %(rows) +
           '"query":{"bool":{"must":[{"query_string":{"analyze_wildcard":true, "query":'+query+'}}, ' +
           '{"range": {"@timestamp": {"gte": %.0f, "lte": %.0f,"format": "epoch_millis"}}}], "must_not":[]}}, ' % (start_time, end_time)+
           '"docvalue_fields":["@timestamp"]}\n\n')

    header = {'origin': 'https://pipeline-kibana.kube.adslabs.org',
              'authorization': 'Basic ' + config['KIBANA_TOKEN'],
              'content-type': 'application/x-ndjson',
              'kbn-version': '5.5.2'}

    url = 'https://pipeline-kibana.kube.adslabs.org/_plugin/kibana/elasticsearch/_msearch'

    # set to bypass SSL cert problem w/ Kibana
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    resp = requests.post(url, data=data, headers=header, verify=False)

    if resp.status_code == 200:
        results = resp.json()
        return results
    logger.warn('For query {}, there was a network problem: {0}\n'.format(query,resp))
    return None

def claimed_records(debug=False,test=False):
    """
    Reporting function; checks SOLR for the following:
        - number of records that have been claimed by at least one ORCID ID, in orcid_pub, orcid_user, orcid_other
            (each reported separately)
        - total number of accepted claims of each of orcid_pub, orcid_user, orcid_other (i.e. if a single record
            has two separate authors who have successfully created a claim, the number reported here is 2)
        - total number of bibcodes that have been claimed, of any type

    The report is designed to be run regularly, and the results compared to previous report runs (via logs)

    :return: None (output to logs)
    """
    if test:
        logger = setup_logging('test_claimed')

    config = {}
    config.update(load_config())

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
    num_orcid_user = 0
    num_orcid_other = 0

    bibcode_pub = set()
    bibcode_user = set()
    bibcode_other = set()
    while results:
        results = query_records(start=start,rows=rows)
        for i in range(len(results)):
            try:
                results[i]['orcid_pub']
            except KeyError:
                pass
            else:
                num_p = len(fnmatch.filter(results[i].get('orcid_pub'), '0000*'))
                num_orcid_pub += num_p
                bibcode_pub.add(results[i].get('bibcode'))
            try:
                results[i]['orcid_user']
            except KeyError:
                pass
            else:
                num_u = len(fnmatch.filter(results[i].get('orcid_user'), '0000*'))
                num_orcid_user += num_u
                bibcode_user.add(results[i].get('bibcode'))
            try:
                results[i]['orcid_other']
            except KeyError:
                pass
            else:
                num_o = len(fnmatch.filter(results[i].get('orcid_other'), '0000*'))
                num_orcid_other += num_o
                bibcode_other.add(results[i].get('bibcode'))

        if debug:
            logger.info('Number of results processed so far: {}'.format(start+rows))

        if test:
            break
        else:
            start += rows

    logger.info('Total number of orcid_pub claims: {}'.format(num_orcid_pub))
    logger.info('Total number of orcid_user claims: {}'.format(num_orcid_user))
    logger.info('Total number of orcid_other claims: {}'.format(num_orcid_other))

    orcid_bibcodes = bibcode_pub.union(bibcode_user).union(bibcode_other)
    logger.info('Total number of records with any ORCID claims: {}'.format(len(orcid_bibcodes)))

def num_claims(app,n_days=7,test=False):
    """
    Reporting function; checks the postgres database for:
        - number of unique ORCID IDs who have created claims in the given range of time
            - if a single user creates a number of claims in the time period, that user is reported only once here
            - counts claims of all types
        - number of claims on a single bibcode by a single user in the given range of time
            - if a user claims 5 separate records in the given time period, the number of claims reported is 5
            - if a user claims a record multiple times in the given time period, the number of claims reported is 1
            - counts claims of type claimed, updated, and removed
        - total number of claims in the given time period
            - does not remove duplicates
            - meant to be compared to Kibana reports on number of rejected claims
    :param n_days: number of days backwards to look, starting from now
    :return: None (outputs to logs)
    """

    if test:
        logger = setup_logging('test_num_claimed')

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
            #claims = session.query(func.count(distinct(ClaimsLog.bibcode)).
            #                       filter(and_(ClaimsLog.created >= beginning, ClaimsLog.created <= now,
            #                                   ClaimsLog.status == s))).all()
            claims = session.query(ClaimsLog).distinct(ClaimsLog.bibcode, ClaimsLog.orcidid).filter(and_(
                ClaimsLog.created >= beginning, ClaimsLog.created <= now,ClaimsLog.status == s)).all()

            logger.info('Number of unique claims by a unique bibcode+ORCID ID pair that have been {} in the last {} days: {}'.
                        format(s,n_days,len(claims)))

        total_claims = session.query(ClaimsLog).filter(and_(ClaimsLog.created >= beginning,
                                                            ClaimsLog.created <= now,
                                                            ClaimsLog.status.in_(statuses))).all()

        logger.info('Total number of non-unique claims with status {} in the last {} days, to compare with logging on rejected claims: {}'.
                    format(statuses,n_days,len(total_claims)))

def num_refused_claims(n_days=7,test=False):
    """
    Queries logs via Kibana to get the number of refused claims over a given time period.

    :param n_days: Number of days backwards to look, starting from now
    :return: None (outputs to logs)
    """

    if test:
        logger = setup_logging('test_kibana')

    query = '"+@log_group:\\"backoffice-orcid_pipeline-daemon\\" +@message:\\"Claim refused\\""'

    # don't need the full set of results as the total is passed separately
    resp = query_Kibana(query=query,n_days=n_days,rows=5)

    total = resp['responses'][0]['hits']['total']

    logger.info('Number of claims rejected in the last {} days: {}'.format(n_days,total))

def num_missing_profile(n_days=7,test=False):
    """
    Queries logs via Kibana to get the number of profiles reported missing over a given time period.

    :param n_days: Number of days backwards to look, starting from now
    :return: None (outputs to logs)
    """

    if test:
        logger = setup_logging('test_kibana')

    query = '"+@log_group:\\"backoffice-orcid_pipeline-daemon\\" +@message:\\"Missing profile for\\""'

    resp = query_Kibana(query=query, n_days=n_days, rows=5)

    total = resp['responses'][0]['hits']['total']

    logger.info('Number of missing profile errors in the last {} days: {}'.format(n_days, total))

if __name__ == '__main__':
    # Runs all reporting scripts, outputs results to logs

    # Before running, tunnel into SOLR and postgres and specify localhost URLs for
    # SOLR_URL_OLD and SQLALCHEMY_URL, respectively, in local_config.py

    config = {}
    config.update(load_config())
    claimed_records()
    num_claims(n_days=7)
    num_refused_claims(n_days=7)
    num_missing_profile(n_days=7)
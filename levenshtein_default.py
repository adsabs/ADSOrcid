from ADSOrcid.models import AuthorInfo
from ADSOrcid import tasks
from adsputils import load_config, setup_logging
import numpy as np
import Levenshtein
import sys
import os
import requests
import matplotlib
matplotlib.use('TkAgg')
from matplotlib import pyplot as py

app = tasks.app
logger = setup_logging('levenshtein_test')

def extract_names(author_dict):
    authors = []
    for f in ('author', 'orcid_name', 'author_norm', 'short_name'):
        if f in author_dict and author_dict[f]:
            for name in author_dict[f]:
                authors.append(name)

    return authors

def query_solr(endpoint, query, start=0, rows=200, sort="date desc", fl='bibcode'):
    d = {'q': query,
         'sort': sort,
         'start': start,
         'rows': rows,
         'wt': 'json',
         'indent': 'true'
         }
    if fl:
        d['fl'] = fl

    response = requests.get(endpoint, params=d)
    if response.status_code == 200:
        results = response.json()
        return results
    logger.warn('For query {0}, there was a network problem: {1}\n'.format(query,response))
    return None

def get_max_lev(orcid_field='orcid_user'):
    """
    For every author with an ORCID ID in the authors table in postgres, checks SOLR for any accepted claims (in
    orcid_pub, orcid_other, or orcid_user) that they have. Calculates the maximum Levenshtein distance ratio
    possible for any of the author's name variants (from postgres) against the name on the record with the claim.

    :param orcid_field: Field to query in SOLR; orcid_pub, orcid_user, or orcid_other
    :return max_lev: list of maximum Levenshtein ratios possible for accepted claims, one per ORCID ID/record pair
    """

    max_lev_dict = {}
    logger.info('Starting max lev function for {}'.format(orcid_field))
    num_rec = 0
    with app.session_scope() as session:
        # get list of ORCID IDs to check from the authors table in postgres. This will get all ORCID IDs for
        # orcid_user and orcid_other, but will miss some for orcid_pub
        for r in session.query(AuthorInfo).order_by(AuthorInfo.id.asc()).all():
            if num_rec % 100 == 0:
                logger.info('Querying record {} for {}'.format(num_rec,orcid_field))
            rec = r.toJSON()
            orcidid = rec['orcidid']
            # use all name variants listed in postgres for a given author
            authors = extract_names(rec['facts'])
            if orcid_field == 'orcid_pub':
                response = query_solr(config['SOLR_URL_OLD'], 'orcid_pub:"' + orcidid + '"', sort="bibcode desc", fl='*')
            elif orcid_field == 'orcid_other':
                response = query_solr(config['SOLR_URL_OLD'], 'orcid_other:"' + orcidid + '"', sort="bibcode desc",
                                      fl='*')
            else:
                response = query_solr(config['SOLR_URL_OLD'], 'orcid_user:"' + orcidid + '"', sort="bibcode desc",
                                      fl='*')
            if response['response']['docs'] == []:
                num_rec += 1
                continue
            else:
                data = response['response']['docs']
                lev_all = []
                for record in data:
                    idx = np.where(np.array(record[orcid_field]) == orcidid)
                    rec_author = record['author'][idx[0][0]]
                    tmplev = []
                    for var in authors:
                        # make sure the two comparison strings are the same type
                        try:
                            rec_author = unicode(rec_author,'utf-8')
                        except TypeError:
                            rec_author = rec_author
                        try:
                            var = unicode(var,'utf-8')
                        except TypeError:
                            var = var
                        # check the Levenshtein ratio for all variant names
                        tmplev.append(Levenshtein.ratio(rec_author,var))
                    # keep only the best one for each record
                    lev_all.append(max(tmplev))
            max_lev_dict[orcidid] = lev_all

            num_rec += 1

    # unroll the dictionary into a giant list
    max_lev = [item for sublist in max_lev_dict.values() for item in sublist]

    return max_lev

def get_mismatch_lev(save_path=os.getcwd()):
    """
    Gets Levenshtein ratios for rejected claims from log files.
    Logs must be grepped for 'No match found' and the results
    saved to the file logs_mismatch.txt in the save_path before running.

    :param save_path: path to the folder that contains the file logs_mismatch.txt
    :return bad_lev: a list of the Levenshtein ratios of the author names from the rejected claims
    """

    mismatch_file = save_path + '/logs_mismatch.txt'
    with open(mismatch_file) as f:
        lines = f.readlines()

    bad_lev = []
    search_string = 'No match found: the closest is: ('
    for line in lines:
        beg = line.find(search_string)+len(search_string)
        bad_lev.append(float(line[beg:line.find(',',beg)]))

    return bad_lev

def plot_hist(orcid_field='orcid_user',save_path=os.getcwd()):
    """
    Plots histograms of the Levenshtein ratio for every claim, accepted (into orcid_pub, orcid_user,
    orcid_other) or rejected

    :param orcid_field: claims being plotted
        accepted claims: orcid_pub, orcid_user, orcid_other
        rejected claims: mismatch
    :param save_path: path to save figure image files
    :return: None
    """
    if orcid_field == 'mismatch':
        lev = get_mismatch_lev(save_path=save_path)
        plcol = 'red'
    else:
        lev = get_max_lev(orcid_field=orcid_field)
        plcol = 'green'

    bin_edges = np.arange(0,1.05,.05)

    py.clf()

    n, bins, patches = py.hist(lev, bin_edges, facecolor=plcol, alpha=0.75, edgecolor='black')

    nperc = 100. * n / n.sum()
    for i in range(len(bins[0:-1])):
        if nperc[i] > 0.1:
            py.text(bins[i], n[i] + n.max() * .01, '{:.1f}%'.format(nperc[i]))

    py.text(0.05,n.max() * 0.95,'Median: {:.3f}'.format(np.median(lev)))
    py.text(0.05,n.max() * 0.9, 'Mean: {:.3f}'.format(np.mean(lev)))
    py.text(0.05,n.max() * 0.85, 'Standard deviation: {:.3f}'.format(np.std(lev)))

    py.axvline(x=config['MIN_LEVENSHTEIN_RATIO'],ls='--')
    py.xlabel('Levenshtein distance ratio')
    py.ylabel('No. of claims')
    py.title(orcid_field)

    np.save(save_path+'/maxlev_' + orcid_field + '.npy',lev)
    py.savefig(save_path+'/maxlev_' + orcid_field + '.png')

if __name__ == '__main__':
    # When running from command line, enter the save_path as an argument. This
    # is the path to which the plots and data will be saved.

    # Before running, tunnel into SOLR and postgres and specify localhost URLs for
    # SOLR_URL_OLD and SQLALCHEMY_URL, respectively, in local_config.py

    save_path = sys.argv[1]
    config = {}
    config.update(load_config())
    plot_hist(orcid_field='orcid_user',save_path=save_path)
    plot_hist(orcid_field='orcid_other',save_path=save_path)
    plot_hist(orcid_field='orcid_pub',save_path=save_path)
    plot_hist(orcid_field='mismatch',save_path=save_path)

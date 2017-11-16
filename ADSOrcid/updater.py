
"""
Library for updating papers (db claims/records). 
"""

from ADSOrcid import names
from ADSOrcid.models import ClaimsLog, Records
from adsputils import get_date, setup_logging
from datetime import timedelta
from sqlalchemy.sql.expression import and_
import Levenshtein
import json
import requests


logger = setup_logging('updater')     


def update_record(rec, claim, min_levenshtein):
    """
    update the ADS Record; we'll add ORCID information into it 
    (at the correct position)
    
    :param: rec - JSON structure, it contains metadata; we expect
            it to have 'authors' field, and 'claims' field
            
    :param: claim - JSON structure, it contains claim data, 
            especially:
                orcidid
                author
                author_norm
            We use those field to find out which author made the
            claim.
    
    :return: tuple(clain_category, position) or None if no record
        was updated
    """
    assert(isinstance(rec, dict))
    assert(isinstance(claim, dict))
    assert('authors' in rec)
    assert('claims' in rec)
    assert(isinstance(rec['authors'], list))
    
    claims = rec.get('claims', {})
    rec['claims'] = claims
    authors = rec.get('authors', [])
    
    # make sure the claims have the necessary structure
    fld_name = u'unverified'
    if 'account_id' in claim and claim['account_id']: # the claim was made by ADS verified user
        fld_name = u'verified'
    
    num_authors = len(authors)
    
    if fld_name not in claims or claims[fld_name] is None:
        claims[fld_name] = ['-'] * num_authors
    elif len(claims[fld_name]) < num_authors: # check the lenght is correct
        claims[fld_name] += ['-'] * (num_authors - len(claims[fld_name]))
    elif len(claims[fld_name]) > num_authors:
        claims[fld_name] = claims[fld_name][0:num_authors]

    # always remove the orcidid
    modified = False    
    orcidid = claim['orcidid']
    for v in claims.values():
        while orcidid in v:
            v[v.index(orcidid)] = '-'
            modified = True
            
    # create unique list of all name variations to match name in bib record,
    # keep original order of names
    claim_names = []
    for fx in ('author', 'orcid_name', 'author_norm', 'short_name'):
        if fx in claim and claim[fx]:            
            assert(isinstance(claim[fx], list))
            for name in claim[fx]:
                if name not in claim_names:
                    claim_names.append(name)

    if claim_names:
        idx = find_orcid_position(rec['authors'], claim_names, min_levenshtein=min_levenshtein)
        if idx > -1:              
            if idx >= num_authors:
                logger.error(u'Index is beyond list boundary: \n' + 
                             u'Field {fx}, author {author}, len(authors)={la}, len({fx})=lfx'
                             .format(fx=fx, author=claim[fx], la=num_authors, lfx=len(claim[fx]))
                             )
            else:
                claims[fld_name][idx] = claim.get('status', 'created') == 'removed' and '-' or orcidid
                return (fld_name, idx)
    
    if modified:
        return ('removed', -1)

def find_orcid_position(authors_list, name_variants,
                        min_levenshtein=0.9):
    """
    Find the position of ORCID in the list of other strings
    
    :param authors_list - array of names that will be searched
    :param name_variants - array of names to be matched
    
    :return list of positions that match
    
    Attempts to match any of the names given in name_variants against
    the list of names in authors_list by normalizing both lists.
    If exact matches fail, attempt an approximate match so long as
    the Levenshtein ratio is above min_levenshtein.

    Note: the array of name_variants should be given in order of 
    decreasing authority and "quality" (typically full author names 
    first, abbreviations later) since matching is attempted in sequence

    """
    al = [names.cleanup_name(x).lower().encode('utf8') for x in authors_list]
    nv = [names.cleanup_name(x).lower().encode('utf8') for x in name_variants]
    
    # compute similarity between all authors (and the supplied variants)
    # this is not very efficient, however the lists should be small
    # and short, so 3000 operations take less than 1s)
    res = []
    aidx = vidx = 0
    for variant in nv:
        aidx = 0
        for author in al:
            ldist = Levenshtein.ratio(author, variant)
            if ldist == 1:
                logger.debug('Found exact match for author="%s", variant="%s", authors=%s', 
                             author, variant, authors_list)
                return aidx
            res.append((ldist, aidx, vidx))
            aidx += 1
        vidx += 1
        
    # sort results from the highest match but return longest names first
    # XXX: maybe we should have a weighted score so that the score for
    # longer names is boosted, but the ratio already does that to some extent
    res = sorted(res, key=lambda x: (x[0], len(al[x[1]] + nv[x[2]])), reverse=True)
    
    if len(res) == 0:
        return -1
    
    if res[0][0] < min_levenshtein:
        logger.debug('No match found, closest is: %s (required:%s) author="%s", variant="%s"', 
                     res[0], min_levenshtein, al[res[0][1]], nv[res[0][2]])
        return -1
    
    logger.debug('Found match: %s (required:%s), author="%s", varian="%s", authors=%s', 
                 res[0], min_levenshtein, al[res[0][1]], nv[res[0][2]], authors_list)
    return res[0][1]


def _remove_orcid(rec, orcidid):
    """Finds and removes the orcidid from the list of claims.
    
    :return: True/False if the rec was modified
    """
    modified = False
    claims = rec.get('claims', {})
    for data in claims.values():
        if orcidid in data:
            data[data.index(orcidid)] = '-'
            modified = True
    return modified

def reindex_all_claims(app, orcidid, since=None, ignore_errors=False):
    """
    Procedure that will re-play all claims
    that were modified since a given starting point.
    """
    
    last_check = get_date(since or '1974-11-09T22:56:52.518001Z')
    recs_modified = set()
    
    with app.session_scope() as session:
        author = app.retrieve_orcid(orcidid)
        claimed = set()
        removed = set()
        for claim in session.query(ClaimsLog).filter(
                        and_(ClaimsLog.orcidid == orcidid, ClaimsLog.created > last_check)
                        ).all():
            if claim.status in ('claimed', 'updated', 'forced'):
                claimed.add(claim.bibcode)
            elif claim.status == 'removed':
                removed.add(claim.bibcode)

        with app.session_scope() as session:    
            for bibcode in removed:
                r = session.query(Records).filter_by(bibcode=bibcode).first()
                if r is None:
                    continue
                rec = r.toJSON()
                if _remove_orcid(rec, orcidid):
                    r.claims = json.dumps(rec.get('claims'))
                    r.updated = get_date()
                    recs_modified.add(bibcode)
        
            for bibcode in claimed:
                r = session.query(Records).filter_by(bibcode=bibcode).first()
                if r is None:
                    continue
                rec = r.toJSON()

                claim = {'bibcode': bibcode, 'orcidid': orcidid}
                claim.update(author.get('facts', {}))
                try:
                    _claims = update_record(rec, claim)
                    if _claims:
                        r.claims = json.dumps(rec.get('claims', {}))
                        r.updated = get_date()
                        recs_modified.add(bibcode)
                except Exception, e:
                    if ignore_errors:
                        app.logger.error(u'Error processing {0} {1}'.format(bibcode, orcidid))
                    else:
                        raise e
                    
                    
            session.commit()
        
        return list(recs_modified)


def get_all_touched_profiles(app, since='1974-11-09T22:56:52.518001Z', max_failures=5, max_cons_failures=2):
    """Queries the orcid-service for all new/updated
    orcid profiles"""
    
    orcid_ids = set()
    latest_point = get_date(since) # RFC 3339 format
    failures = cons_failures = 0
    
    while True:
        # increase the timestamp by one microsec and get new updates
        latest_point = latest_point + timedelta(microseconds=1)
        r = requests.get(app.conf.get('API_ORCID_UPDATES_ENDPOINT') % latest_point.isoformat(),
                    params={'fields': ['orcid_id', 'updated', 'created']},
                    headers = {'Authorization': 'Bearer {0}'.format(app.conf.get('API_TOKEN'))})
    
        if r.status_code != 200:
            cons_failures += 1
            failures += 1
            if cons_failures < max_cons_failures and failures < max_failures:
                logger.warn('Error querying API enpoint, will retry: failures=%s, cons_failures=%s' % (failures, cons_failures))
                latest_point = latest_point - timedelta(microseconds=1)
                continue
            raise Exception(r.text)
        
        if r.text.strip() == "":
            break
        
        # we received the data, immediately update the databaes (so that other processes don't 
        # ask for the same starting date)
        data = r.json()
        
        if len(data) == 0:
            break
        
        cons_failures = 0 # reset
        
        # data should be ordered by date update (but to be sure, let's check it); we'll save it
        # as latest 'check point'
        dates = [get_date(x['updated']) for x in data]
        dates = sorted(dates, reverse=True)
        latest_point = dates[0]
        for rec in data:
            orcid_ids.add(rec['orcid_id'])
        
    return list(orcid_ids)
            

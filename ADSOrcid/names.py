
from builtins import range
from ADSOrcid.models import AuthorInfo, ChangeLog
from copy import deepcopy
from .exceptions import IgnorableException
import sys
"""
Tools for enhancing our knowledge about orcid ids (authors).
"""


    
   

    

def build_short_forms(orig_name):
    orig_name = cleanup_name(orig_name)
    if ',' not in orig_name:
        return [] # refuse to do anything
    surname, other_names = orig_name.split(',', 1)
    ret = set()
    parts = [x for x in other_names.split(' ') if len(x)]
    if len(parts) == 1 and len(parts[0]) == 1:
        return []
    for i in range(len(parts)):
        w_parts = deepcopy(parts)
        x = w_parts[i]
        if len(x) > 1:
            w_parts[i] = x[0]
            ret.add(u'{0}, {1}'.format(surname, ' '.join(w_parts)))
    w_parts = [x[0] for x in parts]
    while len(w_parts) > 0:
        ret.add(u'{0}, {1}'.format(surname, ' '.join(w_parts)))
        w_parts.pop()

    return list(ret)
    
        
    
def extract_names(orcidid, doc):
    o = cleanup_orcidid(orcidid)
    r = {}
    if 'orcid_pub' not in doc:
        raise Exception('Solr doc is missing orcid field')
    
    orcids = [cleanup_orcidid(x) for x in doc['orcid_pub']]
    idx = None
    try:
        idx = orcids.index(o)
    except ValueError:
        raise Exception('Orcid %s is not present in the response for: %s' % (orcidid, doc))
    
    for f in 'author', 'author_norm':
        if f in doc:
            try:
                r[f] = doc[f][idx]
            except IndexError:
                raise Exception('The orcid %s should be at index: %s (but it wasnt)\n%s'
                                 % (orcidid, idx, doc))
    return r

    
def cleanup_orcidid(orcid):
    return orcid.replace('-', '').lower()

        
def cleanup_name(name):
    """
    Removes some unnecessary characters from the name; 
    always returns a unicode
    """
    if not name:
        return u''
    if sys.version_info > (3,):
        test_type = str
    else:
        test_type = unicode
    if not isinstance(name, test_type):
        name = name.decode('utf8') # assumption, but ok...
    name = name.replace(u'.', u'')
    name = u' '.join(name.split())
    return name 
        
    
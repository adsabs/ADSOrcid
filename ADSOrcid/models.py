# -*- coding: utf-8 -*-

from builtins import str
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, TIMESTAMP
from sqlalchemy.types import Enum
import json
import sys
from adsputils import get_date, UTCDateTime

Base = declarative_base()


class KeyValue(Base):
    __tablename__ = 'storage'
    key = Column(String(255), primary_key=True)
    value = Column(Text)

class AuthorInfo(Base):
    __tablename__ = 'authors'
    id = Column(Integer, primary_key=True)
    orcidid = Column(String(19), unique=True)
    name = Column(String(255))
    facts = Column(Text)
    status = Column(Enum('blacklisted', 'postponed', name='status'))
    account_id = Column(Integer)
    created = Column(UTCDateTime, default=get_date)
    updated = Column(UTCDateTime, default=get_date)
    
    def toJSON(self):
        return {'id': self.id, 'orcidid': self.orcidid,
                'name': self.name, 'facts': self.facts and json.loads(self.facts) or {},
                'status': self.status, 'account_id': self.account_id,
                'created': self.created and get_date(self.created).isoformat() or None, 'updated': self.updated and get_date(self.updated).isoformat() or None
                }
    
    
class ClaimsLog(Base):
    __tablename__ = 'claims'
    id = Column(Integer, primary_key=True)
    orcidid = Column(String(19))
    bibcode = Column(String(19))
    status = Column(Enum('claimed', 'updated', 'removed', 'unchanged', 'forced', '#full-import', name='status'))
    provenance = Column(String(255))
    created = Column(UTCDateTime, default=get_date)
    
    def toJSON(self):
        if sys.version_info > (3,):
            out_prov = str(self.provenance)
        else:
            out_prov = unicode(self.provenance)
        return {'id': self.id, 'orcidid': self.orcidid,
                'bibcode': self.bibcode, 'status': self.status,
                'provenance': out_prov, 'created': self.created and get_date(self.created).isoformat() or None
                }
    
    
class Records(Base):
    __tablename__ = 'records'
    id = Column(Integer, primary_key=True)
    bibcode = Column(String(19))
    claims = Column(Text)
    authors = Column(Text)
    created = Column(UTCDateTime, default=get_date)
    updated = Column(UTCDateTime, default=get_date)
    processed = Column(UTCDateTime)
    
    def toJSON(self):
        return {'id': self.id, 'bibcode': self.bibcode,
                'authors': self.authors and json.loads(self.authors) or [],
                'claims': self.claims and json.loads(self.claims) or {},
                'created': self.created and get_date(self.created).isoformat() or None, 'updated': self.updated and get_date(self.updated).isoformat() or None, 
                'processed': self.processed and get_date(self.processed).isoformat() or None
                }


class ChangeLog(Base):
    __tablename__ = 'change_log'
    id = Column(Integer, primary_key=True)
    created = Column(UTCDateTime, default=get_date)
    key = Column(String(255))
    oldvalue = Column(Text)
    newvalue = Column(Text)
    
    
    def toJSON(self):
        return {'id': self.id, 
                'key': self.key,
                'created': self.created and get_date(self.created).isoformat() or None,
                'newvalue': self.newvalue,
                'oldvalue': self.oldvalue
                }
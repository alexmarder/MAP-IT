import os
import re
from functools import partial

import lxml.html

from utils import File2


cdef class OrgInfo:
    def __init__(self, org_id, changed, org_name, country, source):
        self.org_id = org_id
        self.changed = changed
        self.org_name = org_name
        self.country = country
        self.source = source

    cpdef dict asdict(self):
        return dict(org_id=self.org_id, changed=self.changed, org_name=self.org_name, country=self.country, source=self.source)


cdef class ASInfo:
    def __init__(self, aut, changed, aut_name, org_id, source):
        self.aut = aut
        self.changed = changed
        self.aut_name = aut_name
        self.org_id = org_id
        self.source = source

    cpdef dict asdict(self):
        return dict(aut=self.aut, changed=self.changed, aut_name=self.aut_name, org_id=self.org_id, source=self.source)


cdef class PotarooInfo:
    def __init__(self, aut, aut_name, name, country, url):
        self.aut = aut
        self.aut_name = aut_name
        self.name = name
        self.country = country
        self.url = url

    cpdef dict asdict(self):
        return dict(aut=self.aut, aut_name=self.aut_name, name=self.name, country=self.country, url=self.url)


cdef class Info:
    def __init__(self, asinfo=None, orginfo=None, potarooinfo=None):
        self.asinfo = asinfo
        self.orginfo = orginfo
        self.potarooinfo = potarooinfo

    @property
    def asn(self):
        if self.asinfo:
            return self.asinfo.aut
        elif self.potarooinfo:
            return self.potarooinfo.aut

    @property
    def asn_name(self):
        if self.potarooinfo:
            return self.potarooinfo.name

    @property
    def country(self):
        if self.potarooinfo:
            return self.potarooinfo.country
        elif self.orginfo:
            return self.orginfo.country

    @property
    def name(self):
        if self.orginfo:
            return self.orginfo.org_name
        elif self.potarooinfo:
            return self.potarooinfo.name

    @property
    def org(self):
        if self.orginfo:
            return self.orginfo.org_id
        elif self.asinfo:
            return self.asinfo.aut_name
        elif self.potarooinfo:
            return self.potarooinfo.aut_name

    @property
    def url(self):
        if self.potarooinfo:
            return self.potarooinfo.url


cdef class AS2Org(dict):
    def __init__(self, str filename, bint include_potaroo=False, str compression='infer', str additional='validation-siblings.txt', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data = {}
        ases, orgs = read_caida(filename, compression)
        if additional:
            if os.path.exists(additional):
                with open(additional) as f:
                    for line in f:
                        if line.strip() and line[0] != '#':
                            asns = list(map(int, line.split()))
                            for first in asns:
                                if first in ases:
                                    asinfo = ases[first]
                                    forg = asinfo.org_id
                                    for asn in asns:
                                        old = ases[asn].asdict()
                                        if old['org_id'] != forg:
                                            old['org_id'] = forg
                                            old['source'] = additional
                                            ases[asn] = ASInfo(**old)
                                    break
            else:
                print('WARNING: The file {} does not exists.'.format(additional))
        for asn, asinfo in ases.items():
            self.data[asn] = Info(asinfo=asinfo, orginfo=orgs[asinfo.org_id])
        if include_potaroo:
            pots = {p.aut: p for p in potaroo()}
            for asn, potarooinfo in pots.items():
                if asn in self:
                    self.data[asn].potarooinfo = potarooinfo
                else:
                    self.data[asn] = Info(potarooinfo=potarooinfo)
        for asn, info in self.data.items():
            self[asn] = info.org
    
    def __missing__(self, key):
        return str(key)

    cpdef Info info(self, int asn):
        return self.data[asn]

    cpdef str name(self, int asn):
        return self.data[asn].name if asn in self.data else str(asn)


def add_asn(ases, t):
    aut, changed, aut_name, org_id, source = t
    aut = int(aut)
    ases[aut] = ASInfo(aut, changed, aut_name, org_id, source)


def add_org(orgs, t):
    orgs[t[0]] = OrgInfo(*t)


def read_caida(filename, compression):
    ases = {}
    orgs = {}
    method = None
    format_re = re.compile(r'# format:\s*(.*)')
    with File2(filename, compression=compression) as f:
        for line in f:
            m = format_re.match(line)
            if m:
                fields = m.group(1).split('|')
                method = partial(add_org, orgs) if fields[0] == 'org_id' else partial(add_asn, ases)
            elif line[0] != '#' and method is not None:
                method(line.split('|'))
    return ases, orgs


def potaroo(filename='autnums2.html'):
    regex = re.compile(r'AS(\d+)\s+(-Reserved AS-|[A-Za-z0-9-]+)?(?:\s+-\s+)?(.*),\s+([A-Z]{2})')
    t = lxml.html.parse(filename).getroot()
    t.make_links_absolute('http://bgp.potaroo.net/cidr/autnums.html')
    for line, a in zip(t.find('.//pre').text_content().splitlines()[1:], t.find('.//pre').xpath('.//a')):
        try:
            asn, aid, name, country = regex.match(line).groups()
            yield PotarooInfo(int(asn), aid, name, country, a.get('href'))
        except AttributeError:
            pass

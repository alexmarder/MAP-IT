import re
from collections import namedtuple, UserDict
from functools import partial
from logging import getLogger

import lxml.html
import requests

from utils import File2

log = getLogger()

OrgInfo = namedtuple('OrgInfo', ['org_id', 'changed', 'org_name', 'country', 'source'])
ASInfo = namedtuple('ASInfo', ['aut', 'changed', 'aut_name', 'org_id', 'source'])
PotarooInfo = namedtuple('PotarooInfo', ['aut', 'aut_name', 'name', 'country', 'url'])


class Info:
    def __init__(self, asinfo=None, orginfo=None, potarooinfo=None):
        self._asinfo = asinfo
        self._orginfo = orginfo
        self._potarooinfo = potarooinfo

    @property
    def asn(self):
        if self._asinfo:
            return self._asinfo.aut
        elif self._potarooinfo:
            return self._potarooinfo.aut

    @property
    def asinfo(self):
        return self._asinfo

    @asinfo.setter
    def asinfo(self, asinfo):
        self._asinfo = asinfo

    @property
    def asn_name(self):
        if self._potarooinfo:
            return self._potarooinfo.name

    @property
    def country(self):
        if self._potarooinfo:
            return self._potarooinfo.country
        elif self._orginfo:
            return self._orginfo.country

    @property
    def name(self):
        if self._orginfo:
            return self._orginfo.org_name
        elif self._potarooinfo:
            return self._potarooinfo.name

    @property
    def org(self):
        if self._orginfo:
            return self._orginfo.org_id
        elif self._asinfo:
            return self._asinfo.aut_name
        elif self._potarooinfo:
            return self._potarooinfo.aut_name

    @property
    def orginfo(self):
        return self._orginfo

    @orginfo.setter
    def orginfo(self, orginfo):
        self._orginfo = orginfo

    @property
    def potarooinfo(self):
        return self._potarooinfo

    @potarooinfo.setter
    def potarooinfo(self, potarooinfo):
        self._potarooinfo = potarooinfo

    @property
    def url(self):
        if self._potarooinfo:
            return self._potarooinfo.url


class AS2Org(UserDict):
    def __init__(self, filename, include_potaroo=True, compression='infer'):
        super().__init__()
        log.info('Creating AS2Org tool.')
        ases, orgs = read_caida(filename, compression)
        for asn, asinfo in ases.items():
            self.data[asn] = Info(asinfo=asinfo, orginfo=orgs[asinfo.org_id])
        if include_potaroo:
            pots = {p.aut: p for p in potaroo()}
            for asn, potarooinfo in pots.items():
                if asn in self:
                    self.data[asn].potarooinfo = potarooinfo
                else:
                    self.data[asn] = Info(potarooinfo=potarooinfo)

    def __getitem__(self, asn):
        return self.data[asn].org if asn in self.data else str(asn)

    def info(self, asn):
        return self.data[asn]

    def name(self, asn):
        return self.data[asn].name if asn in self.data else str(asn)


def add_asn(ases, t):
    ases[int(t[0])] = ASInfo(*t)


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


def potaroo(url='http://bgp.potaroo.net/cidr/autnums.html'):
    regex = re.compile(r'AS(\d+)\s+(-Reserved AS-|[A-Za-z0-9-]+)?(?:\s+-\s+)?(.*),\s+([A-Z]{2})')
    r = requests.get(url)
    t = lxml.html.fromstring(r.text)
    t.make_links_absolute('http://bgp.potaroo.net/cidr/autnums.html')
    for line, a in zip(t.find('.//pre').text_content().splitlines()[1:], t.find('.//pre').xpath('.//a')):
        try:
            asn, aid, name, country = regex.match(line).groups()
            yield PotarooInfo(int(asn), aid, name, country, a.get('href'))
        except AttributeError:
            pass

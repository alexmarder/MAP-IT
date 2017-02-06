from collections import namedtuple
from copy import copy
from logging import getLogger

import pandas as pd

log = getLogger()

columns = ['Address', 'Direction', 'Otherside', 'ASN', 'ConnASN', 'Org', 'ConnOrg', 'Direct', 'Certain', 'Stub']
UpdateInfo = namedtuple(
    'Update', columns)


class Updates:
    def __init__(self, orgs=None, asns=None, direct=None, stubs=None):
        self.orgs = {} if orgs is None else orgs
        self.asns = {} if asns is None else asns
        self.direct = set() if direct is None else direct
        self.stubs = set() if stubs is None else stubs

    def __contains__(self, half):
        return half in self.orgs

    def __copy__(self):
        return self.copy()

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False

    def __getitem__(self, half):
        return self.orgs[half]

    def __iter__(self):
        yield from self.orgs.keys()

    def __len__(self):
        return len(self.orgs)

    def asn(self, half):
        return self.asns[half]

    def asn_default(self, half, default=None):
        return self.asns.get(half, default)

    def copy(self):
        return Updates(copy(self.orgs), copy(self.asns), copy(self.direct), copy(self.stubs))

    def dataframe(self):
        if len(self) > 0:
            return pd.DataFrame(self.iteritems()).set_index(['Address', 'Direction']).sort_index()
        else:
            log.warning('There were no inferences made. This is likely because the interface graph is too sparse.')
            return pd.DataFrame(columns=columns)

    def difference(self, other):
        for k in self.orgs.keys() | other.orgs.keys():
            if self.orgs.get(k) != other.orgs.get(k):
                yield k

    def direct_mappings(self):
        for half in self.direct:
            yield half, self.asns[half], self.orgs[half]

    def has_duplicates(self):
        return any(half.otherhalf in self.orgs for half in self.orgs)

    def iscertain(self, half):
        return any(self.is_inverse(half, neighbor) for neighbor in half.neighbors)

    def isdirect(self, half):
        return half in self.direct

    def is_inverse(self, half, neighbor):
        return half.org == self.orgs.get(neighbor) and self.orgs.get(half) == neighbor.org

    def iteritems(self):
        for half, asn in self.asns.items():
            yield UpdateInfo(
                Address=half.address, Direction=half.direction,
                Otherside=half.otherside_address if half.asn != -2 else None, ASN=half.asn, ConnASN=asn, Org=half.org,
                ConnOrg=self.orgs[half], Direct=half in self.direct, Certain=self.iscertain(half),
                Stub=half in self.stubs)

    def mapping(self, half):
        return self.asns[half], self.orgs[half]

    def org(self, half):
        return self.orgs[half]

    def org_default(self, half, default=None):
        return self.orgs.get(half, default)

    def remove(self, half):
        if half in self:
            del self.asns[half]
            del self.orgs[half]
        self.direct.discard(half)

    def update(self, half, asn, org, isdirect=True, isstub=False):
        self.asns[half] = asn
        self.orgs[half] = org
        if isdirect:
            self.direct.add(half)
        if isstub:
            self.stubs.add(half)

    def update_from_half(self, half, other, isdirect=False):
        self.update(half, self.asns[other], self.orgs[other], isdirect)

    def write(self, filename):
        self.dataframe().to_csv(filename)

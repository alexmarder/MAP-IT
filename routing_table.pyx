import csv
from itertools import chain

from radix import Radix
from utils import File2

cdef list PRIVATE4 = ['0.0.0.0/8', '10.0.0.8/8', '100.64.0.0/10', '127.0.0.0/8', '169.254.0.0/16', '172.16.0.0/12',
            '192.0.0.0/24', '192.0.2.0/24', '192.31.196.0/24', '192.52.193.0/24', '192.88.99.0/24', '192.168.0.0/16',
            '192.175.48.0/24', '198.18.0.0/15', '198.51.100.0/24', '203.0.113.0/24', '240.0.0.0/4',
            '255.255.255.255/32']
cdef list PRIVATE6 = ['::1/128', '::/128', '::ffff:0:0/96', '64:ff9b::/96', '100::/64', '2001::/23', '2001::/32', '2001:1::1/128',
            '2001:2::/48', '2001:3::/32', '2001:4:112::/48', '2001:5::/32', '2001:10::/28', '2001:20::/28',
            '2001:db8::/32', '2002::/16', '2620:4f:8000::/48', 'fc00::/7', 'fe80::/10']
cdef str  MULTICAST4 = '224.0.0.0/3'
cdef str MULTICAST6 = 'FF00::/8'


class RoutingTable(Radix):
    @classmethod
    def private(cls, inet='both'):
        rt = cls()
        rt.add_private(inet=inet, remove=False)
        rt.add_default()
        return rt

    @classmethod
    def ip2as(cls, filename):
        rt = cls()
        with File2(filename) as f:
            f.readline()
            for prefix, asn in csv.reader(f):
                try:
                    rt.add_prefix(int(asn), prefix)
                except TypeError:
                    print(asn, prefix)
                    raise
        return rt

    def __init__(self):
        super().__init__()

    def __getitem__(self, item):
        return self.search_best(item).data['asn']

    def __setitem__(self, key, value):
        self.add(key).data['asn'] = value

    def add_default(self):
        self.add_prefix(0, '0.0.0.0/0')

    def add_ixp(self, str network=None, masklen=None, packed=None, remove=True):
        if remove:
            covered = self.search_covered(network, masklen) if network and masklen else self.search_covered(network)
            for node in covered:
                try:
                    self.delete(node.prefix)
                except KeyError:
                    pass
        self.add_prefix(-1, network)

    def add_prefix(self, int asn, *args, **kwargs):
        node = self.add(*args, **kwargs)
        node.data['asn'] = asn

    def add_multicast(self, str inet='both', bint remove=True):
        prefixes = []
        if inet == 'ipv4' or 'both':
            prefixes.append(MULTICAST4)
        if inet == 'ipv6' or 'both':
            prefixes.append(MULTICAST6)
        for prefix in prefixes:
            if remove:
                for node in self.search_covered(prefix):
                    self.delete(node.prefix)
            self.add_prefix(-3, prefix)

    def add_private(self, str inet='both', bint remove=True):
        if inet == 'both':
            prefixes = chain(PRIVATE4, PRIVATE6)
        elif inet == 'ipv4':
            prefixes = PRIVATE4
        elif inet == 'ipv6':
            prefixes = PRIVATE6
        else:
            raise Exception('Unknown INET {}'.format(inet))
        for prefix in prefixes:
            if remove:
                nodes = self.search_covered(prefix)
                for node in nodes:
                    self.delete(node.prefix)
            self.add_prefix(-2, prefix)

    def add_rir(self, rir, ixp_asns):
        rirrows = []
        for address, prefixlen, asn in rir:
            if asn not in ixp_asns:
                if not self.search_covering(address, prefixlen):
                    rirrows.append((address, prefixlen, asn))
        for address, prefixlen, asn in rirrows:
            self.add_prefix(asn, address, prefixlen)

    def isglobal(self, str address):
        return self[address] >= -1


cpdef bint valid(long asn) except -1:
    return asn != 23456 and 0 < asn < 64496 or 131071 < asn < 4200000000
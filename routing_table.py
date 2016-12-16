from itertools import chain
import sys

import pandas as pd
from radix import Radix

PRIVATE4 = ['0.0.0.0/8', '10.0.0.8/8', '100.64.0.0/10', '127.0.0.0/8', '169.254.0.0/16', '172.16.0.0/12',
            '192.0.0.0/24', '192.0.2.0/24', '192.31.196.0/24', '192.52.193.0/24', '192.88.99.0/24', '192.168.0.0/16',
            '192.175.48.0/24', '198.18.0.0/15', '198.51.100.0/24', '203.0.113.0/24', '240.0.0.0/4',
            '255.255.255.255/32']
PRIVATE6 = ['::1/128', '::/128', '::ffff:0:0/96', '64:ff9b::/96', '100::/64', '2001::/23', '2001::/32', '2001:1::1/128',
            '2001:2::/48', '2001:3::/32', '2001:4:112::/48', '2001:5::/32', '2001:10::/28', '2001:20::/28',
            '2001:db8::/32', '2002::/16', '2620:4f:8000::/48', 'fc00::/7', 'fe80::/10']


class RoutingTable(Radix):
    def __init__(self):
        super().__init__()

    def __getitem__(self, item):
        return self.search_best(item).data['asn']

    def __setitem__(self, key, value):
        self.add(key).data['asn'] = value

    def add_default(self):
        self.add_prefix(0, '0.0.0.0/0')

    def add_ixp(self, network=None, masklen=None, packed=None, remove=True):
        if remove:
            covered = self.search_covered(network, masklen) if network and masklen else self.search_covered(network)
            for node in covered:
                try:
                    self.delete(node.prefix)
                except KeyError:
                    pass
        self.add_prefix(-2, network, masklen, packed)

    def add_prefix(self, asn, network=None, masklen=None, packed=None):
        if network and masklen:
            node = self.add(network, masklen)
        elif network:
            node = self.add(network)
        node.data['asn'] = asn
        # try:
        #     self.add(network=network, masklen=masklen, packed=packed).data['asn'] = asn
        # except TypeError:
        #     print(asn, network, masklen, packed)
        #     sys.exit(1)

    def add_private(self, inet='both', remove=True):
        if inet == 'both':
            prefixes = chain(PRIVATE4, PRIVATE6)
        elif inet == 'ipv4':
            prefixes = PRIVATE4
        else:
            prefixes = PRIVATE6
        for prefix in prefixes:
            if remove:
                for node in self.search_covered(prefix):
                    self.delete(node.prefix)
            self.add_prefix(-1, prefix)


def create_routing_table(bgp=None, ixp_prefixes=None, ixp_asns=None, bgp_compression='infer'):
    if not isinstance(bgp, pd.DataFrame):
        bgp_original = pd.read_table(bgp, comment='#', names=['Address', 'Prefixlen', 'ASN'], compression=bgp_compression)
        bgp = bgp_original[~bgp_original.ASN.str.contains(',|_')].copy()
        bgp['ASN'] = pd.to_numeric(bgp.ASN)
    if not isinstance(ixp_prefixes, pd.DataFrame):
        ixp_prefixes = pd.read_csv(ixp_prefixes, comment='#', index_col=0)
    if not isinstance(ixp_asns, pd.DataFrame):
        ixp_asns = pd.read_csv(ixp_asns, comment='#', index_col=0)
    rt = RoutingTable()
    for address, prefixlen, asn in bgp[~bgp.ASN.isin(ixp_asns.index)].itertuples(index=False):
        rt.add_prefix(asn.item(), address, prefixlen)
    for address, prefixlen, asn in bgp[bgp.ASN.isin(ixp_asns.index)].itertuples(index=False):
        rt.add_ixp(address, prefixlen)
    for prefix in ixp_prefixes.index:
        rt.add_ixp(prefix)
    rt.add_private()
    rt.add_default()
    return rt

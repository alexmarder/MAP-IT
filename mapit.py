import bz2
import gzip
import json
import socket
import struct
import sys
from argparse import ArgumentParser, FileType
from collections import defaultdict
from glob import glob
from itertools import chain
from logging import getLogger, StreamHandler, INFO
from subprocess import Popen, PIPE

import numpy as np
import pandas as pd
from radix import Radix

from algorithm import algorithm
from interface_half import InterfaceHalf

PRIVATE4 = ['0.0.0.0/8', '10.0.0.8/8', '100.64.0.0/10', '127.0.0.0/8', '169.254.0.0/16', '172.16.0.0/12',
            '192.0.0.0/24', '192.0.2.0/24', '192.31.196.0/24', '192.52.193.0/24', '192.88.99.0/24', '192.168.0.0/16',
            '192.175.48.0/24', '198.18.0.0/15', '198.51.100.0/24', '203.0.113.0/24', '240.0.0.0/4',
            '255.255.255.255/32']
PRIVATE6 = ['::1/128', '::/128', '::ffff:0:0/96', '64:ff9b::/96', '100::/64', '2001::/23', '2001::/32', '2001:1::1/128',
            '2001:2::/48', '2001:3::/32', '2001:4:112::/48', '2001:5::/32', '2001:10::/28', '2001:20::/28',
            '2001:db8::/32', '2002::/16', '2620:4f:8000::/48', 'fc00::/7', 'fe80::/10']

log = getLogger()
ch = StreamHandler(sys.stderr)
log.addHandler(ch)


class FileWrapper:
    def __init__(self, filename, read=True):
        self.filename = filename
        self.read = read

    def __enter__(self):
        ftype = self.filename.rpartition('.')[2]
        if ftype == 'gz':
            self.f = gzip.open(self.filename, 'rt' if self.read else 'wt')
        elif ftype == 'bz2':
            self.f = bz2.open(self.filename, 'rt' if self.read else 'wt')
        else:
            self.f = open(self.filename, 'r' if self.read else 'w')
        return self.f

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.f.close()
        return False


class Warts:
    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        ftype = self.filename.rpartition('.')[2]
        if ftype == 'gz':
            self.p = Popen('gzcat {} | sc_warts2json'.format(self.filename), shell=True, stdout=PIPE,
                           universal_newlines=True)
        elif ftype == 'bz2':
            self.p = Popen('bzcat {} | sc_warts2json'.format(self.filename), shell=True, stdout=PIPE,
                           universal_newlines=True)
        else:
            self.p = self.p = Popen('sc_warts2json {}'.format(self.filename), shell=True, stdout=PIPE,
                                    universal_newlines=True)
        return self.p.stdout

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.p.kill()
        return False


def cycle_free(trace):
    prev = None
    seen = set()
    for address in trace:
        if address and address != prev:
            if address in seen:
                return False
            seen.add(address)
            prev = address
    return True


def extract_trace(j):
    trace = np.full(j['hop_count'], fill_value=None, dtype='object')
    for hop in j['hops']:
        if 'icmp_q_ttl' not in hop or hop['icmp_q_ttl'] == 1:
            ttl = hop['probe_ttl'] - 1
            addr = hop['addr']
            if trace[ttl] is None:
                trace[ttl] = addr
            elif trace[ttl] != addr:
                trace[ttl] = False
    return trace


def create_adjacencies(fregex):
    adjacencies = set()
    addresses = set()
    files = glob(fregex)
    log.info('Number of files to read: {:,d}'.format(len(files)))
    for i, filename in enumerate(files, 0):
        if log.getEffectiveLevel() == INFO:
            sys.stderr.write(
                '\r\033[K{:,d} / {:,d} ({:.2%}) Adjacencies {:,d} Addresses {:,d} Reading {}'.format(
                    i, len(files), i / len(files), len(adjacencies), len(addresses), filename))
        with Warts(filename) as f:
            for line in f:
                j = json.loads(line)
                if 'hops' in j:
                    addresses.update(hop['addr'] for hop in j['hops'])
                    if j['stop_reason'] != 'LOOP':
                        trace = extract_trace(j)
                        if cycle_free(trace):
                            adjacencies.update((x, y) for x, y in zip(trace, trace[1:]) if x and y)
    log.info('\r\033[KAdjacencies \r\033[K{:,d} / {:,d} ({:.2%}) Adjacencies {:,d} Addresses {:,d}'.format(
        len(files), len(files), 1, len(adjacencies), len(addresses)))
    return adjacencies, addresses


def determine_otherside(address, all_interfaces):
    """
    Attempts to determine if an interface address in assigned from a /30 or /31 prefix.
    :param address: IPv4 interface address in dot notation
    :param all_interfaces: All known IPv4 interface addresses already converted to integers
    :return: IPv4 address in dot notation
    """
    ip = struct.unpack("!L", socket.inet_aton(address))[0]
    remainder = ip % 4
    network_address = ip - remainder
    broadcast_address = network_address + 3
    if remainder == 0:  # Definitely /31
        otherside = ip + 1
    elif remainder == 3:  # Definitely /31
        otherside = ip - 1
    elif network_address in all_interfaces or broadcast_address in all_interfaces:
        # Definitely /31 because either the network address or broadcast address was seen in interfaces
        # It's either 1 from the network address or 1 from the broadcast address
        otherside = network_address if remainder == 1 else broadcast_address
    else:
        # It's between the network and broadcast address
        # We can't be sure if it's a /30 or /31, so we assume it's a /30
        otherside = (ip + 1) if remainder == 1 else (ip - 1)
    return socket.inet_ntoa(struct.pack('!L', otherside))


def main():
    parser = ArgumentParser()
    parser.add_argument('-a', '--adjacencies', dest='adjacencies', help='Adjacencies derived from traceroutes')
    parser.add_argument('-b', '--bgp', dest='bgp', help='BGP prefixes')
    parser.add_argument('-c', '--addresses', dest='addresses', help='List of addresses')
    parser.add_argument('-f', '--factor', dest='factor', type=float, default='0', help='Factor used in the paper')
    parser.add_argument('-i', '--interfaces', dest='interfaces', help='Interface information')
    parser.add_argument('-o', '--as2org', dest='as2org', help='AS2ORG mappings')
    parser.add_argument('-p', '--providers', dest='providers', help='List of ISP ASes')
    parser.add_argument('-t', '--traces', dest='traces',
                        help='Warts traceroute files as Unix regex (can be warts.gz or warts.bz2)')
    parser.add_argument('-v', dest='verbose', action='count', default=0, help='Increase verbosity for each v')
    parser.add_argument('-w', '--output', dest='output', type=FileType('w'), default='-', help='Output filename')
    parser.add_argument('-x', '--ixp-asns', dest='ixp_asns', help='IXP ASNs')
    parser.add_argument('-y', '--ixp-prefixes', dest='ixp_prefixes', help='IXP prefixes')
    parser.add_argument('--addresses-exit', dest='addresses_exit', type=FileType('w'),
                        help='Extract addresses from traces and exit.')
    parser.add_argument('--interface-exit', dest='interface_exit', type=FileType('w'),
                        help='Extract interface info and exit')
    parser.add_argument('--trace-exit', dest='trace_exit', type=FileType('w'),
                        help='Extract adjacencies and addresses from the traceroutes and exit')
    args = parser.parse_args()

    log.setLevel(max((3 - args.verbose) * 10, 10))

    private_radix = Radix()
    ixp_radix = Radix()
    bgp_radix = Radix()

    addresses = set()

    if args.traces:
        adjacencies, addresses = create_adjacencies(args.traces)
        if args.trace_exit:
            log.info('Writing adjacencies to {}'.format(args.trace_exit))
            args.trace_exit.writelines('{}\t{}\n'.format(x, y) for x, y in sorted(adjacencies))
        if args.addresses_exit:
            log.info('Writing addresses to {}'.format(args.addresses_exit))
            args.addresses_exit.writelines('\n'.join(sorted(addresses)))
        if args.trace_exit or args.addresses_exit:
            sys.exit(0)
    else:
        log.info('Reading adjacencies from {}'.format(args.adjacencies))
        with FileWrapper(args.adjacencies) as f:
            adjacencies = {tuple(l.split()) for l in f}
    if args.addresses:
        log.info('Reading addresses from {}'.format(args.addresses))
        with FileWrapper(args.addresses) as f:
            addresses.update(l.strip() for l in f)
    neighbors = defaultdict(list)
    for x, y in adjacencies:
        neighbors[(x, True)].append(y)
        neighbors[(y, False)].append(x)
    if args.interfaces:
        df = pd.read_csv(args.interfaces, index_col='Address')
        asns, orgs, othersides = df['ASN', 'Org', 'Otherside'].to_dict('records')
    else:
        addresses = {struct.unpack("!L", socket.inet_aton(addr.strip()))[0] for addr in addresses}
        if args.ixp_asns:
            with open(args.ixp_asns) as f:
                ixp_asns = {int(asn.strip()) for asn in f}
        else:
            ixp_asns = {}
        log.info('IXP ASNs: {:,d}'.format(len(ixp_asns)))
        if args.ixp_prefixes:
            with open(args.ixp_prefixes) as f:
                for prefix in f:
                    ixp_radix.add(prefix)
        log.info('IXP Prefixes: {:,d}'.format(len(ixp_radix.prefixes())))
        for ip in chain(PRIVATE4, PRIVATE6):
            private_radix.add(ip)
        with FileWrapper(args.bgp) as f:
            for line in f:
                address, prefixlen, asn = line.split()
                try:
                    # asn = int(asn.partition('_')[0] if '_' in asn else asn)
                    asn = int(asn)
                    if asn in ixp_asns:
                        ixp_radix.add(address, int(prefixlen))
                    else:
                        bgp_radix.add(address, int(prefixlen)).data['asn'] = asn
                except ValueError:
                    log.debug('{} cannot be converted to int'.format(asn))
        bgp_radix.add('0.0.0.0/0').data['asn'] = 0
        log.info('IXP Prefixes: {:,d}'.format(len(ixp_radix.prefixes())))
        log.info('BGP Prefixes: {:,d}'.format(len(bgp_radix.prefixes())))
        if args.as2org:
            with FileWrapper(args.as2org) as f:
                as2org = {int(asn): org for asn, org in map(str.split, f)}
        else:
            as2org = None
        unique_interfaces = {u for u, _ in adjacencies} | {v for _, v in adjacencies}
        asns = {address: -2 if ixp_radix.search_best(address) else bgp_radix.search_best(address).data['asn'] for
                address in
                unique_interfaces if not private_radix.search_best(address)}
        orgs = {address: as2org.get(asn, asn) for address, asn in asns.items()} if as2org else asns
        othersides = {address: determine_otherside(address, addresses) for address in asns}
        if args.interface_exit:
            df = pd.DataFrame.from_dict({'Otherside': othersides, 'ASN': asns, 'Org': orgs}).rename_axis('Address')
            df.to_csv(args.interface_exit)
            sys.exit(0)
    halves_dict = {
        (address, direction): InterfaceHalf(address, asns[address], orgs[address], direction, othersides[address])
        for (address, direction) in neighbors if address in asns
        }
    for (address, direction), half in halves_dict.items():
        half.set_otherhalf(halves_dict.get((address, not direction)))
        half.set_otherside(halves_dict.get((half.otherside_address, not direction)))
        half.set_neighbors([halves_dict[(neighbor, not direction)] for neighbor in neighbors[(address, direction)] if
                            neighbor in asns])
    allhalves = list(halves_dict.values())
    if args.providers:
        with FileWrapper(args.providers) as f:
            providers = {int(asn.strip()) for asn in f}
    else:
        providers = None
    updates = algorithm(allhalves, threshold=args.factor, providers=providers)
    updates.write(args.output)


if __name__ == '__main__':
    main()

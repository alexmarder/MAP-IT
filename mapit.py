import json
import socket
import struct
import sys
from argparse import ArgumentParser, FileType
from collections import defaultdict
from glob import glob
from logging import getLogger, StreamHandler
from subprocess import Popen, PIPE

import numpy
import pandas as pd

from algorithm import algorithm
from as2org import AS2Org
from interface_half import InterfaceHalf
from progress import Progress, status, finish_status
from routing_table import create_routing_table
from trace import Warts, extract_trace, cycle_free, process_trace_file
from utils import File2, create_cluster, setup_parallel, stop_cluster

PRIVATE4 = ['0.0.0.0/8', '10.0.0.8/8', '100.64.0.0/10', '127.0.0.0/8', '169.254.0.0/16', '172.16.0.0/12',
            '192.0.0.0/24', '192.0.2.0/24', '192.31.196.0/24', '192.52.193.0/24', '192.88.99.0/24', '192.168.0.0/16',
            '192.175.48.0/24', '198.18.0.0/15', '198.51.100.0/24', '203.0.113.0/24', '240.0.0.0/4',
            '255.255.255.255/32']
PRIVATE6 = ['::1/128', '::/128', '::ffff:0:0/96', '64:ff9b::/96', '100::/64', '2001::/23', '2001::/32', '2001:1::1/128',
            '2001:2::/48', '2001:3::/32', '2001:4:112::/48', '2001:5::/32', '2001:10::/28', '2001:20::/28',
            '2001:db8::/32', '2002::/16', '2620:4f:8000::/48', 'fc00::/7', 'fe80::/10']

log = getLogger()
if not log.hasHandlers():
    ch = StreamHandler(sys.stderr)
    log.addHandler(ch)


def create_adjacencies(fregex, pool=None):
    adjacencies = set()
    addresses = set()
    files = glob(fregex)
    pb = Progress(len(files), 'Reading traceroutes', increment=1, callback=lambda: 'Adjacencies {:,d} Addresses {:,d}'.format(len(adjacencies), len(addresses)))
    if pool:
        p = create_cluster(pool)
        dv, lv = setup_parallel()
        with dv.sync_imports():
            import json
            import numpy
        dv['Popen'] = Popen
        dv['PIPE'] = PIPE
        dv['Warts'] = Warts
        dv['extract_trace'] = extract_trace
        dv['cycle_free'] = cycle_free
        results = lv.map_async(process_trace_file, files)
    else:
        results = map(process_trace_file, files)
    for new_adjacencies, new_addresses in pb.iterator(results):
        adjacencies.update(new_adjacencies)
        addresses.update(new_addresses)
    if pool:
        stop_cluster()
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
    parser.add_argument('-f', '--factor', dest='factor', type=float, default=0, help='Factor used in the paper')
    parser.add_argument('-i', '--interfaces', dest='interfaces', help='Interface information')
    parser.add_argument('-o', '--as2org', dest='as2org', help='AS2ORG mappings')
    parser.add_argument('-m', '--pool', dest='pool', help='Number of processes to use')
    # parser.add_argument('-p', '--asn-providers', dest='providers', help='List of ISP ASes')
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
    parser.add_argument('--potaroo', dest='potaroo', action='store_true',
                        help='Include AS identifiers and names from http://bgp.potaroo.net/cidr/autnums.html')
    parser.add_argument('--trace-exit', dest='trace_exit', type=FileType('w'),
                        help='Extract adjacencies and addresses from the traceroutes and exit')
    providers_group = parser.add_mutually_exclusive_group()
    providers_group.add_argument('-r', '--rel-graph', dest='rel_graph', help='CAIDA relationship graph')
    providers_group.add_argument('-p', '--asn-providers', dest='asn_providers', help='List of ISP ASes')
    providers_group.add_argument('-q', '--org-providers', dest='org_providers', help='List of ISP ORGs')
    parser.add_argument('--bgp-compression', dest='bgp_compression', choices=['infer', 'gzip', 'bzip2'],
                        default='infer', help='Compression passed to pandas read_table')
    args = parser.parse_args()

    log.setLevel(max((3 - args.verbose) * 10, 10))

    addresses = set()

    if args.traces:
        adjacencies, addresses = create_adjacencies(args.traces, pool=args.pool)
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
        with File2(args.adjacencies) as f:
            adjacencies = {tuple(l.split()) for l in f}
    if args.addresses:
        log.info('Reading addresses from {}'.format(args.addresses))
        with File2(args.addresses) as f:
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
        ip2as = create_routing_table(args.bgp, args.ixp_prefixes, args.ixp_asns, bgp_compression=args.bgp_compression)
        as2org = AS2Org(args.as2org, include_potaroo=args.potaroo)
        status('Extracting addresses from adjacencies')
        unique_interfaces = {u for u, _ in adjacencies} | {v for _, v in adjacencies}
        finish_status('Found {:,d}'.format(len(unique_interfaces)))
        log.info('Mapping IP addresses to ASes.')
        asns = {}
        for address in unique_interfaces:
            asn = ip2as[address]
            if asn != -1:
                asns[address] = asn
        if as2org:
            log.info('Mapping ASes to Orgs.')
            orgs = {address: as2org.get(asn, asn) for address, asn in asns.items()}
        else:
            orgs = asns
        log.info('Determining other sides for each address (assuming point-to-point).')
        othersides = {address: determine_otherside(address, addresses) for address in asns}
        if args.interface_exit:
            df = pd.DataFrame.from_dict({'Otherside': othersides, 'ASN': asns, 'Org': orgs}).rename_axis('Address')
            df.to_csv(args.interface_exit)
            sys.exit(0)
    log.info('Creating interface halves.')
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
    if args.asn_providers:
        with File2(args.providers) as f:
            providers = {int(asn.strip()) for asn in f}
    elif args.org_providers:
        with File2(args.providers) as f:
            providers = {asn.strip() for asn in f}
    elif args.rel_graph:
        rels = pd.read_csv(args.rel_graph, sep='|', comment='#', names=['AS1', 'AS2', 'Rel'], usecols=[0, 1, 2])
        providers = set(rels[rels.Rel == -1].AS1.unique())
    else:
        providers = None
    updates = algorithm(allhalves, factor=args.factor, providers=providers)
    updates.write(args.output)


if __name__ == '__main__':
    main()

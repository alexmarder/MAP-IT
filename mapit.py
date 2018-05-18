#!/usr/bin/env python
import socket
import struct
import sys
from argparse import ArgumentParser, FileType
from collections import defaultdict
from logging import getLogger, StreamHandler

import pandas as pd

from algorithm import algorithm
from as2org import AS2Org
from interface_half import InterfaceHalf
from progress import Progress, status, finish_status
from routing_table import RoutingTable
from utils import File2

log = getLogger()
if not log.hasHandlers():
    ch = StreamHandler(sys.stderr)
    log.addHandler(ch)


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


def read_adjacencies(filename):
    log.info('Reading adjacencies from {}'.format(filename))
    with File2(filename) as f:
        return {tuple(l.split()) for l in f}


def main():
    parser = ArgumentParser()
    parser.add_argument('-a', '--adjacencies', help='Adjacencies derived from traceroutes')
    parser.add_argument('-b', '--ip2as', help='BGP prefixes')
    parser.add_argument('-c', '--addresses', help='List of addresses')
    parser.add_argument('-f', '--factor', type=float, default=0, help='Factor used in the paper')
    parser.add_argument('-i', '--interfaces', dest='interfaces', help='Interface information')
    parser.add_argument('-o', '--as2org', help='AS2ORG mappings')
    parser.add_argument('-v', dest='verbose', action='count', default=0, help='Increase verbosity for each v')
    parser.add_argument('-w', '--output', type=FileType('w'), default='-', help='Output filename')
    parser.add_argument('--addresses-exit', dest='addresses_exit', type=FileType('w'), help='Extract addresses from traces and exit.')
    parser.add_argument('--potaroo', action='store_true', help='Include AS identifiers and names from http://bgp.potaroo.net/cidr/autnums.html')
    parser.add_argument('--trace-exit', type=FileType('w'), help='Extract adjacencies and addresses from the traceroutes and exit')
    providers_group = parser.add_mutually_exclusive_group()
    providers_group.add_argument('-r', '--rel-graph', help='CAIDA relationship graph')
    providers_group.add_argument('-p', '--asn-providers', help='List of ISP ASes')
    providers_group.add_argument('-q', '--org-providers', help='List of ISP ORGs')
    parser.add_argument('-I', '--iterations', type=int, default=100)
    args = parser.parse_args()

    log.setLevel(max((3 - args.verbose) * 10, 10))

    ip2as = RoutingTable.ip2as(args.ip2as)
    as2org = AS2Org(args.as2org, include_potaroo=False)

    adjacencies = read_adjacencies(args.adjacencies)
    neighbors = defaultdict(list)
    for x, y in adjacencies:
        neighbors[(x, True)].append(y)
        neighbors[(y, False)].append(x)
    status('Extracting addresses from adjacencies')
    unique_interfaces = {u for u, _ in adjacencies} | {v for _, v in adjacencies}
    finish_status('Found {:,d}'.format(len(unique_interfaces)))
    status('Converting addresses to ipnums')
    addresses = {struct.unpack("!L", socket.inet_aton(addr.strip()))[0] for addr in unique_interfaces}
    finish_status()
    log.info('Mapping IP addresses to ASes.')
    asns = {}
    for address in unique_interfaces:
        asn = ip2as[address]
        if asn != -2:
            asns[address] = asn
    if as2org:
        log.info('Mapping ASes to Orgs.')
        orgs = {address: as2org[asn] for address, asn in asns.items()}
    else:
        orgs = asns
    log.info('Determining other sides for each address (assuming point-to-point).')
    othersides = {address: determine_otherside(address, addresses) for address in asns}
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
    updates = algorithm(allhalves, factor=args.factor, providers=providers, iterations=args.iterations)
    updates.write(args.output)


if __name__ == '__main__':
    main()

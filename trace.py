import json
from subprocess import Popen, PIPE

import numpy


class Warts:
    def __init__(self, filename, json=True):
        self.filename = filename
        self.json = json

    def __enter__(self):
        ftype = self.filename.rpartition('.')[2]
        if ftype == 'gz':
            self.p = Popen('gunzip -c {} | sc_warts2json'.format(self.filename), shell=True, stdout=PIPE,
                           universal_newlines=True)
        elif ftype == 'bz2':
            self.p = Popen('bzcat {} | sc_warts2json'.format(self.filename), shell=True, stdout=PIPE,
                           universal_newlines=True)
        else:
            self.p = self.p = Popen('sc_warts2json {}'.format(self.filename), shell=True, stdout=PIPE,
                                    universal_newlines=True)
        return map(json.loads, self.p.stdout) if self.json else self.p.stdout

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
    trace = numpy.full(j['hop_count'], fill_value=None, dtype='object')
    for hop in j['hops']:
        if 'icmp_q_ttl' not in hop or hop['icmp_q_ttl'] == 1:
            ttl = hop['probe_ttl'] - 1
            addr = hop['addr']
            if trace[ttl] is None:
                trace[ttl] = addr
            elif trace[ttl] != addr:
                trace[ttl] = False
    return trace


def process_trace_file(filename):
    addresses = set()
    adjacencies = set()
    with Warts(filename) as f:
        for j in f:
            if 'hops' in j:
                addresses.update(hop['addr'] for hop in j['hops'])
                if j['stop_reason'] != 'LOOP':
                    trace = extract_trace(j)
                    if cycle_free(trace):
                        adjacencies.update((x, y) for x, y in zip(trace, trace[1:]) if x and y)
    return adjacencies, addresses
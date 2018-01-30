import json
from subprocess import Popen, PIPE

import numpy


class TraceReader:
    def __init__(self, filename, json=True):
        self.filename = filename
        self.json = json

    def __enter__(self):
        ftype = self.filename.rpartition('.')[2]
        if ftype == 'gz':
            self.p = Popen('gunzip -c {} '.format(self.filename), shell=True, stdout=PIPE,
                           universal_newlines=True)
        elif ftype == 'bz2':
            self.p = Popen('bzcat {} '.format(self.filename), shell=True, stdout=PIPE,
                           universal_newlines=True)
        else:
            self.p = self.p = Popen('cat {}'.format(self.filename), shell=True, stdout=PIPE,
                                    universal_newlines=True)
        return json.load(self.p.stdout) if self.json else self.p.stdout

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


def extract_trace(hops):
    trace = numpy.full(len(hops), fill_value=None, dtype='object')
    for hop in hops:
        if 'result' in hop:
            for reply in hop['result']: 
                ttl = hop['hop'] - 1
                if 'from' in reply and ttl<len(trace):
                    addr = reply['from']
                    if trace[ttl] is None:
                        trace[ttl] = addr
                    elif trace[ttl] != addr:
                        trace[ttl] = False
    return trace


def process_trace_file(filename):
    addresses = set()
    adjacencies = set()
    with TraceReader(filename) as f:
            for trace in f:
                if 'result' in trace:
                    for hop in trace['result']:
                        if 'result' in hop:
                            addresses.update(reply['from'] for reply in hop['result'] if 'from' in reply)
                            # there is no status code in atlas. Should check it there is a
                            # loop?
                            # if j['stop_reason'] != 'LOOP':
                    trace = extract_trace(trace['result'])
                    if cycle_free(trace):
                        adjacencies.update((x, y) for x, y in zip(trace, trace[1:]) if x and y)
    return adjacencies, addresses

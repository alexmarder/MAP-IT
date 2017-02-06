import sys
import logging


log = logging.getLogger()
if not log.hasHandlers():
    ch = logging.StreamHandler(sys.stderr)
    log.addHandler(ch)


class Progress:
    """A class for creating progress updates for long running operations."""
    def __init__(self, total=None, message='', increment=1, multiplier=1, start=0, callback=None):
        self.total = total
        self.message = message
        self.increment = increment
        self.multiplier = multiplier
        self.current = start
        self.callback = callback if callback else str

    def iterator(self, iterable):
        """Iterates over iterable and automatically updates the status at the predefined increments."""
        if should_output():
            self.show()
            i = 0
            for n in iterable:
                i += self.multiplier
                yield n
                if i == self.increment:
                    self.current += i
                    i = 0
                    self.show()
            self.current += i
            self.finish()
        else:
            yield from iterable

    def finish(self):
        self.show()
        sys.stderr.write('\n')

    def show(self):
        if self.total:
            sys.stderr.write('\r\033[K{:s} {:.2%} ({:,d} / {:,d}). {:s}'.format(self.message, self.current / self.total, self.current, self.total, self.callback()))
        else:
            sys.stderr.write('\r\033[K{:s} {:,d}. {:s}'.format(self.message, self.current, self.callback()))


def finish_status(message='Done'):
    if should_output():
        sys.stderr.write('{:s}.\n'.format(message))


def status(message):
    if should_output():
        sys.stderr.write('\r\033[K{:s} '.format(message))


def cstatus(message):
    if should_output():
        sys.stderr.write(message)


def should_output():
    return logging.getLogger().getEffectiveLevel() <= logging.INFO

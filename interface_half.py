from collections import namedtuple

IH = namedtuple('InterfaceHalf', ['address', 'otherside', 'direction', 'asn', 'org', 'neighbors'])


class InterfaceHalf:
    """
    An interface half (the interface in either the forward or backward direction)
    """

    __slots__ = (
        'address', 'otherhalf', 'otherside', 'asn', 'org', 'direction', 'neighbors', 'identifier', 'hash_value',
        'num_neighbors', 'otherside_address', 'otherside2_address', 'neighbors_addresses')

    def __init__(self, address, asn, org, direction, otherside):
        self.address = address
        self.otherhalf = None
        self.otherside = None
        self.asn = asn
        self.org = org
        self.direction = direction
        self.otherside_address = otherside
        self.neighbors = None
        self.num_neighbors = 0
        self.identifier = (self.address, self.direction)
        self.hash_value = hash(self.identifier)

    def __dict__(self):
        return {slot: getattr(self, slot) for slot in InterfaceHalf.__slots__}

    def __eq__(self, other):
        return self.identifier == other

    def __hash__(self):
        return self.hash_value

    def __repr__(self):
        return 'InterfaceHalf{}'.format(str(self.identifier))

    def set_neighbors(self, neighbors):
        self.neighbors = neighbors
        self.num_neighbors = len(neighbors)

    def set_otherhalf(self, half):
        self.otherhalf = half

    def set_otherside(self, half):
        self.otherside = half

    def tuple(self):
        return IH(self.address, self.otherside_address, self.direction, self.asn, self.org,
                  tuple(self.neighbors_addresses))

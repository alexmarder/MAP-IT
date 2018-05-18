from collections import defaultdict
from logging import getLogger

import numpy as np

from updates import Updates

log = getLogger()


def max2(iterable, key=lambda x: x):
    first = None
    second = None
    first_value = np.NINF
    second_value = np.NINF
    for v in iterable:
        n = key(v)
        if n > first_value:
            second = first
            second_value = first_value
            first = v
            first_value = n
        elif n > second_value:
            second = v
            second_value = n
    return first, first_value, second, second_value


def connected_org(half, updates, f):
    orgs = defaultdict(list)
    for neighbor in half.neighbors:
        if neighbor in updates:
            asn, org = updates.mapping(neighbor)
            orgs[org].append(asn)
        else:
            orgs[neighbor.org].append(neighbor.asn)
    org, first, _, second = max2(orgs, key=lambda x: len(orgs[x]))
    if len(orgs) == 1 or (first != second and first > half.num_neighbors * f):
        asns = defaultdict(int)
        for asn in orgs[org]:
            asns[asn] += 1
        asn = max(asns, key=lambda x: asns[x])
        return asn, org


def add_borders(halves, updates, f):
    new_updates = updates.copy()
    for half in halves:
        if not updates.isdirect(half):
            if half.asn != -2 or half.direction:
                network = connected_org(half, updates, f)
                if network:
                    asn, org = network
                    if org != half.org and asn != -2:
                        new_updates.update(half, asn, org, True)
    return new_updates


def add_othersides(new_updates):
    for half in new_updates.direct:
        if half.asn != -2 and half.otherside and not new_updates.isdirect(half.otherside):
            new_updates.update(half.otherside, new_updates.asn(half), new_updates.org(half), False)


def resolve_direct(forward_half, backward_half, forward_asn, new_updates):
    if forward_asn == 0:
        remove_half = forward_half
    else:
        remove_half = backward_half
    if not new_updates.isdirect(remove_half.otherside) or new_updates.asn(remove_half) == 0:
        new_updates.remove(remove_half)
        if remove_half.otherside:
            new_updates.remove(remove_half.otherside)


def resolve_indirect(direct_half, indirect_half, new_updates):
    remove_half = direct_half if new_updates.asn(direct_half) == 0 else indirect_half
    new_updates.remove(remove_half)
    # Assume the other sides were assigned incorrectly and keep the direct inference on the indirect half's other side.
    # Also, remove the indirect inference belonging to the direct half's other side.
    if remove_half.otherside and not new_updates.isdirect(remove_half.otherside):
        new_updates.remove(remove_half.otherside)


def dual_inferences(new_updates):
    # Only need to search through current updates
    for half in [half for half in new_updates if half.direction and half.otherhalf in new_updates and half.asn > 0]:
        if half in new_updates and half.otherhalf in new_updates:
            # Only need to look through the forward halves because we are looking for updates on both halves
            forward_asn, forward_org = new_updates.mapping(half)
            backward_asn, backward_org = new_updates.mapping(half.otherhalf)
            if forward_org != backward_org:
                if new_updates.isdirect(half) and new_updates.isdirect(half.otherhalf):
                    resolve_direct(half, half.otherhalf, forward_asn, new_updates)
                elif new_updates.isdirect(half):
                    resolve_indirect(half, half.otherhalf, new_updates)
                elif new_updates.isdirect(half.otherhalf):
                    resolve_indirect(half.otherhalf, half, new_updates)


def is_inverse(half, neighbor, updates):
    return half.org == updates.org_default(neighbor, None) and updates.org(half) == neighbor.org


def inverse_inferences(new_updates):
    for half in tuple(new_updates.direct):
        if not half.direction and not new_updates.isdirect(half.otherside):
            for neighbor in half.neighbors:
                if is_inverse(half, neighbor, new_updates):
                    new_updates.remove(half)
                    if half.otherside:
                        new_updates.remove(half.otherside)
                    break


def create_rerun(updates, new_updates):
    return {neighbor for half in new_updates.difference(updates) if half for neighbor in half.neighbors if
            neighbor.num_neighbors > 1}


def add_step(halves, updates, threshold):
    previous = []
    while True:
        new_updates = add_borders(halves, updates, threshold)
        log.info('Direct: {:,d} inferences'.format(len(new_updates)))
        # if new_updates.direct == updates.direct:
        #     return new_updates
        add_othersides(new_updates)
        log.info('Indirect: {:,d} inferences'.format(len(new_updates)))
        dual_inferences(new_updates)
        log.info('Dual: {:,d} inferences'.format(len(new_updates)))
        inverse_inferences(new_updates)
        log.info('Inverse: {:,d} inferences'.format(len(new_updates)))
        halves = create_rerun(updates, new_updates)
        if updates in previous:
            return updates
        previous.append(updates)
        updates = new_updates.copy()


def discard_update(half, updates):
    if half.otherside and updates.isdirect(half.otherside):
        updates.direct.remove(half)
    else:
        updates.remove(half)
        if half.otherside:
            updates.remove(half.otherside)


def remove_borders(updates, threshold):
    new_updates = updates.copy()
    for half in updates.direct:
        network = connected_org(half, updates, threshold)
        if network:
            _, org = network
            if org != updates[half]:
                discard_update(half, new_updates)
        else:
            discard_update(half, new_updates)
    return new_updates


def remove_step(updates, factor):
    """
    The remove step discards inferences which no longer appear valid.

    This step will continue until there are no changes left to be made.
    :param updates: Updates object with current inferences
    :param factor: 0 <= factor <= 1
    :return: Updates object without discarded inferences
    """
    while True:
        new_updates = remove_borders(updates, factor)
        log.info('Remove: {:,d} inferences'.format(len(new_updates)))
        if updates == new_updates:
            return updates
        updates = new_updates


def stub_heuristic(allhalves, updates, providers):
    """
    Infers ISP->Stub links when the stub AS responds with only a single address following the link.

    Here, we assume that links from an ISP to a stub AS are typically assigned from the ISP's address space.
    There are two primary reasons for this heuristic. The first is that in our experiments only a single stub AS address
    appeared following an ISP link. The second is that we are no longer concerned about third party addresses because a
    stub AS can't appear as a third party address. This heuristic can increase the coverage by allowing MAP-IT to infer
    links with only a single neighbor.
    :param allhalves: All InterfaceHalf objects
    :param updates: The current Update object after completing the main loop which will be directly modified
    :param providers: Set of ISP ASNs
    """
    for half in allhalves:
        # Only need to look at IHs in forward direction with a single neighbor
        if half.direction and half.num_neighbors == 1:
            if half.address == '210.7.39.2':
                print('here')
            # If not inference for half and it's other half
            # If it has an IP2AS mapping
            if half not in updates and half.otherhalf not in updates and half.asn != 0:
                neighbor = half.neighbors[0]
                # If the neighbor has an IP2AS mapping and is not the same ORG as the half
                # If there is no inference for the neighbor and the neighbor is not an ISP
                if neighbor.asn > 0 and neighbor.org != half.org and neighbor not in updates and (
                                neighbor.asn not in providers and neighbor.org not in providers):
                    updates.update(half, neighbor.asn, neighbor.org, isdirect=True, isstub=True)
                    if half.otherside:
                        updates.update(half.otherside, neighbor.asn, neighbor.org, isdirect=False, isstub=True)


def algorithm(allhalves, factor=0.5, providers=None, iterations=100):
    """
    The main MAP-IT algorithm, with the main loop which calls the add step and the remove step.
    :param allhalves: All InterfaceHalf objects created from the traceroutes, including those with 1 neighbor
    :param factor: 0 <= factor <= 1
    :param providers: Set of ISP ASNs
    :return: Updates object with the final set of inter-AS links
    """
    previous_updates = []
    updates = Updates()
    halves = [half for half in allhalves if half.num_neighbors > 1]
    if not halves:
        log.warning('The interface graph is too sparse. No interface has more than one neighbor in the forward or backward direction.')
        log.warning('Only applying the stub heuristic.')
    for iteration in range(iterations):
        log.info('***** Iteration {} *****'.format(iteration))
        updates = add_step(halves, updates, factor)
        updates = remove_step(updates, factor)
        if updates in previous_updates:
            break
        previous_updates.append(updates)
        iteration += 1
    if providers is not None:
        stub_heuristic(allhalves, updates, providers)
        log.info('Stubs Heuristic: Added {:,d} Total {:,d}'.format(len(updates.stubs), len(updates)))
    return updates

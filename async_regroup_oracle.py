"""Exact D-only micro-oracle for a reachable asynchronous cloud state.

The state is deliberately small and synthetic.  Request 0 has completed its
UP transfer and is ready on C0; requests 1 and 2 are D-PRE ready and were
permanently assigned to C0/C1.  This state is legal after P-POST/XDN frames.
It isolates whether a new D-PRE group should account for a *currently queued*
D-PROC member on C0.  No future arrivals or hidden Lout information is used by
the online policies.  The DFS is offline and finds a makespan optimum only.
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations, product


READY, UP, PROC, DOWN, POST, RUN_PRE, RUN_PROC, RUN_POST, FIN = range(9)


@dataclass(frozen=True)
class State:
    t: int
    phase: tuple[int, ...]
    edge: bool
    cloud: tuple[bool, bool]
    up_tail: int
    down_tail: int
    events: tuple[tuple[int, int, str, tuple[int, ...]], ...]
    serial: int
    tokens: tuple[int, ...]


REMOTE = (0, 0, 1)
# All values include setup S.  Fixed batch cost makes D-PROC regrouping useful,
# while the C0 backlog makes a synchronous "one per cloud" prediction wrong.
PRE = {1: 2, 2: 2}
PROC_TIME = {1: 16, 2: 16}
POST = {1: 4, 2: 4, 3: 4}
XFER = 8


def init(post_first: bool = False) -> State:
    # Request 0 is already D-PROC ready on C0.  Requests 1/2 are D-PRE ready.
    phases = (POST, READY, READY) if post_first else (PROC, READY, READY)
    return State(0, phases, True, (True, True), 0, 0, (), 0, (-1, -1, -1))


def push(st: State, when: int, kind: str, ids: tuple[int, ...]) -> State:
    return State(st.t, st.phase, st.edge, st.cloud, st.up_tail, st.down_tail,
                 tuple(sorted(st.events + ((when, st.serial, kind, ids),))),
                 st.serial + 1, st.tokens)


def advance(st: State) -> State | None:
    if not st.events:
        return None
    t = st.events[0][0]
    now = [e for e in st.events if e[0] == t]
    later = list(e for e in st.events if e[0] != t)
    phase = list(st.phase); cloud = list(st.cloud); edge = st.edge
    up, down, serial = st.up_tail, st.down_tail, st.serial
    tokens = list(st.tokens)

    def add(when: int, kind: str, ids: tuple[int, ...]) -> None:
        nonlocal serial
        later.append((when, serial, kind, ids)); serial += 1

    for _, _, kind, ids in now:
        if kind == "PRE_DONE":
            edge = True
            # A D-PRE completion releases one automatic UP transfer per
            # represented cloud, in increasing cloud order.
            for k in (0, 1):
                group = tuple(i for i in ids if REMOTE[i] == k)
                if group:
                    up = max(up, t) + XFER
                    add(up, "UP_DONE", group)
        elif kind == "UP_DONE":
            for i in ids: phase[i] = PROC
        elif kind == "PROC_DONE":
            k = REMOTE[ids[0]]; cloud[k] = True
            for i in ids: phase[i] = DOWN
            down = max(down, t) + XFER
            add(down, "DOWN_DONE", ids)
        elif kind == "DOWN_DONE":
            for i in ids: phase[i] = POST
        elif kind == "POST_DONE":
            edge = True
            for i in ids:
                phase[i] = FIN; tokens[i] = t
        else:
            raise AssertionError(kind)
    return State(t, tuple(phase), edge, tuple(cloud), up, down,
                 tuple(sorted(later)), serial, tuple(tokens))


def start(st: State, e: tuple[str, tuple[int, ...]] | None,
          cs: tuple[tuple[int, tuple[int, ...]] | None, ...]) -> State:
    phase = list(st.phase); edge = st.edge; cloud = list(st.cloud)
    out = st
    if e is not None:
        op, ids = e
        assert edge
        if op == "PRE":
            assert all(phase[i] == READY for i in ids)
            for i in ids: phase[i] = RUN_PRE
            out = push(out, st.t + PRE[len(ids)], "PRE_DONE", ids)
        else:
            assert all(phase[i] == POST for i in ids)
            for i in ids: phase[i] = RUN_POST
            out = push(out, st.t + POST[len(ids)], "POST_DONE", ids)
        edge = False
    # One D PROC can start on every free cloud in the same interaction
    # response.  They are independent, so processing the tuple in cloud order
    # is just a compact representation of simultaneous assignments.
    for c in cs:
        if c is None:
            continue
        k, ids = c
        assert cloud[k] and ids and all(phase[i] == PROC and REMOTE[i] == k for i in ids)
        for i in ids: phase[i] = RUN_PROC
        out = push(out, st.t + PROC_TIME[len(ids)], "PROC_DONE", ids)
        cloud[k] = False
    return State(st.t, tuple(phase), edge, tuple(cloud), out.up_tail, out.down_tail,
                 out.events, out.serial, st.tokens)


def greedy(st: State, first_pre: tuple[int, ...] | None) -> State:
    """Current immediate service policy, varying only its initial D-PRE choice."""
    first = True
    while True:
        # Start any legal non-idle work.  At t=0 edge D-PRE selection models
        # chooseThroughputWave; clouds independently start current D-PROC work.
        e = c = None
        if st.edge:
            post = tuple(i for i, p in enumerate(st.phase) if p == POST)
            ready = tuple(i for i, p in enumerate(st.phase) if p == READY)
            e = ("POST", post) if post else (("PRE", first_pre if first else ready) if ready else None)
        for k in (0, 1):
            q = tuple(i for i, p in enumerate(st.phase) if p == PROC and REMOTE[i] == k)
            if st.cloud[k] and q:
                # start all current members; only one cloud action can be
                # encoded per call, so process the other in another same-time
                # scheduling pass below.
                c = (k, q); break
        if e is not None or c is not None:
            st = start(st, e, (c,))
            if e is not None and e[0] == "PRE": first = False
            # if the other free cloud also has work, it is scheduled without
            # advancing time, exactly like assignments in one interaction frame.
            continue
        st = advance(st)
        if st is None:
            assert all(p == FIN for p in st.phase)  # pragma: no cover
        if all(p == FIN for p in st.phase): return st


def options(st: State):
    es = [None]
    if st.edge:
        for wanted, op in ((READY, "PRE"), (POST, "POST")):
            q = tuple(i for i, p in enumerate(st.phase) if p == wanted)
            es += [(op, x) for n in range(1, len(q)+1) for x in combinations(q, n)]
    per_cloud = []
    for k in (0, 1):
        choices = [None]
        if st.cloud[k]:
            q = tuple(i for i, p in enumerate(st.phase) if p == PROC and REMOTE[i] == k)
            choices += [(k, x) for n in range(1, len(q)+1) for x in combinations(q, n)]
        per_cloud.append(choices)
    return es, list(product(*per_cloud))


def exact(start_state: State | None = None) -> tuple[State, tuple[tuple[int, tuple[str, tuple[int, ...]] | None,
                                     tuple[tuple[int, tuple[int, ...]] | None, ...]], ...]]:
    best: State | None = None
    best_plan = ()
    def dfs(st: State, plan: tuple) -> None:
        nonlocal best, best_plan
        if all(p == FIN for p in st.phase):
            if best is None or max(st.tokens) < max(best.tokens):
                best, best_plan = st, plan
            return
        if best is not None and st.t >= max(best.tokens): return
        es, cs = options(st)
        made = False
        for e, cs in product(es, cs):
            if e is None and all(c is None for c in cs):
                continue
            made = True; dfs(start(st, e, cs), plan + ((st.t, e, cs),))
        if not made:
            nxt = advance(st)
            if nxt is not None: dfs(nxt, plan)
    dfs(init() if start_state is None else start_state, ())
    assert best is not None
    return best, best_plan


def two_wave_choice(reserve_same_frame_dproc: bool) -> tuple[int, ...]:
    """First D-PRE group selected by v36's two-wave model in ``init()``.

    The legacy calculation lets a just-ready C0 D-PROC task disappear from
    its prediction, although the real response will start it immediately.
    The reserved version first accounts for that deterministic cloud and DOWN
    work.  Both versions use only the visible state at t=0.
    """
    ready = ((1,), (2,))
    n = 2
    cloud_tail = [0, 0]
    down_tail = 0
    if reserve_same_frame_dproc:
        # ``greedy``/the production dispatcher starts job 0's one-member
        # D-PROC task in this very interaction response.
        cloud_tail[0] = PROC_TIME[1]
        down_tail = cloud_tail[0] + XFER

    best_pick: tuple[int, ...] = ()
    best_score = float("inf")
    for b in (1, 2):
        for mask in range(1, 4):
            if sum(len(ready[k]) for k in range(2) if mask & (1 << k)) < b:
                continue
            take = [0, 0]
            for _ in range(b):
                candidates = [k for k in range(2)
                              if mask & (1 << k) and take[k] < len(ready[k])]
                k = min(candidates, key=lambda x: (cloud_tail[x] + PROC_TIME[take[x] + 1], take[x], x))
                take[k] += 1

            edge1 = PRE[b]
            up = edge1
            up1 = [0, 0]
            for k in range(2):
                if take[k]:
                    up += XFER
                    up1[k] = up
            cloud1 = [cloud_tail[k] for k in range(2)]
            done1 = []
            for k in range(2):
                if take[k]:
                    cloud1[k] = max(cloud_tail[k], up1[k]) + PROC_TIME[take[k]]
                    done1.append((cloud1[k], k))
            down1 = down_tail
            for done, k in sorted(done1):
                down1 = max(down1, done) + XFER

            rem = [len(ready[k]) - take[k] for k in range(2)]
            rb = n - b
            if rb:
                edge2 = edge1 + PRE[rb]
                up2 = max(up, edge2)
                done2 = []
                for k in range(2):
                    if rem[k]:
                        up2 += XFER
                        done2.append((max(cloud1[k], up2) + PROC_TIME[rem[k]], k))
                down2 = down1
                for done, k in sorted(done2):
                    down2 = max(down2, done) + XFER
                post1 = max(edge2, down1) + POST[b]
                post2 = max(post1, down2) + POST[rb]
                score = post2 + 0.001 * (b * post1 + rb * post2) / n
            else:
                score = max(edge1, down1) + POST[b]

            pick = tuple(ready[k][i] for k in range(2) for i in range(take[k]))
            if score < best_score - 1e-12 or (abs(score - best_score) <= 1e-12 and len(pick) > len(best_pick)):
                best_score, best_pick = score, pick
    return best_pick


if __name__ == "__main__":
    # Current chooseThroughputWave sees identical cloudTail values and selects
    # {1,2}; a queue-aware decoupled selector sends only C1's member first.
    for name, choice in (("synchronous wave prediction", (1, 2)),
                         ("queue-aware asynchronous choice", (2,))):
        s = greedy(init(), choice)
        print(name, "tokens=", s.tokens, "makespan=", max(s.tokens))
    o, plan = exact()
    print("offline exact", "tokens=", o.tokens, "makespan=", max(o.tokens))
    print("offline exact plan=", plan)

    # A separate one-cloud, one-decision exact micro-state for P-PROC
    # interleaving.  A decode upload is already committed and will arrive at
    # t=3; the cloud is free with a 64-layer P-PROC ready.  The only legal
    # choices now are an integer nonempty prefix [0, le), or idling until the
    # guaranteed XDN frame.  An overlong prefix cannot advance P completion
    # before that XDN but delays the ready D-PROC task.
    layers, setup, pproc_total, decode_xdn = 64, 1.0, 100.0, 3.0
    candidates = [(le, setup + pproc_total * le / layers)
                  for le in range(1, layers + 1)]
    exact_prefix = min(candidates, key=lambda x: max(decode_xdn, x[1]))
    v34_equal_four = (layers // 4, setup + pproc_total / 4)
    print("P-PROC exact prefix", exact_prefix,
          "v34 equal-4 prefix", v34_equal_four,
          "D-PROC release exact/v34=",
          max(decode_xdn, exact_prefix[1]), max(decode_xdn, v34_equal_four[1]))

    # D-POST and D-PRE are both edge tasks.  v34 always starts D-POST first;
    # this legal state checks whether that precedence can starve the next
    # cloud launch.  Request 0's result is already down-ready, while 1/2 are
    # D-PRE ready on C0/C1.
    post_state = init(True)
    current_post_first = greedy(post_state, (1, 2))
    optimal_post, optimal_post_plan = exact(post_state)
    print("D-POST-first tokens/makespan=", current_post_first.tokens, max(current_post_first.tokens))
    print("D-POST exact tokens/makespan=", optimal_post.tokens, max(optimal_post.tokens),
          "first response=", optimal_post_plan[:2])

    # Exhaustive table-shape sweep for the asynchronous launch counterexample.
    # The two legal online candidates are: one cross-cloud D-PRE wave, or a
    # C1-only launch followed by C0 at the next D-PRE TDN.  Selecting the
    # better of their deterministic rollouts equals the exact small-state
    # optimum throughout this family; always selecting either one does not.
    old = PRE.copy(), PROC_TIME.copy(), POST.copy(), XFER
    one_wins = split_wins = ties = rollout_misses = 0
    for xfer in (1, 2, 4, 8):
        for pre in (1, 2, 4, 8):
            for proc in (2, 4, 8, 16):
                for post in (1, 2, 4, 8):
                    PRE = {1: pre, 2: pre}; PROC_TIME = {1: proc, 2: proc}
                    POST = {1: post, 2: post, 3: post}; XFER = xfer
                    one = max(greedy(init(), (1, 2)).tokens)
                    split = max(greedy(init(), (2,)).tokens)
                    optimum = max(exact()[0].tokens)
                    if one < split: one_wins += 1
                    elif split < one: split_wins += 1
                    else: ties += 1
                    rollout_misses += min(one, split) != optimum
    PRE, PROC_TIME, POST, XFER = old
    print("two-launch sweep one/split/tie/misses=",
          one_wins, split_wins, ties, rollout_misses)

    # The exact production-model regression: reserve the same-frame C0
    # D-PROC/DOWN commitment before selecting the next D-PRE wave.
    old = PRE.copy(), PROC_TIME.copy(), POST.copy(), XFER
    legacy_misses = reserved_misses = 0
    for xfer in (1, 2, 4, 8):
        for pre in (1, 2, 4, 8):
            for proc in (2, 4, 8, 16):
                for post in (1, 2, 4, 8):
                    PRE = {1: pre, 2: pre}; PROC_TIME = {1: proc, 2: proc}
                    POST = {1: post, 2: post, 3: post}; XFER = xfer
                    optimum = max(exact()[0].tokens)
                    legacy_misses += max(greedy(init(), two_wave_choice(False)).tokens) != optimum
                    reserved_misses += max(greedy(init(), two_wave_choice(True)).tokens) != optimum
    PRE, PROC_TIME, POST, XFER = old
    print("v36 two-wave legacy/reserved misses=", legacy_misses, reserved_misses,
          "choices=", two_wave_choice(False), two_wave_choice(True))

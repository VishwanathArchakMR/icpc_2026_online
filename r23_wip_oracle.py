"""Synthetic, protocol-faithful R<=3 replay for the WIP-admission experiment.

It models the six legal task shapes, automatic FIFO UP/DOWN transfers, full-frame
event decisions, permanent cloud assignment, and unknown-Lout-compatible online
policies.  The exhaustive solver is offline only and is used to discover the
small-instance optimum.  P PROC is intentionally one full legal piece here: this
isolates the fresh-DPRE WIP decision from splitting.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from itertools import combinations, product


ARR, P_UP, P_PROC, P_DOWN, P_POST, D_READY, D_UP, D_PROC, D_DOWN, D_POST, FIN, DPOST_RUN = range(12)


@dataclass(frozen=True)
class Spec:
    r: int
    lout: tuple[int, ...]
    arrivals: tuple[int, ...]
    s: int = 1
    transfer: int = 1
    ppre: int = 1
    pproc: int = 1
    ppost: int = 1
    # Values exclude S, exactly as in the contest table.
    dpre1: int = 5
    dpre2: int = 5
    dpre3: int = 5
    dproc1: int = 5
    dproc2: int = 5
    dproc3: int = 5
    dpost1: int = 5
    dpost2: int = 5
    dpost3: int = 5

    def task(self, op: str, b: int) -> int:
        # P-stage costs are scalar; D-stage costs are indexed by legal batch
        # cardinality.  Every invocation includes the fixed setup S.
        value = getattr(self, op) if op.startswith("p") else getattr(self, op + str(min(3, b)))
        return self.s + value


@dataclass(frozen=True)
class State:
    t: int
    phase: tuple[int, ...]
    it: tuple[int, ...]
    produced: tuple[tuple[int, ...], ...]
    tdr: tuple[int, ...]
    edge_free: bool
    cloud_free: bool
    up_free: int
    down_free: int
    # (time, serial, kind, ids), where kind is a task completion or XDN.
    events: tuple[tuple[int, int, str, tuple[int, ...]], ...]
    serial: int


def initial(spec: Spec) -> State:
    events = tuple(sorted((a, i, "ARR", (i,)) for i, a in enumerate(spec.arrivals)))
    return State(0, (FIN,) * spec.r, (0,) * spec.r, tuple(() for _ in range(spec.r)),
                 (-1,) * spec.r, True, True, 0, 0, events, spec.r)


def add_event(st: State, when: int, kind: str, ids: tuple[int, ...]) -> State:
    event = (when, st.serial, kind, ids)
    return State(st.t, st.phase, st.it, st.produced, st.tdr, st.edge_free, st.cloud_free,
                 st.up_free, st.down_free, tuple(sorted(st.events + (event,))), st.serial + 1)


def advance(st: State, spec: Spec) -> State | None:
    if not st.events:
        return None
    t = st.events[0][0]
    now = [e for e in st.events if e[0] == t]
    later = tuple(e for e in st.events if e[0] != t)
    phase = list(st.phase); it = list(st.it); produced = [list(x) for x in st.produced]
    tdr = list(st.tdr); edge_free = st.edge_free; cloud_free = st.cloud_free
    up_free, down_free, serial = st.up_free, st.down_free, st.serial
    events = list(later)

    def push(when: int, kind: str, ids: tuple[int, ...]) -> None:
        nonlocal serial
        events.append((when, serial, kind, ids)); serial += 1

    # A whole frame is processed before any decision.  Ordering inside a frame
    # cannot create a same-frame dependency, so all effects are collected first.
    for _, _, kind, ids in now:
        if kind == "ARR":
            phase[ids[0]] = ARR
        elif kind == "TDN_PPRE":
            edge_free = True
            i = ids[0]; phase[i] = P_UP
            begin = max(t, up_free); up_free = begin + spec.transfer
            push(up_free, "XUP_P", (i,))
        elif kind == "XUP_P":
            phase[ids[0]] = P_PROC
        elif kind == "TDN_PPROC":
            cloud_free = True
            i = ids[0]; phase[i] = P_DOWN
            begin = max(t, down_free); down_free = begin + spec.transfer
            push(down_free, "XDOWN_P", (i,))
        elif kind == "XDOWN_P":
            phase[ids[0]] = P_POST
        elif kind == "TDN_PPOST":
            edge_free = True
            i = ids[0]; phase[i] = D_READY; tdr[i] = t
        elif kind == "TDN_DPRE":
            edge_free = True
            for i in ids:
                phase[i] = D_UP
            begin = max(t, up_free); up_free = begin + spec.transfer
            push(up_free, "XUP_D", ids)
        elif kind == "XUP_D":
            for i in ids:
                phase[i] = D_PROC
        elif kind == "TDN_DPROC":
            cloud_free = True
            for i in ids:
                phase[i] = D_DOWN
            begin = max(t, down_free); down_free = begin + spec.transfer
            push(down_free, "XDOWN_D", ids)
        elif kind == "XDOWN_D":
            for i in ids:
                phase[i] = D_POST
        elif kind == "TDN_DPOST":
            edge_free = True
            for i in ids:
                produced[i].append(t); it[i] += 1
                phase[i] = FIN if it[i] == spec.lout[i] else D_READY
        else:
            raise AssertionError(kind)
    return State(t, tuple(phase), tuple(it), tuple(tuple(x) for x in produced), tuple(tdr),
                 edge_free, cloud_free, up_free, down_free, tuple(sorted(events)), serial)


def start(st: State, spec: Spec, edge: tuple[str, tuple[int, ...]] | None,
          cloud: tuple[str, tuple[int, ...]] | None) -> State:
    phase = list(st.phase); events = list(st.events); serial = st.serial
    edge_free, cloud_free = st.edge_free, st.cloud_free

    def push(when: int, kind: str, ids: tuple[int, ...]) -> None:
        nonlocal serial
        events.append((when, serial, kind, ids)); serial += 1

    if edge is not None:
        op, ids = edge; assert edge_free
        if op == "PPRE":
            i = ids[0]; assert phase[i] == ARR
            phase[i] = P_UP; push(st.t + spec.task("ppre", 1), "TDN_PPRE", ids)
        elif op == "PPOST":
            i = ids[0]; assert phase[i] == P_POST
            phase[i] = D_READY; push(st.t + spec.task("ppost", 1), "TDN_PPOST", ids)
        elif op == "DPRE":
            assert ids and all(phase[i] == D_READY for i in ids)
            for i in ids: phase[i] = D_UP
            push(st.t + spec.task("dpre", len(ids)), "TDN_DPRE", ids)
        elif op == "DPOST":
            assert ids and all(phase[i] == D_POST for i in ids)
            for i in ids: phase[i] = DPOST_RUN
            push(st.t + spec.task("dpost", len(ids)), "TDN_DPOST", ids)
        else: raise AssertionError(op)
        edge_free = False
    if cloud is not None:
        op, ids = cloud; assert cloud_free
        if op == "PPROC":
            i = ids[0]; assert phase[i] == P_PROC
            phase[i] = P_DOWN; push(st.t + spec.task("pproc", 1), "TDN_PPROC", ids)
        elif op == "DPROC":
            assert ids and all(phase[i] == D_PROC for i in ids)
            for i in ids: phase[i] = D_DOWN
            push(st.t + spec.task("dproc", len(ids)), "TDN_DPROC", ids)
        else: raise AssertionError(op)
        cloud_free = False
    return State(st.t, tuple(phase), st.it, st.produced, st.tdr, edge_free, cloud_free,
                 st.up_free, st.down_free, tuple(sorted(events)), serial)


def policy_actions(st: State, spec: Spec, new: bool) -> tuple[tuple[str, tuple[int, ...]] | None, tuple[str, tuple[int, ...]] | None]:
    ppost = tuple(i for i, x in enumerate(st.phase) if x == P_POST)
    dpost = tuple(i for i, x in enumerate(st.phase) if x == D_POST)
    dready = tuple(i for i, x in enumerate(st.phase) if x == D_READY)
    admit = tuple(i for i, x in enumerate(st.phase) if x == ARR)
    fresh = sum(st.it[i] == 0 for i in dready)
    # This harness fixes batchingMaterial=true, targetWip=2, and deliberately
    # makes urgency false.  It is an exact test of the disputed condition.
    population = sum(x not in (ARR, FIN) for x in st.phase)
    underfilled = population < 2 and (new or fresh == 0)
    edge = None
    if st.edge_free:
        if ppost: edge = ("PPOST", (ppost[0],))
        elif dpost: edge = ("DPOST", dpost)
        elif dready or admit:
            if not dready or (admit and underfilled): edge = ("PPRE", (admit[0],))
            elif dready: edge = ("DPRE", dready)
    pready = tuple(i for i, x in enumerate(st.phase) if x == P_PROC)
    dproc = tuple(i for i, x in enumerate(st.phase) if x == D_PROC)
    cloud = None
    if st.cloud_free:
        if not dproc and pready: cloud = ("PPROC", (pready[0],))
        elif not pready and dproc: cloud = ("DPROC", dproc)
        elif pready and dproc: cloud = ("PPROC", (pready[0],)) if underfilled else ("DPROC", dproc)
    return edge, cloud


def run_policy(spec: Spec, new: bool) -> State:
    st = initial(spec)
    while True:
        st = advance(st, spec)
        if st is None: raise AssertionError("stuck")
        if all(x == FIN for x in st.phase): return st
        edge, cloud = policy_actions(st, spec, new)
        if edge or cloud: st = start(st, spec, edge, cloud)


def options(st: State) -> tuple[list[tuple[str, tuple[int, ...]] | None], list[tuple[str, tuple[int, ...]] | None]]:
    e = [None]; c = [None]
    if st.edge_free:
        for code, wanted, single in (("PPRE", ARR, True), ("PPOST", P_POST, True), ("DPRE", D_READY, False), ("DPOST", D_POST, False)):
            ids = tuple(i for i, x in enumerate(st.phase) if x == wanted)
            if single:
                e += [(code, (i,)) for i in ids]
            else:
                e += [(code, q) for n in range(1, len(ids)+1) for q in combinations(ids, n)]
    if st.cloud_free:
        p = tuple(i for i, x in enumerate(st.phase) if x == P_PROC)
        d = tuple(i for i, x in enumerate(st.phase) if x == D_PROC)
        c += [("PPROC", (i,)) for i in p]
        c += [("DPROC", q) for n in range(1, len(d)+1) for q in combinations(d, n)]
    return e, c


def score_key(st: State) -> tuple[float, float, float]:
    makespan = max(t for xs in st.produced for t in xs)
    tdr = sum(st.tdr) / len(st.tdr)
    gaps = [b-a for xs in st.produced for a, b in zip(xs, xs[1:])]
    return makespan, tdr, sum(gaps) / len(gaps) if gaps else 0.0


def exact(spec: Spec) -> State:
    best: State | None = None
    seen: dict[State, tuple[float, float, float]] = {}
    def dfs(st: State) -> None:
        nonlocal best
        st = advance(st, spec)
        if st is None: return
        if all(x == FIN for x in st.phase):
            if best is None or score_key(st) < score_key(best): best = st
            return
        if best is not None and st.t >= score_key(best)[0]: return
        eopts, copts = options(st)
        progressed = False
        for e, c in product(eopts, copts):
            if e is None and c is None: continue
            progressed = True; dfs(start(st, spec, e, c))
        if not progressed and st.events: dfs(st)
    dfs(initial(spec))
    assert best is not None
    return best


def report(name: str, spec: Spec) -> None:
    old, new, opt = run_policy(spec, False), run_policy(spec, True), exact(spec)
    print(name)
    for label, st in (("old", old), ("new", new), ("exact", opt)):
        print(" ", label, "makespan/meanTDR/meanTPOT=", score_key(st), "tokens=", st.produced)


if __name__ == "__main__":
    # Job 0 becomes decode-ready shortly before the other known jobs arrive.
    # Decode has a strong batch setup saving; all task/link dependencies above
    # remain exact.  These are synthetic instances, not contest test files.
    report("R2 same-frame WIP choice", Spec(2, (2, 2), (0, 8)))
    report("R3 same-frame WIP choice", Spec(3, (2, 2, 2), (0, 8, 8)))

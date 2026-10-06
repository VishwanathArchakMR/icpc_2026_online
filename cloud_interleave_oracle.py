"""Exact R=3/K=1 oracle for v36's cloud P/D interleaving decision.

Synthetic but protocol-faithful initial frame:
  * request 0 is D-PROC ready;
  * request 1's D-UP transfer is already committed and finishes at `incoming`;
  * request 2 is P-PROC ready at layer 0.

All later P/D transfers use independent UP/DOWN FIFO queues and the search
allows arbitrary legal P-PROC prefixes, D-PROC groups, D-PRE groups, and
D-POST groups.  The offline DFS minimizes final-token makespan, then P TDR.
The online comparison policies only use the already committed XDN timestamp.
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations, product


# 0/1 are decode requests; 2 is still in prefill P-PROC.
PPROC, PPOST, DREADY, DUP, DPROC, DDOWN, DPOST, FIN, RUN_P, RUN_E, RUN_D = range(11)
R = 3
L = 4


@dataclass(frozen=True)
class Spec:
    s: int = 2
    incoming: int = 2  # <= S: no legal P-PROC task can finish before it
    xfer: int = 2
    pproc: int = 12
    ppost: int = 2
    dpre1: int = 2
    dpre2: int = 3
    dproc1: int = 5
    dproc2: int = 6
    dpost1: int = 2
    dpost2: int = 3

    def dur(self, name: str, n: int = 1) -> int:
        if name == "P": return self.s + self.pproc * n // L
        if name == "PPOST": return self.s + self.ppost
        values = getattr(self, name.lower() + str(min(n, 2)))
        return self.s + values


@dataclass(frozen=True)
class State:
    t: int
    phase: tuple[int, ...]
    pnext: int
    edge_free: bool
    cloud_free: bool
    up_tail: int
    down_tail: int
    events: tuple[tuple[int, int, str, tuple[int, ...]], ...]
    serial: int
    token: tuple[int, ...]
    p_tdr: int


def initial(sp: Spec) -> State:
    # Job 1's D-UP event is a legal automatic transfer already in flight.
    return State(0, (DPROC, DUP, PPROC), 0, True, True, sp.incoming, 0,
                 ((sp.incoming, 0, "XUP_D", (1,)),), 1, (-1, -1, -1), -1)


def add(st: State, when: int, kind: str, ids: tuple[int, ...]) -> State:
    return State(st.t, st.phase, st.pnext, st.edge_free, st.cloud_free,
                 st.up_tail, st.down_tail,
                 tuple(sorted(st.events + ((when, st.serial, kind, ids),))),
                 st.serial + 1, st.token, st.p_tdr)


def advance(st: State, sp: Spec) -> State | None:
    if not st.events:
        return None
    t = st.events[0][0]
    now = [x for x in st.events if x[0] == t]
    events = [x for x in st.events if x[0] != t]
    ph = list(st.phase); edge, cloud = st.edge_free, st.cloud_free
    up, down, serial, pnext, ptdr = st.up_tail, st.down_tail, st.serial, st.pnext, st.p_tdr
    token = list(st.token)

    def push(when: int, kind: str, ids: tuple[int, ...]) -> None:
        nonlocal serial
        events.append((when, serial, kind, ids)); serial += 1

    for _, _, kind, ids in now:
        if kind == "XUP_D":
            for i in ids: ph[i] = DPROC
        elif kind == "TDN_P":
            cloud = True
            if pnext == L:
                ph[2] = PPOST
                down = max(down, t) + sp.xfer
                push(down, "XDOWN_P", (2,))
            else:
                ph[2] = PPROC
        elif kind == "XDOWN_P":
            ph[2] = PPOST
        elif kind == "TDN_PPOST":
            edge = True; ph[2] = DREADY; ptdr = t
        elif kind == "TDN_DPRE":
            edge = True
            for i in ids: ph[i] = DUP
            up = max(up, t) + sp.xfer
            push(up, "XUP_D", ids)
        elif kind == "TDN_DPROC":
            cloud = True
            for i in ids: ph[i] = DDOWN
            down = max(down, t) + sp.xfer
            push(down, "XDOWN_D", ids)
        elif kind == "XDOWN_D":
            for i in ids: ph[i] = DPOST
        elif kind == "TDN_DPOST":
            edge = True
            for i in ids: ph[i] = FIN; token[i] = t
        else:
            raise AssertionError(kind)
    return State(t, tuple(ph), pnext, edge, cloud, up, down, tuple(sorted(events)),
                 serial, tuple(token), ptdr)


def start(st: State, sp: Spec, e: tuple[str, tuple[int, ...]] | None,
          c: tuple[str, tuple[int, ...] | int] | None) -> State:
    ph = list(st.phase); edge, cloud, pnext = st.edge_free, st.cloud_free, st.pnext
    out = st
    if e is not None:
        op, ids = e; assert edge
        if op == "PPOST":
            assert ids == (2,) and ph[2] == PPOST
            ph[2] = RUN_E; out = add(out, st.t + sp.dur("PPOST"), "TDN_PPOST", ids)
        elif op == "DPRE":
            assert ids and all(ph[i] == DREADY for i in ids)
            for i in ids: ph[i] = RUN_E
            out = add(out, st.t + sp.dur("DPRE", len(ids)), "TDN_DPRE", ids)
        elif op == "DPOST":
            assert ids and all(ph[i] == DPOST for i in ids)
            for i in ids: ph[i] = RUN_E
            out = add(out, st.t + sp.dur("DPOST", len(ids)), "TDN_DPOST", ids)
        else: raise AssertionError(op)
        edge = False
    if c is not None:
        op, arg = c; assert cloud
        if op == "P":
            le = int(arg); assert ph[2] == PPROC and pnext < le <= L
            pnext = le; ph[2] = RUN_P
            out = add(out, st.t + sp.dur("P", le - st.pnext), "TDN_P", (2,))
        elif op == "DPROC":
            ids = arg; assert ids and all(ph[i] == DPROC for i in ids)
            for i in ids: ph[i] = RUN_D
            out = add(out, st.t + sp.dur("DPROC", len(ids)), "TDN_DPROC", ids)
        else: raise AssertionError(op)
        cloud = False
    return State(st.t, tuple(ph), pnext, edge, cloud, out.up_tail, out.down_tail,
                 out.events, out.serial, st.token, st.p_tdr)


def actions(st: State):
    es = [None]
    if st.edge_free:
        if st.phase[2] == PPOST: es.append(("PPOST", (2,)))
        for wanted, op in ((DREADY, "DPRE"), (DPOST, "DPOST")):
            q = tuple(i for i, p in enumerate(st.phase) if p == wanted)
            es += [(op, ids) for n in range(1, len(q) + 1) for ids in combinations(q, n)]
    cs = [None]
    if st.cloud_free:
        if st.phase[2] == PPROC:
            cs += [("P", le) for le in range(st.pnext + 1, L + 1)]
        q = tuple(i for i, p in enumerate(st.phase) if p == DPROC)
        cs += [("DPROC", ids) for n in range(1, len(q) + 1) for ids in combinations(q, n)]
    return es, cs


def key(st: State) -> tuple[int, int]:
    return max(st.token), st.p_tdr


def exact(sp: Spec) -> tuple[State, tuple]:
    best: State | None = None; plan = ()
    def dfs(st: State, hist: tuple) -> None:
        nonlocal best, plan
        if all(p == FIN for p in st.phase):
            if best is None or key(st) < key(best): best, plan = st, hist
            return
        if best is not None and st.t >= key(best)[0]: return
        es, cs = actions(st)
        # Work-conserving choices plus intentional no-op until a known event.
        for e, c in product(es, cs):
            if e is None and c is None:
                continue
            dfs(start(st, sp, e, c), hist + ((st.t, e, c),))
        nxt = advance(st, sp)
        if nxt is not None:
            dfs(nxt, hist + ((st.t, "WAIT", None),))
    # Initial frame has already been delivered, so schedule before advancing.
    dfs(initial(sp), ())
    assert best is not None
    return best, plan


def policy(sp: Spec, mode: str) -> State:
    st = initial(sp)
    first = True
    defer_until_upload = False
    def best(ids: tuple[int, ...], name: str, wire: bool) -> tuple[int, ...]:
        if not ids:
            return ()
        b = max(range(1, len(ids) + 1),
                key=lambda n: n / (sp.dur(name, n) + (sp.xfer if wire else 0)))
        return ids[:b]
    while not all(p == FIN for p in st.phase):
        es, cs = actions(st)
        e = None
        if st.edge_free:
            e = next((x for x in es if x and x[0] == "PPOST"), None)
            if e is None:
                posts = tuple(i for i, p in enumerate(st.phase) if p == DPOST)
                e = ("DPOST", best(posts, "DPOST", False)) if posts else None
            if e is None:
                ready = tuple(i for i, p in enumerate(st.phase) if p == DREADY)
                e = ("DPRE", best(ready, "DPRE", True)) if ready else None
        c = None
        dready = tuple(i for i, p in enumerate(st.phase) if p == DPROC)
        d = ("DPROC", best(dready, "DPROC", True)) if st.cloud_free and dready else None
        pfull = ("P", L) if st.cloud_free and st.phase[2] == PPROC else None
        if mode == "rollout" and not defer_until_upload and d and st.edge_free and st.phase[2] == PPOST:
            q = len(dready)
            # Restricted exact two-action rollout: no existing D-POST/D-PRE
            # work, so P POST -> one-member D PRE -> UP is deterministic.
            if q == len(d) and len(best(dready + (2,), "DPROC", True)) == q + 1:
                ppost_done = st.t + sp.dur("PPOST")
                dpre_done = ppost_done + sp.dur("DPRE", 1)
                eta = dpre_done + sp.xfer
                old_proc = st.t + sp.dur("DPROC", q)
                old_down = max(st.down_tail, old_proc) + sp.xfer
                p_proc = max(old_proc, eta) + sp.dur("DPROC", 1)
                p_down = max(old_down, p_proc) + sp.xfer
                old_post = max(dpre_done, old_down) + sp.dur("DPOST", q)
                now_last = max(old_post, p_down) + sp.dur("DPOST", 1)
                joint_proc = eta + sp.dur("DPROC", q + 1)
                joint_down = max(st.down_tail, joint_proc) + sp.xfer
                defer_last = max(dpre_done, joint_down) + sp.dur("DPOST", q + 1)
                defer_until_upload = defer_last < now_last
        # At the disputed initial state, v36 chooses whole P PROC; alternatives
        # either wait for the committed XDN or run D PROC immediately.
        if first and d and pfull:
            c = pfull if mode == "v36" else (d if mode == "immediate" else None)
        elif (mode == "defer_prefill" or defer_until_upload) and d and st.phase[2] in (PPOST, DREADY, RUN_E, DUP):
            # Edge work for the same permanently assigned request is already
            # ready/in flight.  Defer the old decode group until that request
            # joins it; this is the candidate v36 does not currently model.
            c = None
        elif d:
            c = d
        elif pfull:
            c = pfull
        if e or c:
            st = start(st, sp, e, c); first = False
        else:
            nxt = advance(st, sp)
            if nxt is None: raise AssertionError("stuck")
            st = nxt; first = False
    return st


if __name__ == "__main__":
    sp = Spec()
    for label in ("v36", "wait", "immediate", "defer_prefill", "rollout"):
        s = policy(sp, label)
        print(label, key(s), s.token)
    s, p = exact(sp)
    print("exact", key(s), s.token, "plan", p)

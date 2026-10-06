#include <bits/stdc++.h>
using namespace std;

// Edge-Cloud Collaborative Scheduling — V4
// ================================================================
// Goal: retain the strong throughput behavior of the 12.77k/14.26k
// versions while fixing the two concrete weaknesses visible in the
// latest 14.607k submission:
//
//   (A) the scheduler was doing O(R) scans of every request on almost
//       every frame to estimate mean TDR/TPOT. On large tests this can
//       dominate the 15 s limit even though the algorithm itself is fast.
//
//   (B) the adaptive controller could shrink decode batches on tests whose
//       score is overwhelmingly throughput-driven. That was beneficial on
//       latency-sensitive tests (#3/#8/#13/#21), but it regressed #16.
//
// V4 therefore uses:
//   1. O(1) running lower-bound estimates for mean TDR and mean TPOT.
//   2. Explicit score-weight gating: strong throughput tests keep aggressive
//      batching; only mixed/latency-heavy tests activate the guard.
//   3. An age-aware first-token queue: old requests waiting for P POST/D PRE
//      get protected from starvation, but recurring decode work remains the
//      default when throughput weight is high.
//   4. A hard adaptive cap on decode batch size only when waiting pressure is
//      genuinely dangerous.
//   5. Selective 2/4-piece prefill only in latency mode and only if a decode
//      opportunity on the same worker is long enough to justify the extra S.
//   6. Link/worker-aware P PRE placement using the shared UP FIFO endpoint,
//      the per-worker virtual CPU endpoint, the input DOWN FIFO endpoint, and
//      the exact input transfer cost.
//
// Important protocol facts used here are exactly those in the statement:
//   - each task occupies its server for S + dur;
//   - UP and DOWN are independent FIFO links;
//   - D PRE/D POST may span remotes; D PROC may not;
//   - P PROC pieces are consecutive, nonempty and gap-free.
// ================================================================

struct TaskTimeTable {
    vector<pair<long long,double>> col[6];

    void addRow(long long bs, const array<double,6>& v) {
        for (int c = 0; c < 6; ++c)
            if (v[c] >= 0.0) col[c].push_back({bs, v[c]});
    }

    void finalize() {
        for (auto &v : col) sort(v.begin(), v.end());
    }

    double lookup(int c, long long x) const {
        const auto& v = col[c];
        if (v.empty()) return 1.0;
        if (x <= v.front().first) return v.front().second;
        if (x >= v.back().first) return v.back().second;
        auto it = upper_bound(v.begin(), v.end(),
                              make_pair(x, numeric_limits<double>::infinity()));
        auto hi = *it;
        auto lo = *prev(it);
        const double f = double(x - lo.first) / double(hi.first - lo.first);
        return lo.second + f * (hi.second - lo.second);
    }
};

enum Phase : unsigned char {
    NONE = 0,
    ARRIVED,
    WAIT_UP_PRE,
    READY_PPROC,
    WAIT_DOWN_PRE,
    READY_PPOST,
    READY_DPRE,
    WAIT_UP_DEC,
    READY_DPROC,
    WAIT_DOWN_DEC,
    READY_DPOST,
    FINISHED
};

struct Req {
    long long Lin = -1;
    int remote = -1;
    Phase phase = NONE;

    double arrival = -1.0;
    double lastToken = -1.0;
    bool hasToken = false;

    // First-token guard: true while the request is ready for its first D PRE.
    bool firstDecode = false;

    // Prefill piece state.
    unsigned char chunks = 1;
    unsigned char piece = 0;

    bool finThisFrame = false;
};

static inline double clamp01(double x) {
    return max(0.0, min(1.0, x));
}

int main() {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    int K;
    double S, latency, bandwidth;
    long long bytesPerToken;
    int numLayers;

    if (!(cin >> K >> S >> latency >> bandwidth >> bytesPerToken >> numLayers))
        return 0;

    double SLO1, SLO2, tpUB, tpBase, distBase, wtp, wc;
    cin >> SLO1 >> SLO2 >> tpUB >> tpBase >> distBase >> wtp >> wc;

    int N;
    cin >> N;
    TaskTimeTable table;
    for (int i = 0; i < N; ++i) {
        long long bs;
        array<double,6> x{};
        cin >> bs;
        for (double &z : x) cin >> z;
        table.addRow(bs, x);
    }
    table.finalize();

    vector<Req> req;
    req.resize(2048);

    vector<int> assigned(K, 0);
    vector<double> remoteEst(K, 0.0);
    vector<char> remoteFree(K, 1);
    bool localFree = true;

    deque<int> arrivedQ;
    vector<deque<int>> pprocQ(K);
    deque<int> ppostQ;

    // D PRE is split into a first-token lane and recurring-token lane.
    // This makes the starvation rule explicit rather than relying on luck in
    // a single FIFO queue.
    deque<int> dpreFirstQ;
    deque<int> dpreRepeatQ;
    vector<deque<int>> dprocQ(K);
    deque<int> dpostQ;

    // Shared FIFO virtual endpoints.
    double upEnd = 0.0;
    double downEnd = 0.0;
    int upPending = 0;
    int downPending = 0;

    // ---------------------------------------------------------------
    // O(1) running lower-bound metrics for the official scoring terms.
    // ---------------------------------------------------------------
    // TDR completed contribution.
    long double tdrSum = 0.0L;
    long long tdrDone = 0;
    // Requests not yet P POST-complete.
    long long tdrPending = 0;
    long double tdrPendingArrivalSum = 0.0L;

    // TPOT completed-gap contribution.
    long double tpotSum = 0.0L;
    long long tpotDone = 0;
    // Active requests that already produced >=1 token and may produce another.
    long long activeGapCount = 0;
    long double activeLastTokenSum = 0.0L;

    // Reusable frame buffer; no allocation after first growth.
    vector<int> frameDPost;
    frameDPost.reserve(4096);

    auto ensure = [&](int rid) {
        if (rid >= (int)req.size()) req.resize(rid + 1);
    };

    auto xfer = [&](long long len) -> double {
        return latency + 8.0 * double(len) * double(bytesPerToken)
                     / (bandwidth * 1e6);
    };

    auto task = [&](int col, long long bs) -> double {
        return S + table.lookup(col, bs);
    };

    // ---------------------------------------------------------------
    // O(1) current estimates matching the official metric definitions.
    // These are scheduling lower bounds, not claimed final metrics.
    // ---------------------------------------------------------------
    auto currentTdr = [&](double now) -> double {
        const long long cnt = tdrDone + tdrPending;
        if (!cnt) return 0.0;
        const long double sum =
            tdrSum + (long double)tdrPending * now - tdrPendingArrivalSum;
        return double(sum / (long double)cnt);
    };

    auto currentTpot = [&](double now) -> double {
        const long long cnt = tpotDone + activeGapCount;
        if (!cnt) return 0.0;
        const long double sum =
            tpotSum + (long double)activeGapCount * now - activeLastTokenSum;
        return double(sum / (long double)cnt);
    };

    auto ratios = [&](double now) {
        const double a = SLO1 > 0.0 ? currentTdr(now) / SLO1 : 0.0;
        const double b = SLO2 > 0.0 ? currentTpot(now) / SLO2 : 0.0;
        return pair<double,double>{a,b};
    };

    // The score weights are known from startup. Do not let a heuristic
    // controller ignore the official objective. wc is the importance of the
    // SLO component, wtp is the importance of throughput.
    auto mode = [&](double now) -> int {
        // 2 = latency, 1 = mixed, 0 = throughput.
        auto [rt, rp] = ratios(now);

        // Strong throughput tests should stay close to the proven full-batch
        // scheduler. This condition is deliberately conservative.
        if (wtp >= 0.72 && wc <= 0.28) return 0;

        // Strong SLO tests: activate starvation/latency protection early.
        if (wc >= 0.62) return 2;

        // Mixed tests: only enter latency mode when pressure is actually
        // visible, otherwise preserve throughput behavior.
        if (max(rt, rp) >= 0.90 || (rt >= 0.75 && rp >= 0.75)) return 2;
        if (max(rt, rp) >= 0.55) return 1;
        return 0;
    };

    // ---------------------------------------------------------------
    // Remote choice for P PRE.
    // The UP FIFO position is common to all workers. The differentiating
    // term is the remote CPU reservation; we also include the eventual DOWN
    // FIFO and P POST because the end of P POST is exactly TDR completion.
    // ---------------------------------------------------------------
    auto chooseRemote = [&](int rid, double now) -> int {
        const long long lin = req[rid].Lin;
        const double ppreEnd = now + task(0, lin);
        const double upDone = max(now, upEnd);
        const double upFinish = upDone + xfer(lin);
        const double pprocDur = task(1, lin);
        const double ppostDur = task(2, lin);

        int best = 0;
        double bestScore = 1e300;

        for (int k = 0; k < K; ++k) {
            // If a previous reservation was made for this worker, it captures
            // both P PROC and D PROC work already committed by us.
            const double procFinish = max(remoteEst[k], upFinish) + pprocDur;
            const double downFinish = max(downEnd, procFinish) + xfer(lin);
            const double tdrFinish = downFinish + ppostDur;

            // Throughput-sensitive tests should prefer finishing the remote
            // workload; SLO-heavy tests should prefer the shortest TDR path.
            double score;
            if (wtp >= 0.72 && wc <= 0.28) {
                score = procFinish;
            } else {
                const double throughputTerm = procFinish;
                const double latencyTerm = tdrFinish;
                score = wtp * throughputTerm + wc * latencyTerm;
            }

            // Tiny deterministic load term prevents pathological ties while
            // keeping the actual estimated finish time dominant.
            score += 1e-7 * assigned[k];

            if (score < bestScore) {
                bestScore = score;
                best = k;
            }
        }
        return best;
    };

    // ---------------------------------------------------------------
    // Prefill chunking policy.
    // More chunks pay S more often, so they are only enabled in latency mode
    // and only if a useful decode interruption exists.
    // ---------------------------------------------------------------
    auto chooseChunks = [&](int rid, double now) -> unsigned char {
        if (numLayers < 4 || K == 0) return 1;

        const int k = req[rid].remote;
        if (k < 0 || k >= K) return 1;

        const int dWaiting = (int)dprocQ[k].size();
        const int firstWaiting = (int)dpreFirstQ.size();
        if (dWaiting == 0 && firstWaiting == 0) return 1;

        const int m = mode(now);
        if (m == 0) return 1;

        const double full = task(1, req[rid].Lin);
        const double oneDecode = task(4, 1) + xfer(1);
        const auto [rt, rp] = ratios(now);

        if (full < 1.7 * oneDecode) return 1;
        if (rt < 0.75 && rp < 0.75 && dWaiting < 2) return 1;

        if (m == 2 && numLayers >= 8 && dWaiting >= 4 && full > 4.0 * oneDecode)
            return 4;
        return 2;
    };

    // ---------------------------------------------------------------
    // Batch candidates.
    // Important: high-throughput mode keeps the full ready set. The adaptive
    // search is only entered when SLO pressure justifies giving up some
    // amortization.
    // ---------------------------------------------------------------
    static constexpr int CAND[] = {
        1,2,4,8,16,32,64,128,256,512,1024,2048,4096
    };

    auto cappedCandidate = [&](int n, int cap) -> int {
        if (n <= 0) return 0;
        n = min(n, cap);
        for (int i = (int)(sizeof(CAND)/sizeof(CAND[0])) - 1; i >= 0; --i)
            if (CAND[i] <= n) return CAND[i];
        return 1;
    };

    auto firstTokenGuardCount = [&]() -> int {
        return (int)dpreFirstQ.size();
    };

    // Select D PRE batch size. The caller chooses members separately.
    auto chooseDPreBatch = [&](double now, int totalReady) -> int {
        if (totalReady <= 1) return totalReady;
        const int md = mode(now);
        if (md == 0) return totalReady;

        auto [rt, rp] = ratios(now);

        // Queue-starvation guard: never allow the first-token lane to be buried
        // under an arbitrarily large recurring-token batch in SLO-sensitive
        // mode. The whole group can still be >1 when enough first-token work is
        // present; this merely creates a hard ceiling as pressure rises.
        int cap = totalReady;
        if (md == 2) {
            if (max(rt, rp) >= 1.50) cap = min(cap, 8);
            else if (max(rt, rp) >= 1.10) cap = min(cap, 16);
            else if (max(rt, rp) >= 0.90) cap = min(cap, 32);
            else cap = min(cap, 64);
        } else {
            if (max(rt, rp) >= 1.20) cap = min(cap, 32);
            else cap = min(cap, 128);
        }

        // If there are many first-token requests, give them a little more room;
        // batching them together still amortizes S and is much better than one
        // request per D PRE.
        if (firstTokenGuardCount() >= cap / 2 && cap < totalReady)
            cap = min(totalReady, cap * 2);

        // Estimate batch efficiency from D PRE + UP transfer chain + the
        // parallel cloud compute + DOWN FIFO + D POST. Candidate evaluation is
        // intentionally small so the interactive loop stays fast.
        int cands[13];
        int nc = 0;
        const int c = cappedCandidate(totalReady, cap);
        for (int z : CAND) {
            if (z <= c) cands[nc++] = z;
        }
        if (nc == 0 || cands[nc-1] != c) cands[nc++] = c;

        // For scoring we only need a compact throughput proxy and a shortest-
        // task proxy. Exact cloud/link prediction is expensive if done for
        // every candidate; use the total group transfer + task duration and
        // current average worker endpoint as a cheap bound.
        double bestScore = -1e300;
        int best = cands[0];
        for (int ci = 0; ci < nc; ++ci) {
            const int b = cands[ci];
            double dur = task(3, b) + task(5, b);

            // Number of distinct remotes is bounded by K. In expectation, a
            // broad batch touches more remotes; use min(K,b) as a safe upper
            // penalty for the shared UP FIFO.
            const int distinctUpper = min(K, b);
            dur += distinctUpper * latency;
            dur += xfer(b);

            const double throughput = double(b) / max(dur, 1e-9);
            const double latencyScore = 1.0 / max(dur, 1e-9);

            double score = wtp * throughput + wc * 0.35 * latencyScore;

            // In strong first-token pressure, reward larger batches that contain
            // many first-token requests. Member selection below guarantees that
            // first-token requests are pulled first.
            if (!dpreFirstQ.empty()) {
                const int firstInside = min((int)dpreFirstQ.size(), b);
                score += wc * 0.05 * double(firstInside) / double(b);
            }

            if (score > bestScore) {
                bestScore = score;
                best = b;
            }
        }
        return min(best, totalReady);
    };

    auto chooseDPostBatch = [&](double now, int n) -> int {
        if (n <= 1) return n;
        const int md = mode(now);
        if (md == 0) return n;
        auto [rt, rp] = ratios(now);
        int cap = n;
        if (md == 2) {
            if (rp >= 1.50) cap = min(cap, 8);
            else if (rp >= 1.10) cap = min(cap, 16);
            else if (rp >= 0.90) cap = min(cap, 32);
            else cap = min(cap, 64);
        } else {
            cap = min(cap, rp >= 1.20 ? 32 : 128);
        }
        return cappedCandidate(n, cap);
    };

    auto chooseDProcBatch = [&](int k, double now, int n) -> int {
        if (n <= 1) return n;
        const int md = mode(now);
        if (md == 0) return n;
        auto [rt, rp] = ratios(now);
        (void)rt;
        int cap = n;
        if (md == 2) {
            if (rp >= 1.50) cap = min(cap, 8);
            else if (rp >= 1.10) cap = min(cap, 16);
            else if (rp >= 0.90) cap = min(cap, 32);
            else cap = min(cap, 64);
        } else {
            cap = min(cap, rp >= 1.20 ? 32 : 128);
        }
        return cappedCandidate(n, cap);
    };

    auto popFront = [](deque<int>& q) {
        const int x = q.front();
        q.pop_front();
        return x;
    };

    // Pull D PRE members: first-token work first, then recurring-token work.
    auto takeDPreMembers = [&](int b, vector<int>& ids) {
        ids.clear();
        for (int i = 0; i < b && !dpreFirstQ.empty(); ++i)
            ids.push_back(popFront(dpreFirstQ));
        while ((int)ids.size() < b && !dpreRepeatQ.empty())
            ids.push_back(popFront(dpreRepeatQ));
    };

    string first;
    while (cin >> first) {
        if (first == "END") break;
        const double now = stod(first);

        int e;
        cin >> e;

        frameDPost.clear();

        // -------------------------------------------------------------
        // READ THE WHOLE FRAME FIRST.
        // -------------------------------------------------------------
        for (int ev = 0; ev < e; ++ev) {
            string kind;
            cin >> kind;

            if (kind == "ARR") {
                int rid;
                long long lin;
                cin >> rid >> lin;
                ensure(rid);

                Req& r = req[rid];
                r.Lin = lin;
                r.remote = -1;
                r.phase = ARRIVED;
                r.arrival = now;
                r.lastToken = -1.0;
                r.hasToken = false;
                r.firstDecode = false;
                r.chunks = 1;
                r.piece = 0;
                r.finThisFrame = false;

                arrivedQ.push_back(rid);

                ++tdrPending;
                tdrPendingArrivalSum += now;
            }
            else if (kind == "TDN") {
                string server, maj, step;
                cin >> server >> maj >> step;

                const bool isLocal = (server == "E");
                const int k = isLocal ? -1 : stoi(server.substr(1));

                if (maj == "P") {
                    if (step == "PRE") {
                        int remote, rid;
                        double dur;
                        cin >> remote >> rid >> dur;
                        ensure(rid);

                        req[rid].remote = remote;
                        req[rid].phase = WAIT_UP_PRE;
                        localFree = true;

                        ++upPending;
                        upEnd = max(upEnd, now) + xfer(req[rid].Lin);
                    }
                    else if (step == "PROC") {
                        int ls, le, remote, rid;
                        double dur;
                        cin >> ls >> le >> remote >> rid >> dur;
                        ensure(rid);

                        if (remote >= 0 && remote < K) {
                            remoteFree[remote] = 1;
                            remoteEst[remote] = now;
                        }

                        if (le < numLayers) {
                            req[rid].piece = (unsigned char)(req[rid].piece + 1);
                            req[rid].phase = READY_PPROC;
                            pprocQ[req[rid].remote].push_back(rid);
                        } else {
                            req[rid].phase = WAIT_DOWN_PRE;
                            ++downPending;
                            downEnd = max(downEnd, now) + xfer(req[rid].Lin);
                        }
                    }
                    else { // POST
                        int remote, rid;
                        double dur;
                        cin >> remote >> rid >> dur;
                        ensure(rid);

                        req[rid].phase = READY_DPRE;
                        req[rid].firstDecode = true;

                        tdrSum += (long double)(now - req[rid].arrival);
                        ++tdrDone;
                        --tdrPending;
                        tdrPendingArrivalSum -= req[rid].arrival;

                        dpreFirstQ.push_back(rid);
                        localFree = true;
                    }
                }
                else { // D
                    if (step == "PRE") {
                        int minus1, m;
                        cin >> minus1 >> m;
                        int cnt[8] = {0};
                        for (int i = 0; i < m; ++i) {
                            int rid;
                            cin >> rid;
                            req[rid].phase = WAIT_UP_DEC;
                            const int rr = req[rid].remote;
                            if (rr >= 0 && rr < K) ++cnt[rr];
                        }
                        double dur;
                        cin >> dur;
                        localFree = true;

                        // Transfers enter in increasing remote index order.
                        for (int rr = 0; rr < K; ++rr) {
                            if (!cnt[rr]) continue;
                            ++upPending;
                            upEnd = max(upEnd, now) + xfer(cnt[rr]);
                        }
                    }
                    else if (step == "PROC") {
                        int remote, m;
                        cin >> remote >> m;
                        for (int i = 0; i < m; ++i) {
                            int rid;
                            cin >> rid;
                            req[rid].phase = WAIT_DOWN_DEC;
                        }
                        double dur;
                        cin >> dur;

                        if (remote >= 0 && remote < K) {
                            remoteFree[remote] = 1;
                            remoteEst[remote] = now;
                        }

                        ++downPending;
                        downEnd = max(downEnd, now) + xfer(m);
                    }
                    else { // POST
                        int minus1, m;
                        cin >> minus1 >> m;
                        for (int i = 0; i < m; ++i) {
                            int rid;
                            cin >> rid;
                            frameDPost.push_back(rid);
                        }
                        double dur;
                        cin >> dur;
                        localFree = true;
                    }
                }
            }
            else if (kind == "XDN") {
                string dir, sub;
                int remote, m;
                long long size;
                cin >> dir >> remote >> size >> sub >> m;
                (void)size;

                if (sub == "PRE") {
                    // m is guaranteed 1 for input-stage transfers.
                    int rid;
                    cin >> rid;

                    if (dir == "UP") {
                        if (upPending > 0) --upPending;
                        if (upPending == 0) upEnd = now;

                        req[rid].phase = READY_PPROC;
                        req[rid].chunks = chooseChunks(rid, now);
                        req[rid].piece = 0;
                        pprocQ[req[rid].remote].push_back(rid);
                    }
                    else {
                        if (downPending > 0) --downPending;
                        if (downPending == 0) downEnd = now;

                        req[rid].phase = READY_PPOST;
                        ppostQ.push_back(rid);
                    }
                }
                else { // DEC
                    if (dir == "UP") {
                        if (upPending > 0) --upPending;
                        if (upPending == 0) upEnd = now;

                        for (int i = 0; i < m; ++i) {
                            int rid;
                            cin >> rid;
                            req[rid].phase = READY_DPROC;
                            dprocQ[remote].push_back(rid);
                        }
                    }
                    else {
                        if (downPending > 0) --downPending;
                        if (downPending == 0) downEnd = now;

                        for (int i = 0; i < m; ++i) {
                            int rid;
                            cin >> rid;
                            req[rid].phase = READY_DPOST;
                            dpostQ.push_back(rid);
                        }
                    }
                }
            }
            else { // FIN
                int rid;
                cin >> rid;
                ensure(rid);
                req[rid].finThisFrame = true;
            }
        }

        // Resolve D POST after all FIN events in this frame are known.
        for (int rid : frameDPost) {
            Req& r = req[rid];

            if (r.hasToken) {
                tpotSum += (long double)(now - r.lastToken);
                ++tpotDone;
                // active gap was already represented by this request since its
                // previous token.
                activeLastTokenSum -= r.lastToken;
            } else {
                ++activeGapCount;
            }

            r.hasToken = true;
            r.lastToken = now;
            activeLastTokenSum += now;

            if (r.finThisFrame) {
                r.phase = FINISHED;
                r.finThisFrame = false;
                if (r.remote >= 0) --assigned[r.remote];

                // Final token means there can be no future TPOT gap for this
                // request; remove its active contribution immediately.
                --activeGapCount;
                activeLastTokenSum -= now;
            } else {
                r.phase = READY_DPRE;
                r.firstDecode = false;
                dpreRepeatQ.push_back(rid);
            }
        }

        // -------------------------------------------------------------
        // Choose assignments.
        // -------------------------------------------------------------
        array<string, 9> out{};
        int outN = 0;

        auto pushOut = [&](string s) {
            if (outN < (int)out.size()) out[outN++] = std::move(s);
        };

        const int md = mode(now);
        const auto [rt, rp] = ratios(now);

        // -------------------------------------------------------------
        // LOCAL: the local computer is the principal TDR/TPOT gate.
        // -------------------------------------------------------------
        if (localFree) {
            // Severe TDR pressure: finish P POST first. This is the shortest
            // way to convert a prefill-complete request into decode-ready work.
            if ((md == 2 && rt >= 0.90) && !ppostQ.empty()) {
                const int rid = popFront(ppostQ);
                ostringstream ss;
                ss << "E P POST " << req[rid].remote << ' ' << rid;
                pushOut(ss.str());
                localFree = false;
            }
            else if ((md == 2 && rt >= 0.90) && !arrivedQ.empty()) {
                const int rid = popFront(arrivedQ);
                const int k = chooseRemote(rid, now);
                req[rid].remote = k;
                ++assigned[k];

                const double pre = now + task(0, req[rid].Lin);
                const double upDone = max(upEnd, pre) + xfer(req[rid].Lin);
                remoteEst[k] = max(remoteEst[k], upDone) + task(1, req[rid].Lin);

                ostringstream ss;
                ss << "E P PRE " << k << ' ' << rid;
                pushOut(ss.str());
                localFree = false;
            }
            else if (!dpostQ.empty()) {
                const int b = chooseDPostBatch(now, (int)dpostQ.size());
                ostringstream ss;
                ss << "E D POST -1 " << b;
                for (int i = 0; i < b; ++i) ss << ' ' << popFront(dpostQ);
                pushOut(ss.str());
                localFree = false;
            }
            else if (!dpreFirstQ.empty() || !dpreRepeatQ.empty()) {
                const int total = (int)dpreFirstQ.size() + (int)dpreRepeatQ.size();
                const int b = chooseDPreBatch(now, total);

                // Starvation guard is expressed by member selection: first-token
                // requests enter before recurring-token requests.
                vector<int> ids;
                ids.reserve(b);
                takeDPreMembers(b, ids);

                ostringstream ss;
                ss << "E D PRE -1 " << ids.size();
                for (int rid : ids) ss << ' ' << rid;
                pushOut(ss.str());
                localFree = false;
            }
            else if (!ppostQ.empty()) {
                const int rid = popFront(ppostQ);
                ostringstream ss;
                ss << "E P POST " << req[rid].remote << ' ' << rid;
                pushOut(ss.str());
                localFree = false;
            }
            else if (!arrivedQ.empty()) {
                const int rid = popFront(arrivedQ);
                const int k = chooseRemote(rid, now);
                req[rid].remote = k;
                ++assigned[k];

                const double pre = now + task(0, req[rid].Lin);
                const double upDone = max(upEnd, pre) + xfer(req[rid].Lin);
                remoteEst[k] = max(remoteEst[k], upDone) + task(1, req[rid].Lin);

                ostringstream ss;
                ss << "E P PRE " << k << ' ' << rid;
                pushOut(ss.str());
                localFree = false;
            }
        }

        // -------------------------------------------------------------
        // REMOTES: decode-first only when it is justified; otherwise use the
        // worker for prefill so new requests keep entering the pipeline.
        // -------------------------------------------------------------
        for (int k = 0; k < K; ++k) {
            if (!remoteFree[k]) continue;

            const bool haveD = !dprocQ[k].empty();
            const bool haveP = !pprocQ[k].empty();
            if (!haveD && !haveP) continue;

            bool doP = false;
            if (haveP && !haveD) {
                doP = true;
            } else if (!haveP) {
                doP = false;
            } else {
                // TDR pressure protects prefill; TPOT pressure protects decode.
                // Strong throughput mode prefers decode only if it is already
                // ready; mixed mode compares normalized pressure directly.
                if (md == 0) {
                    doP = false;
                } else if (md == 2) {
                    doP = rt > rp + 0.05;
                } else {
                    doP = rt > rp;
                }
            }

            if (doP) {
                const int rid = popFront(pprocQ[k]);
                Req& r = req[rid];

                int ls = 0, le = numLayers;
                if (r.chunks == 2) {
                    const int mid = max(1, numLayers / 2);
                    if (r.piece == 0) le = mid;
                    else ls = mid;
                } else if (r.chunks == 4) {
                    const int q = max(1, numLayers / 4);
                    const int a = r.piece;
                    ls = (a == 0 ? 0 : (a == 1 ? q : (a == 2 ? 2*q : 3*q)));
                    le = (a == 0 ? q : (a == 1 ? 2*q : (a == 2 ? 3*q : numLayers)));
                    if (le <= ls) { ls = 0; le = numLayers; }
                    if (a == 3) le = numLayers;
                }

                const double frac = double(le - ls) / double(numLayers);
                const double dur = S + table.lookup(1, r.Lin) * frac;
                remoteEst[k] = now + dur;

                ostringstream ss;
                ss << 'C' << k << " P PROC " << ls << ' ' << le
                   << ' ' << k << ' ' << rid;
                pushOut(ss.str());
                remoteFree[k] = 0;
            }
            else {
                const int b = chooseDProcBatch(k, now, (int)dprocQ[k].size());
                ostringstream ss;
                ss << 'C' << k << " D PROC " << k << ' ' << b;
                for (int i = 0; i < b; ++i) ss << ' ' << popFront(dprocQ[k]);
                pushOut(ss.str());
                remoteFree[k] = 0;
                remoteEst[k] = now + task(4, b);
            }
        }

        cout << outN << '\n';
        for (int i = 0; i < outN; ++i) cout << out[i] << '\n';
        cout.flush();
    }

    return 0;
}

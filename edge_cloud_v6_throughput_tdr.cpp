#include <bits/stdc++.h>
using namespace std;

// ============================================================================
// Edge-Cloud Collaborative Scheduling — V6
// ============================================================================
// Strategy:
//   V6 is deliberately based on the proven V5/12.77k behavior, but removes
//   the two things that hurt the latest submissions:
//
//   (A) decode throttling in bursty/high-throughput tests;
//   (B) starvation of the input pipeline / P POST when recurring decode keeps
//       the system busy.
//
// The scheduler uses four principles:
//   1) FIRST-TOKEN FIFO GUARD:
//      P POST is executed immediately when possible, then first-output D PRE
//      work is protected from recurring decode. This attacks mean TDR without
//      abandoning throughput.
//
//   2) ADMISSION STARVATION GUARD:
//      A queued ARR request is allowed to pre-empt recurring decode only when
//      the oldest unadmitted request has waited long enough. The guard is much
//      weaker in throughput-dominant tests, so Test 22-style workloads retain
//      aggressive decode batching.
//
//   3) LARGE DECODE BATCHES:
//      In throughput mode, a D PROC batch is normally the entire ready set.
//      For very large queues we still evaluate the task-time table and choose
//      the largest near-optimal batch. This avoids arbitrary 32/64/128 caps.
//      The fixed schedule cost S is explicitly included in the efficiency
//      calculation.
//
//   4) REMOTE FAIRNESS ONLY WHEN NECESSARY:
//      Decode remains the default on a free remote. Prefill is forced only when
//      the prefill backlog or oldest arrival age demonstrates starvation.
//      This preserves the huge Test 19 recovery while preventing a Test 22
//      throughput collapse caused by overly frequent P PROC interruptions.
//
// Protocol invariants are unchanged. All decisions happen only after reading
// a complete frame. No future event timestamp is required for correctness.
// ============================================================================

struct TaskTimeTable {
    vector<pair<long long, double>> col[6];

    void addRow(long long bs, const array<double,6>& v) {
        for (int c = 0; c < 6; ++c)
            if (v[c] >= 0.0) col[c].push_back({bs, v[c]});
    }

    void finalize() {
        for (auto &v : col) sort(v.begin(), v.end());
    }

    double lookup(int c, long long bs) const {
        const auto& v = col[c];
        if (v.empty()) return 1.0;
        if (bs <= v.front().first) return v.front().second;
        if (bs >= v.back().first) return v.back().second;
        auto it = upper_bound(v.begin(), v.end(),
                              make_pair(bs, numeric_limits<double>::infinity()));
        auto hi = *it;
        auto lo = *prev(it);
        double f = double(bs - lo.first) / double(hi.first - lo.first);
        return lo.second + f * (hi.second - lo.second);
    }
};

enum class Phase : unsigned char {
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
    long long Lin = 0;
    int remote = -1;
    Phase phase = Phase::NONE;
    double arrival = -1.0;
    double readyDecodeTime = -1.0;
    bool hasProducedToken = false;
    double lastToken = -1.0;
    int chunks = 1;
    int piece = 0;
    bool finThisFrame = false;
};

int main() {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    int K;
    double S, latency, bandwidth;
    long long bytesPerToken;
    int numLayers;
    if (!(cin >> K >> S >> latency >> bandwidth >> bytesPerToken >> numLayers)) return 0;

    double SLO1, SLO2, tpUB, tpBase, distBase, wtp, wc;
    cin >> SLO1 >> SLO2 >> tpUB >> tpBase >> distBase >> wtp >> wc;

    int N;
    cin >> N;
    TaskTimeTable table;
    for (int i = 0; i < N; ++i) {
        long long bs;
        array<double,6> v{};
        cin >> bs;
        for (double &x : v) cin >> x;
        table.addRow(bs, v);
    }
    table.finalize();

    auto xfer = [&](long long len) -> double {
        return latency + 8.0 * double(len) * double(bytesPerToken)
             / (bandwidth * 1e6);
    };

    auto taskBusy = [&](int col, long long bs) -> double {
        return S + table.lookup(col, bs);
    };

    vector<Req> req;
    req.reserve(4096);
    auto ensure = [&](int rid) {
        if (rid >= (int)req.size()) req.resize(rid + 1);
    };

    // ------------------------------------------------------------------------
    // Ready queues.
    // ------------------------------------------------------------------------
    deque<int> arrivalQ;                    // ARR -> P PRE
    vector<deque<int>> pprocQ(K);           // ready P PROC by remote
    deque<int> ppostQ;                      // ready P POST

    // Separate first-token and recurring decode queues on the local computer.
    // The first-token queue is FIFO and is never buried behind repeat decode.
    deque<int> firstDecodeQ;
    deque<int> repeatDecodeQ;

    // Remote D PROC queues. XDN order preserves the D PRE member order.
    vector<deque<int>> dprocQ(K);
    deque<int> dpostQ;

    bool localFree = true;
    vector<char> remoteFree(K, 1);

    // Virtual queue endpoints used only for P PRE placement / admission.
    vector<double> remoteEnd(K, 0.0);
    vector<int> remoteDecodeStreak(K, 0);
    double upEnd = 0.0;
    double downEnd = 0.0;
    int upPending = 0;
    int downPending = 0;

    // ------------------------------------------------------------------------
    // Incremental statistics: O(1) pressure checks.
    // ------------------------------------------------------------------------
    long double tdrDoneSum = 0.0L;
    long long tdrDone = 0;
    long long tdrPending = 0;
    long double pendingArrivalSum = 0.0L;

    long double tpotDoneSum = 0.0L;
    long long tpotDone = 0;
    long long activeGapReqs = 0;
    long double activeLastTokenSum = 0.0L;

    auto meanTDR = [&](double now) -> double {
        long long cnt = tdrDone + tdrPending;
        if (cnt == 0) return 0.0;
        long double sum = tdrDoneSum
                        + (long double)tdrPending * now
                        - pendingArrivalSum;
        return double(sum / (long double)cnt);
    };

    auto meanTPOT = [&](double now) -> double {
        long long cnt = tpotDone + activeGapReqs;
        if (cnt == 0) return 0.0;
        long double sum = tpotDoneSum
                        + (long double)activeGapReqs * now
                        - activeLastTokenSum;
        return double(sum / (long double)cnt);
    };

    auto pressurePair = [&](double now) -> pair<double,double> {
        const double tdr = meanTDR(now);
        const double tpot = meanTPOT(now);
        const double pt = (SLO1 > 0.0 ? tdr / SLO1 : 0.0);
        const double pp = (SLO2 > 0.0 ? tpot / SLO2 : 0.0);
        return {pt, pp};
    };

    // Throughput-dominant mode must remain aggressive. The problem explicitly
    // allows arbitrarily large output groups (bounded only by ready requests).
    const bool throughputMode = (wtp >= 0.72 || wc <= 0.28);

    // ------------------------------------------------------------------------
    // Queue-starvation / admission tests.
    // ------------------------------------------------------------------------
    auto oldestArrivalAge = [&](double now) -> double {
        if (arrivalQ.empty()) return 0.0;
        return max(0.0, now - req[arrivalQ.front()].arrival);
    };

    auto firstTokenWaitingAge = [&](double now) -> double {
        if (firstDecodeQ.empty()) return 0.0;
        return max(0.0, now - req[firstDecodeQ.front()].readyDecodeTime);
    };

    // Strong enough to preserve Test 19, but deliberately much harder to trip
    // in throughput-heavy tests such as Test 22.
    auto admissionStarving = [&](double now) -> bool {
        if (arrivalQ.empty()) return false;
        const double age = oldestArrivalAge(now);
        const size_t q = arrivalQ.size();

        if (!throughputMode) {
            return age > max(2.0 * SLO1, 10.0 * S) || q >= (size_t)max(2, 2*K);
        }

        // Throughput mode: admission intervention is exceptional.
        return age > max(6.0 * SLO1, 25.0 * S) ||
               q >= (size_t)max(8, 4*K);
    };

    auto postUrgent = [&](double now) -> bool {
        if (ppostQ.empty()) return false;
        const int rid = ppostQ.front();
        const double age = now - req[rid].arrival;
        if (!throughputMode) return true;
        return age > max(3.0 * SLO1, 15.0 * S);
    };

    auto chooseRemote = [&](int rid, double now) -> int {
        const Req& r = req[rid];
        const long long lin = r.Lin;
        const double pre = now + taskBusy(0, lin);
        const double upStart = max(pre, upEnd);
        const double upFinish = upStart + xfer(lin);
        const double procDur = taskBusy(1, lin);
        const double postDur = taskBusy(2, lin);

        int best = 0;
        double bestScore = numeric_limits<double>::infinity();
        for (int k = 0; k < K; ++k) {
            const double procStart = max(remoteEnd[k], upFinish);
            const double procFinish = procStart + procDur;
            const double downStart = max(downEnd, procFinish);
            const double downFinish = downStart + xfer(lin);
            const double tdrFinish = downFinish + postDur;

            // Input completion is the primary objective. In throughput mode we
            // mostly balance worker endpoints, but never ignore network queues.
            double score = throughputMode
                         ? procFinish + 0.30 * (tdrFinish - procFinish)
                         : tdrFinish;

            if (remoteFree[k]) score -= 0.20 * procDur;
            score += 1e-8 * double(remoteDecodeStreak[k]);
            score += 1e-9 * double(k);

            if (score < bestScore) {
                bestScore = score;
                best = k;
            }
        }
        return best;
    };

    // ------------------------------------------------------------------------
    // Prefill splitting: conservative 2-piece interleaving only under genuine
    // SLO pressure. Throughput mode stays one-piece, because every additional
    // piece pays another S and can reduce total output rate.
    // ------------------------------------------------------------------------
    auto chooseChunks = [&](int rid, double now) -> int {
        if (throughputMode || numLayers < 4) return 1;
        const int k = req[rid].remote;
        if (k < 0 || k >= K || dprocQ[k].empty()) return 1;
        auto [pt, pp] = pressurePair(now);
        const double full = taskBusy(1, req[rid].Lin);
        const double oneDecode = taskBusy(4, 1);
        if ((pt >= 1.0 || pp >= 1.0) && full >= 3.0 * oneDecode)
            return 2;
        return 1;
    };

    // ------------------------------------------------------------------------
    // Output batching.
    // ------------------------------------------------------------------------
    // Candidate sizes are powers of two plus the full available set.
    // Efficiency includes schedule cost S. We choose the LARGEST candidate
    // whose efficiency is very close to the best; this makes batching stable
    // and strongly biases toward large groups when the table says they are
    // efficient.
    // ------------------------------------------------------------------------
    auto chooseBatch = [&](int column, int n, double pressure) -> int {
        if (n <= 1) return n;
        if (throughputMode) {
            // In high-throughput mode, preserve the proven full-batch behavior
            // unless the table shows a severe collapse for a very large group.
            const int candidatesCap = min(n, 4096);
            int best = 1;
            double bestEff = 0.0;
            int cand = 1;
            while (cand < candidatesCap) {
                double dur = taskBusy(column, cand);
                double eff = double(cand) / max(dur, 1e-9);
                if (eff > bestEff) {
                    bestEff = eff;
                    best = cand;
                }
                if (cand > 2048) break;
                cand <<= 1;
            }
            double fullEff = double(candidatesCap) /
                             max(taskBusy(column, candidatesCap), 1e-9);
            // If full batching is within 96% of the best measured efficiency,
            // take it. That prevents unnecessary fragmentation.
            if (fullEff >= 0.96 * bestEff) return candidatesCap;
            return best;
        }

        // Latency-aware mode: still seek a large efficient batch, but allow a
        // smaller batch when waiting pressure is genuinely high.
        int cap = n;
        if (pressure > 2.0) cap = min(cap, 64);
        else if (pressure > 1.4) cap = min(cap, 128);
        else if (pressure > 1.1) cap = min(cap, 256);

        int best = 1;
        double bestEff = 0.0;
        for (int x = 1; x <= cap; ) {
            double eff = double(x) / max(taskBusy(column, x), 1e-9);
            if (eff > bestEff) {
                bestEff = eff;
                best = x;
            }
            if (x > cap/2) break;
            x <<= 1;
        }
        if (cap >= 1) {
            double eff = double(cap) / max(taskBusy(column, cap), 1e-9);
            if (eff >= 0.94 * bestEff) best = cap;
        }
        return max(1, best);
    };

    // Local output cap is intentionally generous. There is no protocol maximum
    // other than currently ready requests.
    vector<string> out;
    out.reserve(K + 1);

    string token;
    while (cin >> token) {
        if (token == "END") break;
        const double now = stod(token);
        int e;
        cin >> e;

        vector<int> frameDPost;
        frameDPost.reserve(256);

        for (int i = 0; i < e; ++i) {
            string kind;
            cin >> kind;

            if (kind == "ARR") {
                int rid;
                long long lin;
                cin >> rid >> lin;
                ensure(rid);
                req[rid] = Req{};
                req[rid].Lin = lin;
                req[rid].phase = Phase::ARRIVED;
                req[rid].arrival = now;
                arrivalQ.push_back(rid);

                ++tdrPending;
                pendingArrivalSum += now;
            }
            else if (kind == "TDN") {
                string server, major, step;
                cin >> server >> major >> step;

                if (major == "P") {
                    if (step == "PRE") {
                        int remote, rid;
                        double dur;
                        cin >> remote >> rid >> dur;
                        ensure(rid);
                        req[rid].remote = remote;
                        req[rid].phase = Phase::WAIT_UP_PRE;
                        localFree = true;
                    }
                    else if (step == "PROC") {
                        int ls, le, remote, rid;
                        double dur;
                        cin >> ls >> le >> remote >> rid >> dur;
                        ensure(rid);
                        if (0 <= remote && remote < K) {
                            remoteFree[remote] = 1;
                            remoteEnd[remote] = now;
                            remoteDecodeStreak[remote] = 0;
                        }
                        if (le < numLayers) {
                            req[rid].piece++;
                            req[rid].phase = Phase::READY_PPROC;
                            pprocQ[req[rid].remote].push_back(rid);
                        } else {
                            req[rid].phase = Phase::WAIT_DOWN_PRE;
                            ++downPending;
                            downEnd = max(downEnd, now) + xfer(req[rid].Lin);
                        }
                    }
                    else { // P POST
                        int remote, rid;
                        double dur;
                        cin >> remote >> rid >> dur;
                        ensure(rid);
                        req[rid].phase = Phase::READY_DPRE;
                        req[rid].readyDecodeTime = now;

                        tdrDoneSum += now - req[rid].arrival;
                        ++tdrDone;
                        --tdrPending;
                        pendingArrivalSum -= req[rid].arrival;

                        firstDecodeQ.push_back(rid);
                        localFree = true;
                    }
                }
                else { // D
                    if (step == "PRE") {
                        int minus1, m;
                        cin >> minus1 >> m;
                        int cnt[8] = {};
                        for (int j = 0; j < m; ++j) {
                            int rid;
                            cin >> rid;
                            req[rid].phase = Phase::WAIT_UP_DEC;
                            int r = req[rid].remote;
                            if (0 <= r && r < K) ++cnt[r];
                        }
                        double dur;
                        cin >> dur;
                        (void)minus1; (void)dur;
                        localFree = true;

                        // Automatic UP transfers enter the shared FIFO in remote
                        // index order for this D PRE group.
                        for (int r = 0; r < K; ++r) {
                            if (!cnt[r]) continue;
                            ++upPending;
                            upEnd = max(upEnd, now) + xfer(cnt[r]);
                        }
                    }
                    else if (step == "PROC") {
                        int remote, m;
                        cin >> remote >> m;
                        for (int j = 0; j < m; ++j) {
                            int rid;
                            cin >> rid;
                            req[rid].phase = Phase::WAIT_DOWN_DEC;
                        }
                        double dur;
                        cin >> dur;
                        (void)dur;
                        if (0 <= remote && remote < K) {
                            remoteFree[remote] = 1;
                            remoteEnd[remote] = now;
                            remoteDecodeStreak[remote] = 0;
                        }
                        ++downPending;
                        downEnd = max(downEnd, now) + xfer(m);
                    }
                    else { // D POST
                        int minus1, m;
                        cin >> minus1 >> m;
                        frameDPost.resize(frameDPost.size() + m);
                        for (int j = 0; j < m; ++j) cin >> frameDPost[(int)frameDPost.size() - m + j];
                        double dur;
                        cin >> dur;
                        (void)minus1; (void)dur;
                        localFree = true;
                    }
                }
            }
            else if (kind == "XDN") {
                string dir, sub;
                int remote, m;
                long long bytes;
                cin >> dir >> remote >> bytes >> sub >> m;
                (void)bytes;
                vector<int> ids(m);
                for (int j = 0; j < m; ++j) cin >> ids[j];

                if (dir == "UP") {
                    if (upPending > 0) --upPending;
                    if (upPending == 0) upEnd = now;

                    if (sub == "PRE") {
                        int rid = ids[0];
                        req[rid].phase = Phase::READY_PPROC;
                        req[rid].chunks = chooseChunks(rid, now);
                        req[rid].piece = 0;
                        pprocQ[req[rid].remote].push_back(rid);
                    } else {
                        for (int rid : ids) {
                            req[rid].phase = Phase::READY_DPROC;
                            dprocQ[remote].push_back(rid);
                        }
                    }
                } else {
                    if (downPending > 0) --downPending;
                    if (downPending == 0) downEnd = now;

                    if (sub == "PRE") {
                        int rid = ids[0];
                        req[rid].phase = Phase::READY_PPOST;
                        ppostQ.push_back(rid);
                    } else {
                        for (int rid : ids) {
                            req[rid].phase = Phase::READY_DPOST;
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

        // Resolve output-token completions after all FIN lines are known.
        for (int rid : frameDPost) {
            Req& r = req[rid];
            if (r.hasProducedToken) {
                tpotDoneSum += now - r.lastToken;
                ++tpotDone;
                activeLastTokenSum -= r.lastToken;
            } else {
                ++activeGapReqs;
            }

            r.hasProducedToken = true;
            r.lastToken = now;
            activeLastTokenSum += now;

            if (r.finThisFrame) {
                r.phase = Phase::FINISHED;
                r.finThisFrame = false;
                --activeGapReqs;
                activeLastTokenSum -= now;
            } else {
                r.phase = Phase::READY_DPRE;
                repeatDecodeQ.push_back(rid);
            }
        }

        auto [pt, pp] = pressurePair(now);
        out.clear();

        // --------------------------------------------------------------------
        // LOCAL COMPUTER SCHEDULER
        // --------------------------------------------------------------------
        // 1) D POST: if results are already home, finish them immediately.
        // 2) P POST: this completes the input stage and therefore ends TDR.
        // 3) Admission guard: start an ARR-side P PRE when the oldest request
        //    has genuinely starved.
        // 4) First-token D PRE: FIFO protected; never buried behind repeats.
        // 5) Repeat D PRE: throughput engine.
        // 6) Normal P PRE admission.
        // --------------------------------------------------------------------
        if (localFree) {
            if (!dpostQ.empty()) {
                int b = chooseBatch(5, (int)dpostQ.size(), pp);
                ostringstream ss;
                ss << "E D POST -1 " << b;
                for (int i = 0; i < b; ++i) {
                    ss << ' ' << dpostQ.front();
                    dpostQ.pop_front();
                }
                out.push_back(ss.str());
                localFree = false;
            }
            else if (!ppostQ.empty() && postUrgent(now)) {
                int rid = ppostQ.front();
                ppostQ.pop_front();
                ostringstream ss;
                ss << "E P POST " << req[rid].remote << ' ' << rid;
                out.push_back(ss.str());
                localFree = false;
            }
            else if (admissionStarving(now) && !arrivalQ.empty()) {
                int rid = arrivalQ.front();
                arrivalQ.pop_front();
                int k = chooseRemote(rid, now);
                req[rid].remote = k;

                const double preFinish = now + taskBusy(0, req[rid].Lin);
                const double upStart = max(preFinish, upEnd);
                const double upFinish = upStart + xfer(req[rid].Lin);
                remoteEnd[k] = max(remoteEnd[k], upFinish) + taskBusy(1, req[rid].Lin);
                upEnd = upFinish;
                ++upPending;

                ostringstream ss;
                ss << "E P PRE " << k << ' ' << rid;
                out.push_back(ss.str());
                localFree = false;
            }
            else if (!firstDecodeQ.empty()) {
                // Strong first-token FIFO. A newly ready request cannot wait for
                // an arbitrary number of recurring tokens before entering D PRE.
                int b = chooseBatch(3, (int)firstDecodeQ.size(), max(pt,pp));
                // In latency mode keep a small first-token batch; in throughput
                // mode full batching is retained.
                if (!throughputMode && firstTokenWaitingAge(now) > SLO1)
                    b = min(b, max(2, K));

                ostringstream ss;
                ss << "E D PRE -1 " << b;
                for (int i = 0; i < b; ++i) {
                    ss << ' ' << firstDecodeQ.front();
                    firstDecodeQ.pop_front();
                }
                out.push_back(ss.str());
                localFree = false;
            }
            else if (!ppostQ.empty()) {
                int rid = ppostQ.front();
                ppostQ.pop_front();
                ostringstream ss;
                ss << "E P POST " << req[rid].remote << ' ' << rid;
                out.push_back(ss.str());
                localFree = false;
            }
            else if (!repeatDecodeQ.empty()) {
                int b = chooseBatch(3, (int)repeatDecodeQ.size(), pp);
                ostringstream ss;
                ss << "E D PRE -1 " << b;
                for (int i = 0; i < b; ++i) {
                    ss << ' ' << repeatDecodeQ.front();
                    repeatDecodeQ.pop_front();
                }
                out.push_back(ss.str());
                localFree = false;
            }
            else if (!arrivalQ.empty()) {
                // No decode work is waiting, so admit input immediately.
                int rid = arrivalQ.front();
                arrivalQ.pop_front();
                int k = chooseRemote(rid, now);
                req[rid].remote = k;

                const double preFinish = now + taskBusy(0, req[rid].Lin);
                const double upStart = max(preFinish, upEnd);
                const double upFinish = upStart + xfer(req[rid].Lin);
                remoteEnd[k] = max(remoteEnd[k], upFinish) + taskBusy(1, req[rid].Lin);
                upEnd = upFinish;
                ++upPending;

                ostringstream ss;
                ss << "E P PRE " << k << ' ' << rid;
                out.push_back(ss.str());
                localFree = false;
            }
        }

        // --------------------------------------------------------------------
        // REMOTE COMPUTER SCHEDULER
        // --------------------------------------------------------------------
        // Decode is the default because output throughput dominates many tests.
        // Prefill interrupts decode only under a proven starvation condition.
        // The old V5 "every two decode tasks" rule was too strong and is the
        // main reason heavy-output throughput regressed.
        // --------------------------------------------------------------------
        for (int k = 0; k < K; ++k) {
            if (!remoteFree[k]) continue;

            const bool haveD = !dprocQ[k].empty();
            const bool haveP = !pprocQ[k].empty();
            if (!haveD && !haveP) continue;

            bool chooseP = false;

            if (!haveD) {
                chooseP = true;
            }
            else if (!haveP) {
                chooseP = false;
            }
            else {
                // Oldest admitted-but-not-finished input work on this remote.
                // We deliberately use queue age, not global request count.
                double localOldestP = 0.0;
                if (!pprocQ[k].empty()) {
                    int rid = pprocQ[k].front();
                    localOldestP = max(0.0, now - req[rid].arrival);
                }

                const bool severeAge = (!throughputMode &&
                    localOldestP > max(1.5 * SLO1, 10.0 * S));

                const bool queuePressure =
                    pprocQ[k].size() >= max<size_t>(4, dprocQ[k].size() * 3);

                // In throughput mode allow a long decode streak. Only a real
                // admission starvation condition interrupts it.
                int streakLimit = throughputMode ? 8 : 3;
                const bool streakPressure = remoteDecodeStreak[k] >= streakLimit;

                if (severeAge || (queuePressure && streakPressure) ||
                    (admissionStarving(now) && streakPressure)) {
                    chooseP = true;
                } else {
                    chooseP = false; // decode wins by default
                }
            }

            if (chooseP) {
                int rid = pprocQ[k].front();
                pprocQ[k].pop_front();
                Req& r = req[rid];

                int ls = 0;
                int le = numLayers;
                if (r.chunks == 2) {
                    int mid = max(1, numLayers / 2);
                    if (r.piece == 0) le = mid;
                    else { ls = mid; le = numLayers; }
                }
                if (le <= ls) {
                    ls = 0;
                    le = numLayers;
                    r.chunks = 1;
                    r.piece = 0;
                }

                const double frac = double(le-ls) / double(numLayers);
                remoteEnd[k] = now + S + table.lookup(1, r.Lin) * frac;
                remoteDecodeStreak[k] = 0;

                ostringstream ss;
                ss << 'C' << k << " P PROC " << ls << ' ' << le
                   << ' ' << k << ' ' << rid;
                out.push_back(ss.str());
                remoteFree[k] = 0;
            }
            else {
                int b = chooseBatch(4, (int)dprocQ[k].size(), pp);
                ostringstream ss;
                ss << 'C' << k << " D PROC " << k << ' ' << b;
                for (int i = 0; i < b; ++i) {
                    ss << ' ' << dprocQ[k].front();
                    dprocQ[k].pop_front();
                }
                out.push_back(ss.str());
                remoteFree[k] = 0;
                remoteEnd[k] = now + taskBusy(4, b);
                ++remoteDecodeStreak[k];
            }
        }

        cout << out.size() << '\n';
        for (const string& s : out) cout << s << '\n';
        cout.flush();
    }

    return 0;
}

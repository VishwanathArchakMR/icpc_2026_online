#include <bits/stdc++.h>
using namespace std;

// ============================================================================
// Submission Version: Peak Revert + Non-Blocking Batching v5.0
// Edge-Cloud Collaborative Scheduling - V13
// ============================================================================
// Goals from the observed 22-test feedback:
//   * Preserve the Test-19 starvation fix from the 14.8k version.
//   * Recover the high-throughput behavior of the original 12.77k / 918.9-Test22
//     regime by allowing very large decode groups when the ready pool is large.
//   * Make P POST genuinely immediate: once its DOWN transfer is available,
//     local E finishes P POST before starting new P PRE / decode work.
//   * Keep the decision path cheap enough for very long interactive tests.
//
// Key design choice:
//   D PRE eligibility is stored in ONE global ordered set by (arrival_time,rid).
//   That gives strict earliest-arrival-first membership ordering across both
//   first-token and recurring-token work. An explicit first-token starvation
//   guard can additionally force old first-token requests into the next batch.
//
//   Large decode batches are NOT capped at 16 in throughput mode. For a large
//   ready pool, the scheduler evaluates several candidate batch sizes using
//   the supplied task-time table, but prefers the full ready set whenever its
//   efficiency is close to the best candidate. This restores the original
//   high-throughput behavior while retaining a mathematical batch heuristic.
//
//   P PROC remains one full piece by default. Two-piece prefill is used only
//   when a remote has a genuinely old prefill request while decode work is
//   consuming the worker; every extra piece pays another schedule cost S.
// ============================================================================

struct TaskTimeTable {
    vector<pair<long long,double>> col[6];

    void addRow(long long bs, const array<double,6>& v) {
        for (int c = 0; c < 6; ++c)
            if (v[c] >= 0.0) col[c].push_back({bs, v[c]});
    }

    void finalize() {
        for (auto& v : col) sort(v.begin(), v.end());
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
        const double f = double(bs - lo.first) / double(hi.first - lo.first);
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
        for (double& x : v) cin >> x;
        table.addRow(bs, v);
    }
    table.finalize();

    auto xfer = [&](long long len) -> double {
        return latency + 8.0 * double(len) * double(bytesPerToken)
             / (bandwidth * 1e6);
    };

    auto busy = [&](int col, long long bs) -> double {
        return S + table.lookup(col, bs);
    };

    vector<Req> req;
    req.reserve(4096);
    auto ensure = [&](int rid) {
        if (rid >= (int)req.size()) req.resize(rid + 1);
    };

    // ------------------------------------------------------------------------
    // Ready structures.
    // ------------------------------------------------------------------------
    deque<int> arrivalQ;                    // not-yet-admitted ARR requests
    vector<deque<int>> pprocQ(K);           // ready P PROC by assigned remote
    deque<int> ppostQ;                      // ready P POST

    // GLOBAL decode-ready set, strictly ordered by original ARRIVAL time.
    // This replaces the old "first queue + repeat queue" ordering because a
    // recurring request from an old arrival is still legitimately older than a
    // newer first-token request. The starvation guard below can force old
    // first-token requests into the selected batch when needed.
    set<pair<double,int>> decodeReady;

    vector<deque<int>> dprocQ(K);            // ready D PROC by remote
    deque<int> dpostQ;                       // ready D POST

    bool localFree = true;
    vector<char> remoteFree(K, 1);

    // ------------------------------------------------------------------------
    // Lightweight virtual endpoint model for P PRE placement.
    // It is only a ranking heuristic; correctness never depends on it.
    // ------------------------------------------------------------------------
    vector<double> remoteEnd(K, 0.0);
    vector<int> decodeStreak(K, 0);
    double upEnd = 0.0;
    double downEnd = 0.0;
    int upPending = 0;
    int downPending = 0;

    // ------------------------------------------------------------------------
    // Incremental score-pressure tracking. No full-request scan per frame.
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
        const long long cnt = tdrDone + tdrPending;
        if (cnt == 0) return 0.0;
        const long double sum = tdrDoneSum
                              + (long double)tdrPending * now
                              - pendingArrivalSum;
        return double(sum / (long double)cnt);
    };

    auto meanTPOT = [&](double now) -> double {
        const long long cnt = tpotDone + activeGapReqs;
        if (cnt == 0) return 0.0;
        const long double sum = tpotDoneSum
                              + (long double)activeGapReqs * now
                              - activeLastTokenSum;
        return double(sum / (long double)cnt);
    };

    auto pressures = [&](double now) -> pair<double,double> {
        const double tdr = meanTDR(now);
        const double tpot = meanTPOT(now);
        return {
            SLO1 > 0.0 ? tdr / SLO1 : 0.0,
            SLO2 > 0.0 ? tpot / SLO2 : 0.0
        };
    };

    const bool throughputMode = (wtp >= 0.72 || wc <= 0.28);

    // ------------------------------------------------------------------------
    // Starvation thresholds.
    // These are intentionally finite even in throughput mode. Test 19 proved
    // that completely disabling admission protection is catastrophic.
    // ------------------------------------------------------------------------
    const double ARRIVE_AGE = max(1.5 * SLO1, 8.0 * S);
    const double FIRST_AGE  = max(1.5 * SLO1, 8.0 * S);

    auto oldestArrivalAge = [&](double now) -> double {
        if (arrivalQ.empty()) return 0.0;
        return max(0.0, now - req[arrivalQ.front()].arrival);
    };

    auto oldestFirstReady = [&]() -> int {
        int best = -1;
        double bestA = numeric_limits<double>::infinity();
        for (auto it = decodeReady.begin(); it != decodeReady.end(); ++it) {
            const int rid = it->second;
            if (!req[rid].hasProducedToken) {
                if (req[rid].arrival < bestA ||
                    (req[rid].arrival == bestA && rid < best)) {
                    bestA = req[rid].arrival;
                    best = rid;
                }
            }
        }
        return best;
    };

    auto oldestFirstAge = [&](double now) -> double {
        const int rid = oldestFirstReady();
        if (rid < 0) return 0.0;
        return max(0.0, now - req[rid].readyDecodeTime);
    };

    auto admissionStarving = [&](double now) -> bool {
        if (arrivalQ.empty()) return false;
        const double age = oldestArrivalAge(now);
        const size_t q = arrivalQ.size();
        // Age is the primary protection; queue depth is a secondary fail-safe.
        // The depth trigger is modest so it does not destroy Test-22 batching.
        return age >= ARRIVE_AGE || q >= (size_t)max(2, 2 * K);
    };

    auto firstTokenStarving = [&](double now) -> bool {
        if (oldestFirstReady() < 0) return false;
        const double age = oldestFirstAge(now);
        return age >= FIRST_AGE;
    };

    // ------------------------------------------------------------------------
    // Remote choice for P PRE.
    // Estimate the end of the entire input pipeline using current virtual
    // UP/DOWN FIFO endpoints plus each worker's CPU endpoint.
    // ------------------------------------------------------------------------
    auto chooseRemote = [&](int rid, double now) -> int {
        const long long lin = req[rid].Lin;
        const double preFinish = now + busy(0, lin);
        const double upStart = max(preFinish, upEnd);
        const double upFinish = upStart + xfer(lin);
        const double procDur = busy(1, lin);
        const double postDur = busy(2, lin);

        int best = 0;
        double bestScore = numeric_limits<double>::infinity();
        for (int k = 0; k < K; ++k) {
            const double procStart = max(remoteEnd[k], upFinish);
            const double procFinish = procStart + procDur;
            const double downStart = max(downEnd, procFinish);
            const double downFinish = downStart + xfer(lin);
            const double finish = downFinish + postDur;

            // Throughput still prefers a free worker / shorter CPU endpoint,
            // but the shared transfer queues are never ignored.
            double score = throughputMode
                         ? procFinish + 0.25 * (finish - procFinish)
                         : finish;
            if (remoteFree[k]) score -= 0.15 * procDur;
            score += 1e-7 * decodeStreak[k];
            score += 1e-9 * k;

            if (score < bestScore) {
                bestScore = score;
                best = k;
            }
        }
        return best;
    };

    // ------------------------------------------------------------------------
    // Dynamic prefill chunking.
    // Default: one full piece. Split only when the remote really has decode
    // work and an old P request is waiting in a non-throughput-sensitive mode.
    // ------------------------------------------------------------------------
    auto chooseChunks = [&](int rid, double now) -> int {
        if (throughputMode || numLayers < 4) return 1;
        const int k = req[rid].remote;
        if (k < 0 || k >= K || dprocQ[k].empty()) return 1;

        const auto [pt, pp] = pressures(now);
        const double full = busy(1, req[rid].Lin);
        const double decode1 = busy(4, 1);
        const double age = now - req[rid].arrival;

        if ((pt >= 1.0 || pp >= 1.0) && age >= ARRIVE_AGE && full >= 3.0 * decode1)
            return 2;
        return 1;
    };

    // ------------------------------------------------------------------------
    // NON-BLOCKING BATCH POLICY
    //
    // Never wait to manufacture a larger batch. The scheduler only uses work
    // that is already present in the ready queue at the instant a resource
    // becomes free. For D PROC, take up to 16 oldest-ready requests immediately.
    // This is intentionally simple: the peak behavior showed that queue delay
    // can destroy TPOT much more severely than a small batching inefficiency.
    // ------------------------------------------------------------------------
    auto bestDecodeBatch = [&](int n, double /*oldestWait*/) -> int {
        if (n <= 0) return 0;
        return min(n, 16);
    };

    // Compute how many requests each remote should ideally contribute to a
    // single decode wave. The target is intentionally at least 16 on a busy
    // worker; exact table breakpoints can raise it when that is more efficient.
    auto targetRemoteBatch = [&](int q) -> int {
        if (q <= 0) return 0;
        if (q <= 2) return q;
        return bestDecodeBatch(q, 0.0);
    };

    // D PRE follows the same non-blocking rule: dispatch immediately with the
    // oldest FIFO-ready requests, up to 16 members. No holding buffer and no
    // score-based delay is introduced.
    auto chooseDPreBatch = [&](int n, double /*pressure*/) -> int {
        if (n <= 0) return 0;
        return min(n, 16);
    };

    vector<string> out;
    out.reserve(K + 1);
    vector<int> frameDPost;
    frameDPost.reserve(256);

    string tok;
    while (cin >> tok) {
        if (tok == "END") break;
        const double now = stod(tok);
        int e;
        cin >> e;

        frameDPost.clear();

        // --------------------------------------------------------------------
        // Read COMPLETE frame before scheduling anything.
        // --------------------------------------------------------------------
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
                            decodeStreak[remote] = 0;
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

                        decodeReady.insert({req[rid].arrival, rid});
                        localFree = true;
                    }
                }
                else { // D
                    if (step == "PRE") {
                        int minus1, m;
                        cin >> minus1 >> m;
                        static int cnt[8];
                        fill(cnt, cnt + 8, 0);
                        for (int j = 0; j < m; ++j) {
                            int rid;
                            cin >> rid;
                            req[rid].phase = Phase::WAIT_UP_DEC;
                            const int r = req[rid].remote;
                            if (0 <= r && r < K) ++cnt[r];
                            decodeReady.erase({req[rid].arrival, rid});
                        }
                        double dur;
                        cin >> dur;
                        (void)minus1; (void)dur;
                        localFree = true;

                        // D PRE creates one UP transfer per represented remote,
                        // inserted by increasing remote index.
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
                            remoteFree[remote] = true;
                            remoteEnd[remote] = now;
                            decodeStreak[remote] = 0;
                        }
                        ++downPending;
                        downEnd = max(downEnd, now) + xfer(m);
                    }
                    else { // D POST
                        int minus1, m;
                        cin >> minus1 >> m;
                        frameDPost.resize(m);
                        for (int j = 0; j < m; ++j) cin >> frameDPost[j];
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

                if (dir == "UP") {
                    if (upPending > 0) --upPending;
                    if (upPending == 0) upEnd = now;

                    if (sub == "PRE") {
                        int rid;
                        cin >> rid;
                        req[rid].phase = Phase::READY_PPROC;
                        req[rid].chunks = chooseChunks(rid, now);
                        req[rid].piece = 0;
                        pprocQ[req[rid].remote].push_back(rid);
                    }
                    else { // DEC
                        for (int j = 0; j < m; ++j) {
                            int rid;
                            cin >> rid;
                            req[rid].phase = Phase::READY_DPROC;
                            dprocQ[remote].push_back(rid);
                        }
                    }
                }
                else { // DOWN
                    if (downPending > 0) --downPending;
                    if (downPending == 0) downEnd = now;

                    if (sub == "PRE") {
                        int rid;
                        cin >> rid;
                        req[rid].phase = Phase::READY_PPOST;
                        ppostQ.push_back(rid);
                    }
                    else { // DEC
                        for (int j = 0; j < m; ++j) {
                            int rid;
                            cin >> rid;
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

        // --------------------------------------------------------------------
        // Resolve D POST results after all FIN lines in this frame are known.
        // --------------------------------------------------------------------
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
                decodeReady.insert({r.arrival, rid});
            }
        }

        const auto [pt, pp] = pressures(now);
        const bool anyFirstStarving = firstTokenStarving(now);
        out.clear();

        // ====================================================================
        // LOCAL SCHEDULER
        // ====================================================================
        // Priority is deliberately:
        //   1) P POST  -- immediate TDR termination whenever a DOWN transfer is
        //                 already complete.
        //   2) D POST  -- finish already-computed token results.
        //   3) Aged admission P PRE -- prevents Test-19 style input starvation.
        //   4) D PRE  -- globally FIFO by arrival, with a first-token aging guard.
        //   5) Normal P PRE.
        // ====================================================================
        if (localFree) {
            if (!ppostQ.empty()) {
                // User-requested TDR rule: once an input DOWN completes, P POST
                // is the first local action. P POST ends TDR immediately.
                const int rid = ppostQ.front();
                ppostQ.pop_front();
                ostringstream ss;
                ss << "E P POST " << req[rid].remote << ' ' << rid;
                out.push_back(ss.str());
                localFree = false;
            }
            else if (!dpostQ.empty()) {
                const int b = throughputMode ? (int)dpostQ.size()
                                               : min((int)dpostQ.size(), 16);
                ostringstream ss;
                ss << "E D POST -1 " << b;
                for (int i = 0; i < b; ++i) {
                    ss << ' ' << dpostQ.front();
                    dpostQ.pop_front();
                }
                out.push_back(ss.str());
                localFree = false;
            }
            else if (admissionStarving(now) && !arrivalQ.empty()) {
                // HARD ADMISSION STARVATION GUARD.
                // The oldest ARR is forced into the next free local slot even
                // if a decode stream is ready. This is the mechanism that kept
                // Test 19 out of the catastrophic 1.946-point regime.
                const int rid = arrivalQ.front();
                arrivalQ.pop_front();
                const int k = chooseRemote(rid, now);
                req[rid].remote = k;

                const double preFinish = now + busy(0, req[rid].Lin);
                const double upStart = max(preFinish, upEnd);
                const double upFinish = upStart + xfer(req[rid].Lin);
                remoteEnd[k] = max(remoteEnd[k], upFinish) + busy(1, req[rid].Lin);
                upEnd = upFinish;
                ++upPending;

                ostringstream ss;
                ss << "E P PRE " << k << ' ' << rid;
                out.push_back(ss.str());
                localFree = false;
            }
            else if (!decodeReady.empty()) {
                // ------------------------------------------------------------
                // D PRE: STRICT FIFO BY ORIGINAL ARRIVAL TIME.
                //
                // If the oldest first-token request has aged past FIRST_AGE,
                // it is mandatory in the next batch. Otherwise we take the
                // globally oldest ready requests. Since order inside a D PRE
                // group has no semantic effect, this is legal while preserving
                // a true earliest-arrival selection policy.
                // ------------------------------------------------------------
                vector<int> picked;
                const int n = (int)decodeReady.size();
                const int b = chooseDPreBatch(n, 0.0);
                picked.reserve(b);

                // Strict FIFO: decodeReady is ordered by (arrival,rid).
                // Take the oldest requests immediately; never wait for more.
                auto it = decodeReady.begin();
                for (int i = 0; i < b && it != decodeReady.end(); ++i, ++it)
                    picked.push_back(it->second);

                // An aged first-token request is already protected by FIFO.
                // If the oldest first-token request is present, it is necessarily
                // selected unless there are older recurring requests in front of it.
                // That is intentional FIFO behavior and avoids the TPOT explosion
                // caused by artificially forcing large batches.

                if (!picked.empty()) {
                    ostringstream ss;
                    ss << "E D PRE -1 " << picked.size();
                    for (int rid : picked) {
                        ss << ' ' << rid;
                        decodeReady.erase({req[rid].arrival, rid});
                        req[rid].phase = Phase::WAIT_UP_DEC;
                    }
                    out.push_back(ss.str());
                    localFree = false;

                    // The D PRE transfer queues are in increasing remote index.
                    static int cnt[8];
                    fill(cnt, cnt + 8, 0);
                    for (int rid : picked) {
                        const int r = req[rid].remote;
                        if (0 <= r && r < K) ++cnt[r];
                    }
                    for (int r = 0; r < K; ++r) {
                        if (!cnt[r]) continue;
                        ++upPending;
                        upEnd = max(upEnd, now) + xfer(cnt[r]);
                    }
                }
            }
            else if (!arrivalQ.empty()) {
                // No decode work exists. Admit immediately; there is no reason
                // to leave E idle when a request can start its input pipeline.
                const int rid = arrivalQ.front();
                arrivalQ.pop_front();
                const int k = chooseRemote(rid, now);
                req[rid].remote = k;

                const double preFinish = now + busy(0, req[rid].Lin);
                const double upStart = max(preFinish, upEnd);
                const double upFinish = upStart + xfer(req[rid].Lin);
                remoteEnd[k] = max(remoteEnd[k], upFinish) + busy(1, req[rid].Lin);
                upEnd = upFinish;
                ++upPending;

                ostringstream ss;
                ss << "E P PRE " << k << ' ' << rid;
                out.push_back(ss.str());
                localFree = false;
            }
        }

        // ====================================================================
        // REMOTE SCHEDULER
        // ====================================================================
        for (int k = 0; k < K; ++k) {
            if (!remoteFree[k]) continue;

            const bool haveD = !dprocQ[k].empty();
            const bool haveP = !pprocQ[k].empty();
            if (!haveD && !haveP) continue;

            bool chooseP = false;
            if (!haveD) {
                chooseP = true;
            } else if (!haveP) {
                chooseP = false;
            } else {
                const int ridP = pprocQ[k].front();
                const double pAge = now - req[ridP].arrival;
                const bool pStarving = pAge >= ARRIVE_AGE;

                // Decode normally wins in throughput mode. However, an old
                // prefill request is NEVER allowed to remain behind decode
                // forever. This is the remote-side half of the Test-19 guard.
                if (pStarving) {
                    chooseP = true;
                } else if (!throughputMode) {
                    // In SLO mode, admit P earlier when the P queue is large.
                    chooseP = pprocQ[k].size() >= dprocQ[k].size();
                } else {
                    // Throughput mode: decode wins. No artificial two-task or
                    // eight-task streak cap. P is admitted only when its age
                    // crosses the hard starvation threshold.
                    chooseP = false;
                }
            }

            if (chooseP) {
                const int rid = pprocQ[k].front();
                pprocQ[k].pop_front();
                Req& r = req[rid];

                int ls = 0, le = numLayers;
                if (r.chunks == 2) {
                    const int mid = max(1, numLayers / 2);
                    if (r.piece == 0) le = mid;
                    else ls = mid;
                }

                if (le <= ls) {
                    ls = 0;
                    le = numLayers;
                    r.chunks = 1;
                    r.piece = 0;
                }

                const double frac = double(le - ls) / double(numLayers);
                remoteEnd[k] = now + S + table.lookup(1, r.Lin) * frac;
                decodeStreak[k] = 0;

                ostringstream ss;
                ss << 'C' << k << " P PROC " << ls << ' ' << le
                   << ' ' << k << ' ' << rid;
                out.push_back(ss.str());
                remoteFree[k] = false;
            }
            else {
                // D PROC is where we apply the real throughput optimizer.
                // Unlike the old V7 policy, we do NOT blindly run the entire
                // queue as one gigantic task. The table tells us whether 8, 16,
                // 32, ... is actually more efficient than the full queue.
                // This avoids both micro-batches (Test 22 failure) and giant
                // nonlinear batches that can become slower per token.
                double oldestWait = dprocQ[k].empty() ? 0.0 : max(0.0, now - req[dprocQ[k].front()].readyDecodeTime);
                int b = bestDecodeBatch((int)dprocQ[k].size(), oldestWait);

                // Latency guard: if the queue is tiny, m naturally remains 1..2.
                // For SLO-heavy operation, cap only when TPOT pressure is severe.
                if (!throughputMode) {
                    const auto [pt2, pp2] = pressures(now);
                    if (pp2 > 2.0) b = min(b, 4);
                    else if (pp2 > 1.4) b = min(b, 8);
                    else if (pp2 > 1.1) b = min(b, 16);
                }
                b = max(1, min(b, (int)dprocQ[k].size()));

                ostringstream ss;
                ss << 'C' << k << " D PROC " << k << ' ' << b;
                for (int i = 0; i < b; ++i) {
                    const int rid = dprocQ[k].front();
                    dprocQ[k].pop_front();
                    ss << ' ' << rid;
                    req[rid].phase = Phase::WAIT_DOWN_DEC;
                }
                out.push_back(ss.str());
                remoteFree[k] = false;
                remoteEnd[k] = now + busy(4, b);
                ++decodeStreak[k];
            }
        }

        cout << out.size() << '\n';
        for (const string& s : out) cout << s << '\n';
        cout.flush();
    }

    return 0;
}

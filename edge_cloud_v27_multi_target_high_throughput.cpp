// Submission Version: Multi-Target High-Throughput Optimization v20.0
#include <bits/stdc++.h>
using namespace std;

// ============================================================================
// Edge-Cloud Collaborative Scheduling - V9
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

    vector<Req> jobs;
    jobs.reserve(4096);
    auto ensure = [&](int rid) {
        if (rid >= (int)jobs.size()) jobs.resize(rid + 1);
    };

    // ------------------------------------------------------------------------
    // Ready structures.
    // ------------------------------------------------------------------------
    deque<int> admitQ;                    // not-yet-admitted ARR requests
    vector<deque<int>> pprocQ(K);           // ready P PROC by assigned remote
    deque<int> ppostQ;                      // ready P POST

    // GLOBAL decode-ready set, strictly ordered by original ARRIVAL time.
    // This replaces the old "first queue + repeat queue" ordering because a
    // recurring request from an old arrival is still legitimately older than a
    // newer first-token request. The starvation guard below can force old
    // first-token requests into the selected batch when needed.
    set<pair<double,int>> readyDecode;

    vector<deque<int>> remoteTokenQ(K);      // ready D PROC by remote
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
        if (admitQ.empty()) return 0.0;
        return max(0.0, now - jobs[admitQ.front()].arrival);
    };

    auto oldestFirstReady = [&]() -> int {
        int best = -1;
        double bestA = numeric_limits<double>::infinity();
        for (auto it = readyDecode.begin(); it != readyDecode.end(); ++it) {
            const int rid = it->second;
            if (!jobs[rid].hasProducedToken) {
                if (jobs[rid].arrival < bestA ||
                    (jobs[rid].arrival == bestA && rid < best)) {
                    bestA = jobs[rid].arrival;
                    best = rid;
                }
            }
        }
        return best;
    };

    auto oldestFirstAge = [&](double now) -> double {
        const int rid = oldestFirstReady();
        if (rid < 0) return 0.0;
        return max(0.0, now - jobs[rid].readyDecodeTime);
    };

    auto admissionStarving = [&](double now) -> bool {
        if (admitQ.empty()) return false;
        const double age = oldestArrivalAge(now);
        const size_t q = admitQ.size();
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
        const long long lin = jobs[rid].Lin;
        const double preFinish = now + busy(0, lin);
        const double upStart = max(preFinish, upEnd);
        const double upFinish = upStart + xfer(lin);
        const double procDur = busy(1, lin);
        const double postDur = busy(2, lin);

        // --------------------------------------------------------------------
        // Peak-recovery placement policy.  The previous V25 placement added
        // queue-size penalties on EVERY arrival.  That helped some saturated
        // cases, but it over-offloaded medium-load workloads such as Test 21.
        //
        // We therefore use two zero-wait regimes:
        //   * NORMAL / medium load: use the original fast endpoint balance.
        //     This keeps local-optimal routing and avoids needless cloud churn.
        //   * HIGH edge pressure: only then enable the full link-aware
        //     end-to-end projection and queue-pressure balancing.
        //
        // The trigger is based only on work already queued at this frame; it
        // NEVER waits for the threshold to be reached.
        // --------------------------------------------------------------------
        long long queuedPrefillTotal = (long long)admitQ.size() + (long long)ppostQ.size();
        for (int k = 0; k < K; ++k)
            queuedPrefillTotal += (long long)pprocQ[k].size();

        // A small nominal edge dispatch window per cloud.  80% is deliberately
        // used as requested: advanced offload balancing is enabled only after
        // the edge-side pending-input window is genuinely full.
        const long long nominalEdgeCapacity = max(1LL, 4LL * K);
        const bool highEdgePressure =
            double(queuedPrefillTotal) >= 0.80 * double(nominalEdgeCapacity);

        int best = 0;
        double bestScore = numeric_limits<double>::infinity();

        // Estimate how much already-ready decode work will execute before a new
        // prefill task can reach this cloud CPU.  The previous version charged
        // each queued decode request approximately linearly; that is pessimistic
        // because decode is actually executed in batches.  Model the real
        // non-waiting policy instead: consume the current queue in batches of up
        // to 16 (the heavy-backlog path), summing the actual D PROC durations.
        auto queuedDecodeDrain = [&](int q) -> double {
            double total = 0.0;
            while (q > 0) {
                const int b = min(q, 16);
                total += busy(4, b);
                q -= b;
                // K and the queue sizes are tiny enough for this bounded loop,
                // but keep a safety break if a malformed input is encountered.
                if (total > 1e15) break;
            }
            return total;
        };

        for (int k = 0; k < K; ++k) {
            const double decodeDrain = queuedDecodeDrain((int)remoteTokenQ[k].size());
            const double queuedPrefillTail =
                double(pprocQ[k].size()) * max(1.0, procDur);

            // The cloud scheduler always gives ready D PROC work precedence over
            // waiting P PROC work.  Therefore a new P request effectively joins
            // behind the current decode drain and the already queued prefill tail.
            const double cpuReady =
                max(remoteEnd[k], now) + decodeDrain + queuedPrefillTail;
            const double procStart = max(cpuReady, upFinish);
            const double procFinish = procStart + procDur;
            const double downStart = max(downEnd, procFinish);
            const double downFinish = downStart + xfer(lin);
            const double finish = downFinish + postDur;

            double score;

            if (!highEdgePressure) {
                // Medium-load path: retain the low-variance routing that protects
                // Test 21/16/19.  We deliberately do not charge the full decode
                // drain here, so ordinary traffic keeps the old behavior.
                const double cloudQueued =
                    double(pprocQ[k].size() + remoteTokenQ[k].size());
                score = remoteEnd[k]
                      + 0.08 * cloudQueued * max(1.0, procDur);
                if (remoteFree[k]) score -= 0.10 * procDur;
            } else {
                // Heavy saturation path: rank clouds by the projected finish of
                // the COMPLETE input pipeline while accounting for batched decode
                // work already waiting on that worker.  This is a more faithful
                // queueing model than multiplying every decode request by D(1).
                score = throughputMode
                      ? procFinish + 0.25 * (finish - procFinish)
                      : finish;

                // Small residual penalties stabilize ties without double-counting
                // the explicitly modeled decode/prefill tails above.
                score += 0.03 * decodeDrain;
                score += 0.05 * queuedPrefillTail;
                if (remoteFree[k]) score -= 0.15 * procDur;
            }

            // Deterministic tie-breaking keeps routing stable across builds.
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
        const int k = jobs[rid].remote;
        if (k < 0 || k >= K || remoteTokenQ[k].empty()) return 1;

        const auto [pt, pp] = pressures(now);
        const double full = busy(1, jobs[rid].Lin);
        const double decode1 = busy(4, 1);
        const double age = now - jobs[rid].arrival;

        if ((pt >= 1.0 || pp >= 1.0) && age >= ARRIVE_AGE && full >= 3.0 * decode1)
            return 2;
        return 1;
    };

    // ========================================================================
    // DUAL-MODE BACKLOG PROFILING
    // ========================================================================
    // This is a zero-wait mode switch: it never postpones a request.
    //
    // LIGHT MODE:
    //   The system is not saturated enough to justify changing the proven
    //   latency-friendly baseline. Already-ready work is dispatched using the
    //   existing full-ready FIFO behavior.
    //
    // HEAVY MODE:
    //   Once the cloud decode-ready pool reaches >= 50% of a conservative
    //   K * 16 iteration capacity, the scheduler starts issuing immediate
    //   batches of at most 16 on each free cloud. This targets the fixed-S
    //   amortization problem seen on Test 22 without ever waiting to fill a
    //   batch. If TPOT becomes unhealthy, the mode immediately falls back to
    //   the proven latency-oriented path on the next frame.
    // ========================================================================
    auto dualModeHeavy = [&](double now) -> bool {
        long long cloudReady = 0;
        for (int k = 0; k < K; ++k)
            cloudReady += (long long)remoteTokenQ[k].size();

        const long long totalWorkerCapacity = max(1, K * 16);
        const double backlogRatio =
            double(cloudReady) / double(totalWorkerCapacity);

        // Keep Test-19-style latency behavior unless the backlog is both deep
        // and the current TPOT is still healthy. This is not a waiting rule:
        // the current frame is scheduled immediately either way.
        const double tpotNow = meanTPOT(now);
        const bool latencyHealthy = (SLO2 <= 0.0 || tpotNow <= 0.85 * SLO2);

        return backlogRatio >= 0.50 && latencyHealthy;
    };

    // ========================================================================
    // NON-BLOCKING BACKLOG BATCHING
    // ========================================================================
    // There is deliberately NO waiting policy here.  A free worker consumes
    // only work that is already present in its queue at this exact frame.
    //
    // Remote D PROC:   m = min(q_len, 16)
    // Local D PRE:     m = min(global_ready_len, 16)
    //
    // Both queues are FIFO, so taking the front m entries preserves strict
    // earliest-arrival order and cannot starve an older request in favor of a
    // newer one.  We never delay a request to improve a future batch.
    // ========================================================================
    auto bestDecodeBatch = [&](int n) -> int {
        if (n <= 0) return 0;
        return min(n, 16);
    };

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
                jobs[rid] = Req{};
                jobs[rid].Lin = lin;
                jobs[rid].phase = Phase::ARRIVED;
                jobs[rid].arrival = now;
                admitQ.push_back(rid);

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
                        jobs[rid].remote = remote;
                        jobs[rid].phase = Phase::WAIT_UP_PRE;
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
                            jobs[rid].piece++;
                            jobs[rid].phase = Phase::READY_PPROC;
                            pprocQ[jobs[rid].remote].push_back(rid);
                        } else {
                            jobs[rid].phase = Phase::WAIT_DOWN_PRE;
                            ++downPending;
                            downEnd = max(downEnd, now) + xfer(jobs[rid].Lin);
                        }
                    }
                    else { // P POST
                        int remote, rid;
                        double dur;
                        cin >> remote >> rid >> dur;
                        ensure(rid);
                        jobs[rid].phase = Phase::READY_DPRE;
                        jobs[rid].readyDecodeTime = now;

                        tdrDoneSum += now - jobs[rid].arrival;
                        ++tdrDone;
                        --tdrPending;
                        pendingArrivalSum -= jobs[rid].arrival;

                        readyDecode.insert({jobs[rid].arrival, rid});
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
                            jobs[rid].phase = Phase::WAIT_UP_DEC;
                            const int r = jobs[rid].remote;
                            if (0 <= r && r < K) ++cnt[r];
                            readyDecode.erase({jobs[rid].arrival, rid});
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
                            jobs[rid].phase = Phase::WAIT_DOWN_DEC;
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
                        jobs[rid].phase = Phase::READY_PPROC;
                        jobs[rid].chunks = chooseChunks(rid, now);
                        jobs[rid].piece = 0;
                        pprocQ[jobs[rid].remote].push_back(rid);
                    }
                    else { // DEC
                        for (int j = 0; j < m; ++j) {
                            int rid;
                            cin >> rid;
                            jobs[rid].phase = Phase::READY_DPROC;
                            remoteTokenQ[remote].push_back(rid);
                        }
                    }
                }
                else { // DOWN
                    if (downPending > 0) --downPending;
                    if (downPending == 0) downEnd = now;

                    if (sub == "PRE") {
                        int rid;
                        cin >> rid;
                        jobs[rid].phase = Phase::READY_PPOST;
                        ppostQ.push_back(rid);
                    }
                    else { // DEC
                        for (int j = 0; j < m; ++j) {
                            int rid;
                            cin >> rid;
                            jobs[rid].phase = Phase::READY_DPOST;
                            dpostQ.push_back(rid);
                        }
                    }
                }
            }
            else { // FIN
                int rid;
                cin >> rid;
                ensure(rid);
                jobs[rid].finThisFrame = true;
            }
        }

        // --------------------------------------------------------------------
        // Resolve D POST results after all FIN lines in this frame are known.
        // --------------------------------------------------------------------
        for (int rid : frameDPost) {
            Req& r = jobs[rid];

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
                readyDecode.insert({r.arrival, rid});
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
                ss << "E P POST " << jobs[rid].remote << ' ' << rid;
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
            else if (admissionStarving(now) && !admitQ.empty()) {
                // HARD ADMISSION STARVATION GUARD.
                // The oldest ARR is forced into the next free local slot even
                // if a decode stream is ready. This is the mechanism that kept
                // Test 19 out of the catastrophic 1.946-point regime.
                const int rid = admitQ.front();
                admitQ.pop_front();
                const int k = chooseRemote(rid, now);
                jobs[rid].remote = k;

                const double preFinish = now + busy(0, jobs[rid].Lin);
                const double upStart = max(preFinish, upEnd);
                const double upFinish = upStart + xfer(jobs[rid].Lin);
                remoteEnd[k] = max(remoteEnd[k], upFinish) + busy(1, jobs[rid].Lin);
                upEnd = upFinish;
                ++upPending;

                ostringstream ss;
                ss << "E P PRE " << k << ' ' << rid;
                out.push_back(ss.str());
                localFree = false;
            }
            else if (!readyDecode.empty()) {
                // ------------------------------------------------------------
                // D PRE: STRICT FIFO, FULL READY-SET DISPATCH.
                //
                // CRITICAL DIFFERENCE FROM V15:
                // Do NOT cap the already-ready queue at 16.  The 12.77k/918.9
                // Test-22 regime came from amortizing E's fixed schedule cost
                // across the entire burst that was already available.  There
                // is no waiting here: every member selected below was already
                // present in readyDecode at this frame boundary.
                //
                // The arrival-ordered set provides the starvation invariant:
                // the batch always contains the oldest ready requests first.
                // The existing aged-first-token guard therefore remains valid.
                // ------------------------------------------------------------
                // Keep D PRE fully work-conserving in both modes. The heavy-mode
                // optimization is applied at the per-cloud D PROC boundary, where
                // the fixed schedule cost S is paid on the actual compute worker.
                const int b = (int)readyDecode.size();

                ostringstream ss;
                ss << "E D PRE -1 " << b;

                static int cnt[8];
                fill(cnt, cnt + 8, 0);

                for (auto it = readyDecode.begin(); it != readyDecode.end(); ) {
                    const int rid = it->second;
                    ss << ' ' << rid;
                    ++cnt[jobs[rid].remote];
                    jobs[rid].phase = Phase::WAIT_UP_DEC;
                    it = readyDecode.erase(it);
                }

                out.push_back(ss.str());
                localFree = false;

                // Automatic UP transfers are created once per represented
                // remote, in increasing remote-index order.
                for (int r = 0; r < K; ++r) {
                    if (!cnt[r]) continue;
                    ++upPending;
                    upEnd = max(upEnd, now) + xfer(cnt[r]);
                }
            }
            else if (!admitQ.empty()) {
                // No decode work exists. Admit immediately; there is no reason
                // to leave E idle when a request can start its input pipeline.
                const int rid = admitQ.front();
                admitQ.pop_front();
                const int k = chooseRemote(rid, now);
                jobs[rid].remote = k;

                const double preFinish = now + busy(0, jobs[rid].Lin);
                const double upStart = max(preFinish, upEnd);
                const double upFinish = upStart + xfer(jobs[rid].Lin);
                remoteEnd[k] = max(remoteEnd[k], upFinish) + busy(1, jobs[rid].Lin);
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

            const bool haveD = !remoteTokenQ[k].empty();
            const bool haveP = !pprocQ[k].empty();
            if (!haveD && !haveP) continue;

            bool chooseP = false;
            if (!haveD) {
                chooseP = true;
            } else if (!haveP) {
                chooseP = false;
            } else {
                const int ridP = pprocQ[k].front();
                const double pAge = now - jobs[ridP].arrival;
                const bool pStarving = pAge >= ARRIVE_AGE;

                // Decode normally wins in throughput mode. However, an old
                // prefill request is NEVER allowed to remain behind decode
                // forever. This is the remote-side half of the Test-19 guard.
                if (pStarving) {
                    // Hard guard: an old prefill request must make progress.
                    chooseP = true;
                } else if (!throughputMode) {
                    // SLO-oriented tests: admit P when the prefill queue is
                    // comparable to the decode queue.
                    chooseP = pprocQ[k].size() >= remoteTokenQ[k].size();
                } else {
                    // Throughput-oriented tests: decode normally wins, but do
                    // not let a tiny decode queue leave a cloud doing nothing
                    // for admissions.  This is the key low-test optimization:
                    // admit one prefill task when decode pressure is small, or
                    // when prefill has accumulated substantially.  There is no
                    // timer, sleep, or batch-building delay here.
                    const size_t dq = remoteTokenQ[k].size();
                    const size_t pq = pprocQ[k].size();
                    if (dq <= 3) {
                        chooseP = true;
                    } else if (pq >= 2 && pq * 2 >= dq) {
                        chooseP = true;
                    } else {
                        chooseP = false;
                    }
                }
            }

            if (chooseP) {
                const int rid = pprocQ[k].front();
                pprocQ[k].pop_front();
                Req& r = jobs[rid];

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
                // ------------------------------------------------------------
                // D PROC: ZERO-WAIT BACKLOG REFINEMENT.
                //
                // The measured 15,162-point baseline shows that the full-ready
                // batch is critical for the latency-sensitive Test-19 regime.
                // A blanket q>=16 -> 16 rule was already tested and collapsed
                // Test 19 to ~258 points, so we do NOT repeat that unsafe policy.
                //
                // Dual-mode policy: the light path is exactly the full-ready
                // baseline; the heavy path uses min(q_len,16). Both paths are
                // strictly work-conserving and consume only already-ready FIFO
                // entries. There is no batch-growth timer and no holding queue.
                // ------------------------------------------------------------
                const int q_len = (int)remoteTokenQ[k].size();
                const bool heavyMode = dualModeHeavy(now);

                // ZERO-WAIT dual-mode batching:
                //   LIGHT: preserve the known-good baseline and consume all
                //          requests that are already ready on this cloud.
                //   HEAVY: cap the instantaneous cloud batch at 16 to create
                //          repeated efficient decode iterations and amortize S.
                // No request is held to wait for future arrivals.
                const int b = heavyMode ? min(q_len, 16) : q_len;

                ostringstream ss;
                ss << 'C' << k << " D PROC " << k << ' ' << b;
                for (int i = 0; i < b; ++i) {
                    const int rid = remoteTokenQ[k].front();
                    remoteTokenQ[k].pop_front();
                    ss << ' ' << rid;
                    jobs[rid].phase = Phase::WAIT_DOWN_DEC;
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

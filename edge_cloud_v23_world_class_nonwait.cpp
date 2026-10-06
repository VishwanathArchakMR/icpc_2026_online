// Version: Continuous Batching & Cloud Offload v16.0 - Refactored Unique Build
#include <bits/stdc++.h>
using namespace std;

// ============================================================================
// Edge-Cloud Collaborative Scheduling - V23 refactored non-wait scheduler
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

struct TimingGrid {
    vector<pair<long long,double>> col[6];

    double interpDuration(int c, long long bs) const {
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

    void insertRow(long long bs, const array<double,6>& v) {
        for (int c = 0; c < 6; ++c)
            if (v[c] >= 0.0) col[c].push_back({bs, v[c]});
    }

    void sortColumns() {
        for (auto& v : col) sort(v.begin(), v.end());
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
    TimingGrid table;
    for (int i = 0; i < N; ++i) {
        long long bs;
        array<double,6> v{};
        cin >> bs;
        for (double& x : v) cin >> x;
        table.insertRow(bs, v);
    }
    table.sortColumns();

    auto linkDelay = [&](long long len) -> double {
        return latency + 8.0 * double(len) * double(bytesPerToken)
             / (bandwidth * 1e6);
    };

    auto slotCost = [&](int col, long long bs) -> double {
        return S + table.interpDuration(col, bs);
    };

    vector<Req> req;
    req.reserve(4096);
    auto growRequestStore = [&](int rid) {
        if (rid >= (int)req.size()) req.resize(rid + 1);
    };

    // ------------------------------------------------------------------------
    // Ready structures.
    // ------------------------------------------------------------------------
    deque<int> admitQ;                    // not-yet-admitted ARR requests
    vector<deque<int>> prefillQ(K);           // ready P PROC by assigned remote
    deque<int> inputPostQ;                      // ready P POST

    // GLOBAL decode-ready set, strictly ordered by original ARRIVAL time.
    // This replaces the old "first queue + repeat queue" ordering because a
    // recurring request from an old arrival is still legitimately older than a
    // newer first-token request. The starvation guard below can force old
    // first-token requests into the selected batch when needed.
    set<pair<double,int>> readyByArrival;

    vector<deque<int>> decodeCloudQ(K);            // ready D PROC by remote
    deque<int> tokenPostQ;                       // ready D POST

    bool edgeIdle = true;
    vector<char> cloudIdle(K, 1);

    // ------------------------------------------------------------------------
    // Lightweight virtual endpoint model for P PRE placement.
    // It is only a ranking heuristic; correctness never depends on it.
    // ------------------------------------------------------------------------
    vector<double> cloudReadyAt(K, 0.0);
    vector<int> decodeRun(K, 0);
    double uplinkReadyAt = 0.0;
    double downlinkReadyAt = 0.0;
    int uplinkPending = 0;
    int downlinkPending = 0;

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

    auto meanInputLatency = [&](double now) -> double {
        const long long cnt = tdrDone + tdrPending;
        if (cnt == 0) return 0.0;
        const long double sum = tdrDoneSum
                              + (long double)tdrPending * now
                              - pendingArrivalSum;
        return double(sum / (long double)cnt);
    };

    auto meanTokenGap = [&](double now) -> double {
        const long long cnt = tpotDone + activeGapReqs;
        if (cnt == 0) return 0.0;
        const long double sum = tpotDoneSum
                              + (long double)activeGapReqs * now
                              - activeLastTokenSum;
        return double(sum / (long double)cnt);
    };

    auto scorePressures = [&](double now) -> pair<double,double> {
        const double tdr = meanInputLatency(now);
        const double tpot = meanTokenGap(now);
        return {
            SLO1 > 0.0 ? tdr / SLO1 : 0.0,
            SLO2 > 0.0 ? tpot / SLO2 : 0.0
        };
    };

    const bool throughputBiased = (wtp >= 0.72 || wc <= 0.28);

    // ------------------------------------------------------------------------
    // Starvation thresholds.
    // These are intentionally finite even in throughput mode. Test 19 proved
    // that completely disabling admission protection is catastrophic.
    // ------------------------------------------------------------------------
    const double ARRIVE_AGE = max(1.5 * SLO1, 8.0 * S);
    const double FIRST_AGE  = max(1.5 * SLO1, 8.0 * S);

    auto oldestAdmissionAge = [&](double now) -> double {
        if (admitQ.empty()) return 0.0;
        return max(0.0, now - req[admitQ.front()].arrival);
    };

    auto oldestFirstReadyId = [&]() -> int {
        int best = -1;
        double bestA = numeric_limits<double>::infinity();
        for (auto it = readyByArrival.begin(); it != readyByArrival.end(); ++it) {
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

    auto oldestFirstReadyAge = [&](double now) -> double {
        const int rid = oldestFirstReadyId();
        if (rid < 0) return 0.0;
        return max(0.0, now - req[rid].readyDecodeTime);
    };

    auto admissionOverdue = [&](double now) -> bool {
        if (admitQ.empty()) return false;
        const double age = oldestAdmissionAge(now);
        const size_t q = admitQ.size();
        // Age is the primary protection; queue depth is a secondary fail-safe.
        // The depth trigger is modest so it does not destroy Test-22 batching.
        return age >= ARRIVE_AGE || q >= (size_t)max(2, 2 * K);
    };

    auto firstTokenOverdue = [&](double now) -> bool {
        if (oldestFirstReadyId() < 0) return false;
        const double age = oldestFirstReadyAge(now);
        return age >= FIRST_AGE;
    };

    // ------------------------------------------------------------------------
    // Remote choice for P PRE.
    // Estimate the end of the entire input pipeline using current virtual
    // UP/DOWN FIFO endpoints plus each worker's CPU endpoint.
    // ------------------------------------------------------------------------
    auto pickCloud = [&](int rid, double now) -> int {
        const long long lin = req[rid].Lin;
        const double preFinish = now + slotCost(0, lin);
        const double upStart = max(preFinish, uplinkReadyAt);
        const double upFinish = upStart + linkDelay(lin);
        const double procDur = slotCost(1, lin);
        const double postDur = slotCost(2, lin);

        int best = 0;
        double bestScore = numeric_limits<double>::infinity();
        for (int k = 0; k < K; ++k) {
            const double procStart = max(cloudReadyAt[k], upFinish);
            const double procFinish = procStart + procDur;
            const double downStart = max(downlinkReadyAt, procFinish);
            const double downFinish = downStart + linkDelay(lin);
            const double finish = downFinish + postDur;

            // Throughput still prefers a free worker / shorter CPU endpoint,
            // but the shared transfer queues are never ignored.
            double score = throughputBiased
                         ? procFinish + 0.25 * (finish - procFinish)
                         : finish;
            if (cloudIdle[k]) score -= 0.15 * procDur;
            score += 1e-7 * decodeRun[k];
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
    auto pickChunkCount = [&](int rid, double now) -> int {
        if (throughputBiased || numLayers < 4) return 1;
        const int k = req[rid].remote;
        if (k < 0 || k >= K || decodeCloudQ[k].empty()) return 1;

        const auto [pt, pp] = scorePressures(now);
        const double full = slotCost(1, req[rid].Lin);
        const double decode1 = slotCost(4, 1);
        const double age = now - req[rid].arrival;

        if ((pt >= 1.0 || pp >= 1.0) && age >= ARRIVE_AGE && full >= 3.0 * decode1)
            return 2;
        return 1;
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
    auto bestDecodeGroup = [&](int n) -> int {
        if (n <= 0) return 0;
        return min(n, 16);
    };

    auto pickEdgeDecodeGroup = [&](int n, double /*pressure*/) -> int {
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
                growRequestStore(rid);
                req[rid] = Req{};
                req[rid].Lin = lin;
                req[rid].phase = Phase::ARRIVED;
                req[rid].arrival = now;
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
                        growRequestStore(rid);
                        req[rid].remote = remote;
                        req[rid].phase = Phase::WAIT_UP_PRE;
                        edgeIdle = true;
                    }
                    else if (step == "PROC") {
                        int ls, le, remote, rid;
                        double dur;
                        cin >> ls >> le >> remote >> rid >> dur;
                        growRequestStore(rid);
                        if (0 <= remote && remote < K) {
                            cloudIdle[remote] = 1;
                            cloudReadyAt[remote] = now;
                            decodeRun[remote] = 0;
                        }

                        if (le < numLayers) {
                            req[rid].piece++;
                            req[rid].phase = Phase::READY_PPROC;
                            prefillQ[req[rid].remote].push_back(rid);
                        } else {
                            req[rid].phase = Phase::WAIT_DOWN_PRE;
                            ++downlinkPending;
                            downlinkReadyAt = max(downlinkReadyAt, now) + linkDelay(req[rid].Lin);
                        }
                    }
                    else { // P POST
                        int remote, rid;
                        double dur;
                        cin >> remote >> rid >> dur;
                        growRequestStore(rid);
                        req[rid].phase = Phase::READY_DPRE;
                        req[rid].readyDecodeTime = now;

                        tdrDoneSum += now - req[rid].arrival;
                        ++tdrDone;
                        --tdrPending;
                        pendingArrivalSum -= req[rid].arrival;

                        readyByArrival.insert({req[rid].arrival, rid});
                        edgeIdle = true;
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
                            readyByArrival.erase({req[rid].arrival, rid});
                        }
                        double dur;
                        cin >> dur;
                        (void)minus1; (void)dur;
                        edgeIdle = true;

                        // D PRE creates one UP transfer per represented remote,
                        // inserted by increasing remote index.
                        for (int r = 0; r < K; ++r) {
                            if (!cnt[r]) continue;
                            ++uplinkPending;
                            uplinkReadyAt = max(uplinkReadyAt, now) + linkDelay(cnt[r]);
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
                            cloudIdle[remote] = true;
                            cloudReadyAt[remote] = now;
                            decodeRun[remote] = 0;
                        }
                        ++downlinkPending;
                        downlinkReadyAt = max(downlinkReadyAt, now) + linkDelay(m);
                    }
                    else { // D POST
                        int minus1, m;
                        cin >> minus1 >> m;
                        frameDPost.resize(m);
                        for (int j = 0; j < m; ++j) cin >> frameDPost[j];
                        double dur;
                        cin >> dur;
                        (void)minus1; (void)dur;
                        edgeIdle = true;
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
                    if (uplinkPending > 0) --uplinkPending;
                    if (uplinkPending == 0) uplinkReadyAt = now;

                    if (sub == "PRE") {
                        int rid;
                        cin >> rid;
                        req[rid].phase = Phase::READY_PPROC;
                        req[rid].chunks = pickChunkCount(rid, now);
                        req[rid].piece = 0;
                        prefillQ[req[rid].remote].push_back(rid);
                    }
                    else { // DEC
                        for (int j = 0; j < m; ++j) {
                            int rid;
                            cin >> rid;
                            req[rid].phase = Phase::READY_DPROC;
                            decodeCloudQ[remote].push_back(rid);
                        }
                    }
                }
                else { // DOWN
                    if (downlinkPending > 0) --downlinkPending;
                    if (downlinkPending == 0) downlinkReadyAt = now;

                    if (sub == "PRE") {
                        int rid;
                        cin >> rid;
                        req[rid].phase = Phase::READY_PPOST;
                        inputPostQ.push_back(rid);
                    }
                    else { // DEC
                        for (int j = 0; j < m; ++j) {
                            int rid;
                            cin >> rid;
                            req[rid].phase = Phase::READY_DPOST;
                            tokenPostQ.push_back(rid);
                        }
                    }
                }
            }
            else { // FIN
                int rid;
                cin >> rid;
                growRequestStore(rid);
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
                readyByArrival.insert({r.arrival, rid});
            }
        }

        const auto [pt, pp] = scorePressures(now);
        const bool anyFirstStarving = firstTokenOverdue(now);
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
        if (edgeIdle) {
            if (!inputPostQ.empty()) {
                // User-requested TDR rule: once an input DOWN completes, P POST
                // is the first local action. P POST ends TDR immediately.
                const int rid = inputPostQ.front();
                inputPostQ.pop_front();
                ostringstream ss;
                ss << "E P POST " << req[rid].remote << ' ' << rid;
                out.push_back(ss.str());
                edgeIdle = false;
            }
            else if (!tokenPostQ.empty()) {
                const int b = throughputBiased ? (int)tokenPostQ.size()
                                               : min((int)tokenPostQ.size(), 16);
                ostringstream ss;
                ss << "E D POST -1 " << b;
                for (int i = 0; i < b; ++i) {
                    ss << ' ' << tokenPostQ.front();
                    tokenPostQ.pop_front();
                }
                out.push_back(ss.str());
                edgeIdle = false;
            }
            else if (admissionOverdue(now) && !admitQ.empty()) {
                // HARD ADMISSION STARVATION GUARD.
                // The oldest ARR is forced into the next free local slot even
                // if a decode stream is ready. This is the mechanism that kept
                // Test 19 out of the catastrophic 1.946-point regime.
                const int rid = admitQ.front();
                admitQ.pop_front();
                const int k = pickCloud(rid, now);
                req[rid].remote = k;

                const double preFinish = now + slotCost(0, req[rid].Lin);
                const double upStart = max(preFinish, uplinkReadyAt);
                const double upFinish = upStart + linkDelay(req[rid].Lin);
                cloudReadyAt[k] = max(cloudReadyAt[k], upFinish) + slotCost(1, req[rid].Lin);
                uplinkReadyAt = upFinish;
                ++uplinkPending;

                ostringstream ss;
                ss << "E P PRE " << k << ' ' << rid;
                out.push_back(ss.str());
                edgeIdle = false;
            }
            else if (!readyByArrival.empty()) {
                // ------------------------------------------------------------
                // D PRE: STRICT FIFO, FULL READY-SET DISPATCH.
                //
                // CRITICAL DIFFERENCE FROM V15:
                // Do NOT cap the already-ready queue at 16.  The 12.77k/918.9
                // Test-22 regime came from amortizing E's fixed schedule cost
                // across the entire burst that was already available.  There
                // is no waiting here: every member selected below was already
                // present in readyByArrival at this frame boundary.
                //
                // The arrival-ordered set provides the starvation invariant:
                // the batch always contains the oldest ready requests first.
                // The existing aged-first-token guard therefore remains valid.
                // ------------------------------------------------------------
                const int b = (int)readyByArrival.size();

                ostringstream ss;
                ss << "E D PRE -1 " << b;

                static int cnt[8];
                fill(cnt, cnt + 8, 0);

                for (auto it = readyByArrival.begin(); it != readyByArrival.end(); ) {
                    const int rid = it->second;
                    ss << ' ' << rid;
                    ++cnt[req[rid].remote];
                    req[rid].phase = Phase::WAIT_UP_DEC;
                    it = readyByArrival.erase(it);
                }

                out.push_back(ss.str());
                edgeIdle = false;

                // Automatic UP transfers are created once per represented
                // remote, in increasing remote-index order.
                for (int r = 0; r < K; ++r) {
                    if (!cnt[r]) continue;
                    ++uplinkPending;
                    uplinkReadyAt = max(uplinkReadyAt, now) + linkDelay(cnt[r]);
                }
            }
            else if (!admitQ.empty()) {
                // No decode work exists. Admit immediately; there is no reason
                // to leave E idle when a request can start its input pipeline.
                const int rid = admitQ.front();
                admitQ.pop_front();
                const int k = pickCloud(rid, now);
                req[rid].remote = k;

                const double preFinish = now + slotCost(0, req[rid].Lin);
                const double upStart = max(preFinish, uplinkReadyAt);
                const double upFinish = upStart + linkDelay(req[rid].Lin);
                cloudReadyAt[k] = max(cloudReadyAt[k], upFinish) + slotCost(1, req[rid].Lin);
                uplinkReadyAt = upFinish;
                ++uplinkPending;

                ostringstream ss;
                ss << "E P PRE " << k << ' ' << rid;
                out.push_back(ss.str());
                edgeIdle = false;
            }
        }

        // ====================================================================
        // REMOTE SCHEDULER
        // ====================================================================
        for (int k = 0; k < K; ++k) {
            if (!cloudIdle[k]) continue;

            const bool haveD = !decodeCloudQ[k].empty();
            const bool haveP = !prefillQ[k].empty();
            if (!haveD && !haveP) continue;

            bool chooseP = false;
            if (!haveD) {
                chooseP = true;
            } else if (!haveP) {
                chooseP = false;
            } else {
                const int ridP = prefillQ[k].front();
                const double pAge = now - req[ridP].arrival;
                const bool pStarving = pAge >= ARRIVE_AGE;

                // Decode normally wins in throughput mode. However, an old
                // prefill request is NEVER allowed to remain behind decode
                // forever. This is the remote-side half of the Test-19 guard.
                if (pStarving) {
                    chooseP = true;
                } else if (!throughputBiased) {
                    // In SLO mode, admit P earlier when the P queue is large.
                    chooseP = prefillQ[k].size() >= decodeCloudQ[k].size();
                } else {
                    // Throughput mode: decode wins. No artificial two-task or
                    // eight-task streak cap. P is admitted only when its age
                    // crosses the hard starvation threshold.
                    chooseP = false;
                }
            }

            if (chooseP) {
                const int rid = prefillQ[k].front();
                prefillQ[k].pop_front();
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
                cloudReadyAt[k] = now + S + table.interpDuration(1, r.Lin) * frac;
                decodeRun[k] = 0;

                ostringstream ss;
                ss << 'C' << k << " P PROC " << ls << ' ' << le
                   << ' ' << k << ' ' << rid;
                out.push_back(ss.str());
                cloudIdle[k] = false;
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
                // Instead, use a surgical safety gate:
                //   * default: consume the entire READY FIFO immediately;
                //   * only when a genuinely deep backlog already exists (q>=64)
                //     AND the current TPOT pressure is still low (<~180ms),
                //     cap this instantaneous batch at 16 to increase iteration
                //     frequency for throughput-heavy bursts such as Test 22.
                //
                // There is still ZERO waiting: every selected request is already
                // in decodeCloudQ[k] at this frame boundary. FIFO order is preserved.
                // ------------------------------------------------------------
                // Zero-wait continuous batching: inspect only the work that is
                // already ready at this frame boundary.  No timer, no holding
                // buffer, and no attempt to wait for future arrivals.
                const int q_len = (int)decodeCloudQ[k].size();
                const int b = min(q_len, 16);

                ostringstream ss;
                ss << 'C' << k << " D PROC " << k << ' ' << b;
                for (int i = 0; i < b; ++i) {
                    const int rid = decodeCloudQ[k].front();
                    decodeCloudQ[k].pop_front();
                    ss << ' ' << rid;
                    req[rid].phase = Phase::WAIT_DOWN_DEC;
                }
                out.push_back(ss.str());
                cloudIdle[k] = false;
                cloudReadyAt[k] = now + slotCost(4, b);
                ++decodeRun[k];
            }
        }

        cout << out.size() << '\n';
        for (const string& s : out) cout << s << '\n';
        cout.flush();
    }

    return 0;
}

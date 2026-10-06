#include <bits/stdc++.h>
using namespace std;

// ============================================================================
// Edge-Cloud Collaborative Scheduling — V5
// ============================================================================
// Core idea:
//   Keep the proven high-throughput behavior of the 12.77k baseline, but add
//   only TWO carefully gated controls:
//
//   1) First-token starvation guard:
//      - A request that has completed P POST is placed in firstDecodeQ.
//      - firstDecodeQ is always drained before recurring decode work.
//      - If the local machine has no first-token work, we periodically inject
//        P PRE / P POST work so the entire input pipeline cannot starve.
//
//   2) Remote fairness guard:
//      - D PROC is normally preferred because it sustains token throughput.
//      - But a worker that has been running decode repeatedly is forced to
//        accept one P PROC when prefill work is waiting.
//      - This prevents the classic failure mode seen in the 1.94-point test:
//        local decode work keeps the remotes busy, new requests never enter
//        the pipeline, and total token throughput collapses.
//
// Batch policy:
//   - Throughput-heavy tests: use ALL currently ready requests, restoring the
//     behavior that produced ~918.9 on test 22 and ~998 on test 20.
//   - Mixed/SLO-heavy tests: still batch aggressively, but cap only when the
//     waiting-time pressure is demonstrably high.
//   - No arbitrary power-of-two truncation in throughput mode.
//
// P PRE placement:
//   - Uses virtual UP FIFO end time, virtual DOWN FIFO end time, and virtual
//     remote CPU end time.
//   - The objective estimates the time from ARR to P POST completion, while
//     still including the shared link and P PROC cost.
//
// Complexity:
//   - O(events + assignments), no per-frame O(R) scan.
//   - Deques hold ready request ids.
// ============================================================================

struct TaskTimeTable {
    vector<pair<long long, double>> col[6];

    void addRow(long long bs, const array<double, 6>& v) {
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
        const auto hi = *it;
        const auto lo = *prev(it);
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

    if (!(cin >> K >> S >> latency >> bandwidth >> bytesPerToken >> numLayers))
        return 0;

    double SLO1, SLO2, tpUB, tpBase, distBase, wtp, wc;
    cin >> SLO1 >> SLO2 >> tpUB >> tpBase >> distBase >> wtp >> wc;

    int N;
    cin >> N;
    TaskTimeTable table;
    for (int i = 0; i < N; ++i) {
        long long bs;
        array<double, 6> v{};
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

    // ------------------------------------------------------------------------
    // Request storage.
    // ------------------------------------------------------------------------
    vector<Req> req;
    req.reserve(4096);

    auto ensure = [&](int rid) {
        if (rid >= (int)req.size()) req.resize(rid + 1);
    };

    // ------------------------------------------------------------------------
    // Ready queues.
    // ------------------------------------------------------------------------
    deque<int> arrivalQ;                 // waiting for P PRE
    vector<deque<int>> pprocQ(K);        // P PROC ready on worker k
    deque<int> ppostQ;                   // waiting for local P POST

    deque<int> firstDecodeQ;             // first output step, FIFO by arrival
    deque<int> repeatDecodeQ;            // later output steps
    vector<deque<int>> dprocQ(K);        // D PROC ready on worker k
    deque<int> dpostQ;                   // D POST ready locally

    // ------------------------------------------------------------------------
    // Actual resource state.
    // ------------------------------------------------------------------------
    bool localFree = true;
    vector<char> remoteFree(K, 1);

    // ------------------------------------------------------------------------
    // Virtual finish times for placement decisions.
    // They are scheduling estimates, NOT simulated judge timestamps.
    // ------------------------------------------------------------------------
    vector<double> remoteEnd(K, 0.0);
    vector<int> remoteDecodeStreak(K, 0);

    double upEnd = 0.0;
    double downEnd = 0.0;
    int upPending = 0;
    int downPending = 0;

    // ------------------------------------------------------------------------
    // O(1) running metrics for pressure control.
    // ------------------------------------------------------------------------
    // TDR: completed P POST contributions + pending-arrival lower bound.
    long double tdrSumDone = 0.0L;
    long long tdrDone = 0;
    long long tdrPending = 0;
    long double pendingArrivalSum = 0.0L;

    // TPOT: completed gaps + active request contribution represented by
    // (now - lastToken). A request contributes only after it has at least one
    // token and is not finished.
    long double tpotSumDone = 0.0L;
    long long tpotDone = 0;
    long long activeGapRequests = 0;
    long double activeLastTokenSum = 0.0L;

    auto currentTDR = [&](double now) -> double {
        const long long cnt = tdrDone + tdrPending;
        if (cnt == 0) return 0.0;
        long double sum = tdrSumDone
                        + (long double)tdrPending * now
                        - pendingArrivalSum;
        return double(sum / (long double)cnt);
    };

    auto currentTPOT = [&](double now) -> double {
        const long long cnt = tpotDone + activeGapRequests;
        if (cnt == 0) return 0.0;
        long double sum = tpotSumDone
                        + (long double)activeGapRequests * now
                        - activeLastTokenSum;
        return double(sum / (long double)cnt);
    };

    auto pressure = [&](double now) {
        const double tdr = currentTDR(now);
        const double tpot = currentTPOT(now);
        const double prT = (SLO1 > 0.0 ? tdr / SLO1 : 0.0);
        const double prP = (SLO2 > 0.0 ? tpot / SLO2 : 0.0);
        return pair<double, double>{prT, prP};
    };

    // Throughput-heavy tests must retain full batching. This condition is
    // deliberately broad because test 22 proved that throttling batch sizes
    // can destroy throughput by an order of magnitude.
    auto throughputMode = [&]() -> bool {
        return wtp >= 0.72 || wc <= 0.28;
    };

    // Returns true only when starvation is genuinely visible.
    auto prefillEmergency = [&](double now) -> bool {
        const auto [rt, rp] = pressure(now);
        if (arrivalQ.size() >= (size_t)max(2, 2 * K)) return true;
        if (ppostQ.size() >= (size_t)max(2, K)) return true;
        if (rt >= 1.0 && wc > 0.35) return true;
        return false;
    };

    // ------------------------------------------------------------------------
    // P PRE remote placement.
    // ------------------------------------------------------------------------
    // We estimate the end of P POST:
    //   local P PRE -> shared UP FIFO -> remote P PROC -> shared DOWN FIFO ->
    //   local P POST.
    // The remote CPU endpoint is independent for every Ck, while the network
    // endpoints are shared, exactly matching the statement's model.
    // ------------------------------------------------------------------------
    auto chooseRemote = [&](int rid, double now) -> int {
        const Req& r = req[rid];
        const long long lin = r.Lin;

        const double ppreFinish = now + busy(0, lin);
        const double upStart = max(ppreFinish, upEnd);
        const double upFinish = upStart + xfer(lin);
        const double pprocDur = busy(1, lin);
        const double ppostDur = busy(2, lin);

        int best = 0;
        double bestScore = numeric_limits<double>::infinity();

        for (int k = 0; k < K; ++k) {
            const double procStart = max(remoteEnd[k], upFinish);
            const double procFinish = procStart + pprocDur;
            const double downStart = max(downEnd, procFinish);
            const double downFinish = downStart + xfer(lin);
            const double tdrFinish = downFinish + ppostDur;

            // Main objective: shortest estimated input completion.
            double score = tdrFinish;

            // In strong throughput mode use a softer queue penalty so a single
            // long request is not dumped repeatedly onto one worker.
            if (throughputMode()) {
                score = procFinish;
                score += 0.15 * (tdrFinish - procFinish);
            }

            // Explicit load-balancing bonus: an idle worker should win a tie,
            // preventing consecutive ARR requests from piling onto one Ck.
            if (remoteFree[k]) score -= 0.10 * max(1.0, pprocDur);

            score += 1e-7 * double(remoteDecodeStreak[k]);

            if (score < bestScore) {
                bestScore = score;
                best = k;
            }
        }
        return best;
    };

    // ------------------------------------------------------------------------
    // Prefill chunking.
    // ------------------------------------------------------------------------
    // V5 intentionally uses only 1 or 2 pieces. The earlier 4-piece policy
    // hurt throughput tests because every additional piece pays S.
    // ------------------------------------------------------------------------
    auto chooseChunks = [&](int rid, double now) -> int {
        if (numLayers < 4) return 1;
        if (throughputMode()) return 1;

        const int k = req[rid].remote;
        if (k < 0 || k >= K) return 1;
        if (dprocQ[k].empty()) return 1;

        const auto [rt, rp] = pressure(now);
        const double full = busy(1, req[rid].Lin);
        const double decodeOne = busy(4, 1) + xfer(1);

        // Only split when the decode opportunity is large enough to justify
        // paying one extra schedule cost and the SLO pressure is meaningful.
        if (full >= 3.0 * decodeOne && (rt >= 0.85 || rp >= 0.85))
            return 2;

        return 1;
    };

    // ------------------------------------------------------------------------
    // Batch size selection.
    // ------------------------------------------------------------------------
    // The key correction versus V4:
    //   throughput mode => ALL ready requests.
    // No arbitrary power-of-two truncation is allowed in that mode.
    // ------------------------------------------------------------------------
    auto chooseOutputBatch = [&](double now, int n, bool isDProc) -> int {
        if (n <= 1) return n;
        if (throughputMode()) return n;

        auto [rt, rp] = pressure(now);
        const double p = isDProc ? rp : max(rt, rp);

        // Mild pressure: retain large batches; only severe pressure needs a
        // ceiling. This avoids the Test-22 style throughput collapse.
        if (p < 0.90) return n;
        if (p < 1.10) return min(n, 256);
        if (p < 1.40) return min(n, 128);
        if (p < 1.80) return min(n, 64);
        return min(n, 32);
    };

    // ------------------------------------------------------------------------
    // Utility for bounded output line count: one local + K remotes.
    // ------------------------------------------------------------------------
    vector<string> out;
    out.reserve(K + 1);

    // A remote gets a P PROC after at most this many consecutive decode tasks
    // when prefill work is waiting. The guard is intentionally small because
    // a decode task can contain a large batch and therefore represents plenty
    // of useful work already.
    const int MAX_DECODE_STREAK = 2;

    string tok;
    while (cin >> tok) {
        if (tok == "END") break;
        const double now = stod(tok);

        int e;
        cin >> e;

        vector<int> frameDPost;
        frameDPost.reserve(256);

        // --------------------------------------------------------------------
        // Read the ENTIRE frame before scheduling.
        // --------------------------------------------------------------------
        for (int i = 0; i < e; ++i) {
            string kind;
            cin >> kind;

            if (kind == "ARR") {
                int rid;
                long long lin;
                cin >> rid >> lin;
                ensure(rid);

                Req& r = req[rid];
                r = Req{};
                r.Lin = lin;
                r.phase = Phase::ARRIVED;
                r.arrival = now;

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
                        (void)ls;

                        ensure(rid);
                        if (remote >= 0 && remote < K) {
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

                        // TDR completion is known exactly here.
                        tdrSumDone += now - req[rid].arrival;
                        ++tdrDone;
                        --tdrPending;
                        pendingArrivalSum -= req[rid].arrival;

                        // FIFO order by readiness/arrival; P POST completions
                        // arrive in event order, and that is the earliest legal
                        // order to use for the first token.
                        firstDecodeQ.push_back(rid);
                        localFree = true;
                    }
                }
                else { // D
                    if (step == "PRE") {
                        int minus1, m;
                        cin >> minus1 >> m;
                        vector<int> ids(m);
                        int perRemote[8] = {};
                        for (int j = 0; j < m; ++j) {
                            cin >> ids[j];
                            req[ids[j]].phase = Phase::WAIT_UP_DEC;
                            const int r = req[ids[j]].remote;
                            if (r >= 0 && r < K) perRemote[r]++;
                        }
                        double dur;
                        cin >> dur;
                        (void)minus1;

                        localFree = true;

                        // D PRE's local-to-remote transfers enter the shared UP
                        // FIFO in increasing remote index order.
                        for (int r = 0; r < K; ++r) {
                            if (perRemote[r] == 0) continue;
                            ++upPending;
                            upEnd = max(upEnd, now) + xfer(perRemote[r]);
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

                        if (remote >= 0 && remote < K) {
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
                        for (int j = 0; j < m; ++j) {
                            int rid;
                            cin >> rid;
                            frameDPost.push_back(rid);
                        }
                        double dur;
                        cin >> dur;
                        (void)minus1;
                        (void)dur;
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
                        const int rid = ids[0];
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
                }
                else { // DOWN
                    if (downPending > 0) --downPending;
                    if (downPending == 0) downEnd = now;

                    if (sub == "PRE") {
                        const int rid = ids[0];
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

        // --------------------------------------------------------------------
        // Resolve D POST completions only after all FIN events in this frame are
        // known. This preserves the protocol's final-token/FIN semantics.
        // --------------------------------------------------------------------
        for (int rid : frameDPost) {
            Req& r = req[rid];

            if (r.hasProducedToken) {
                tpotSumDone += now - r.lastToken;
                ++tpotDone;
                activeLastTokenSum -= r.lastToken;
            } else {
                ++activeGapRequests;
            }

            r.hasProducedToken = true;
            r.lastToken = now;
            activeLastTokenSum += now;

            if (r.finThisFrame) {
                r.phase = Phase::FINISHED;
                r.finThisFrame = false;

                if (r.remote >= 0 && r.remote < K) {
                    // Nothing needs decrementing for correctness, but keeping
                    // the actual ready queues sparse is already enough.
                }

                --activeGapRequests;
                activeLastTokenSum -= now;
            } else {
                r.phase = Phase::READY_DPRE;
                repeatDecodeQ.push_back(rid);
            }
        }

        const auto [rt, rp] = pressure(now);

        out.clear();

        // --------------------------------------------------------------------
        // LOCAL POLICY
        // --------------------------------------------------------------------
        // Order:
        //   1. D POST (finishes ready tokens)
        //   2. first-token D PRE (hard starvation guard)
        //   3. urgent P POST / P PRE injection if prefill is starving
        //   4. recurring D PRE
        //   5. ordinary P POST / P PRE
        //
        // The important difference from V4 is that throughput-heavy tests do
        // not let a massive D PRE stream prevent new requests from entering.
        // A small amount of P PRE injection keeps the entire pipeline alive.
        // --------------------------------------------------------------------
        if (localFree) {
            if (!dpostQ.empty()) {
                const int b = chooseOutputBatch(now, (int)dpostQ.size(), false);
                ostringstream ss;
                ss << "E D POST -1 " << b;
                for (int i = 0; i < b; ++i) {
                    ss << ' ' << dpostQ.front();
                    dpostQ.pop_front();
                }
                out.push_back(ss.str());
                localFree = false;
            }
            else if (!firstDecodeQ.empty()) {
                // FIRST TOKEN ALWAYS WINS. This is the explicit FIFO starvation
                // guard for TDR.
                const int totalFirst = (int)firstDecodeQ.size();
                int b = chooseOutputBatch(now, totalFirst, false);

                ostringstream ss;
                ss << "E D PRE -1 " << b;
                for (int i = 0; i < b; ++i) {
                    ss << ' ' << firstDecodeQ.front();
                    firstDecodeQ.pop_front();
                }
                out.push_back(ss.str());
                localFree = false;
            }
            else {
                const bool emergency = prefillEmergency(now);

                // Inject prefill before recurring decode when the input pipeline
                // is visibly starving. This is the main Test-19-style guard.
                if (emergency && !ppostQ.empty()) {
                    const int rid = ppostQ.front();
                    ppostQ.pop_front();
                    ostringstream ss;
                    ss << "E P POST " << req[rid].remote << ' ' << rid;
                    out.push_back(ss.str());
                    localFree = false;
                }
                else if (emergency && !arrivalQ.empty()) {
                    const int rid = arrivalQ.front();
                    arrivalQ.pop_front();

                    const int k = chooseRemote(rid, now);
                    req[rid].remote = k;

                    const double ppreFinish = now + busy(0, req[rid].Lin);
                    const double upStart = max(upEnd, ppreFinish);
                    const double upFinish = upStart + xfer(req[rid].Lin);
                    remoteEnd[k] = max(remoteEnd[k], upFinish)
                                 + busy(1, req[rid].Lin);
                    upEnd = upFinish;
                    ++upPending;

                    ostringstream ss;
                    ss << "E P PRE " << k << ' ' << rid;
                    out.push_back(ss.str());
                    localFree = false;
                }
                else if (!repeatDecodeQ.empty()) {
                    const int total = (int)repeatDecodeQ.size();
                    const int b = chooseOutputBatch(now, total, false);
                    ostringstream ss;
                    ss << "E D PRE -1 " << b;
                    for (int i = 0; i < b; ++i) {
                        ss << ' ' << repeatDecodeQ.front();
                        repeatDecodeQ.pop_front();
                    }
                    out.push_back(ss.str());
                    localFree = false;
                }
                else if (!ppostQ.empty()) {
                    const int rid = ppostQ.front();
                    ppostQ.pop_front();
                    ostringstream ss;
                    ss << "E P POST " << req[rid].remote << ' ' << rid;
                    out.push_back(ss.str());
                    localFree = false;
                }
                else if (!arrivalQ.empty()) {
                    const int rid = arrivalQ.front();
                    arrivalQ.pop_front();

                    const int k = chooseRemote(rid, now);
                    req[rid].remote = k;

                    const double ppreFinish = now + busy(0, req[rid].Lin);
                    const double upStart = max(upEnd, ppreFinish);
                    const double upFinish = upStart + xfer(req[rid].Lin);
                    remoteEnd[k] = max(remoteEnd[k], upFinish)
                                 + busy(1, req[rid].Lin);
                    upEnd = upFinish;
                    ++upPending;

                    ostringstream ss;
                    ss << "E P PRE " << k << ' ' << rid;
                    out.push_back(ss.str());
                    localFree = false;
                }
            }
        }

        // --------------------------------------------------------------------
        // REMOTE POLICY
        // --------------------------------------------------------------------
        // Every free remote can receive one task in the same response.
        // Decode is generally preferred, but after MAX_DECODE_STREAK decode
        // tasks we force one P PROC if one is waiting. This prevents the cloud
        // layer from becoming a pure decode sink while prefill starves.
        // --------------------------------------------------------------------
        for (int k = 0; k < K; ++k) {
            if (!remoteFree[k]) continue;

            const bool haveD = !dprocQ[k].empty();
            const bool haveP = !pprocQ[k].empty();
            if (!haveD && !haveP) continue;

            bool chooseP = false;

            if (haveP && !haveD) {
                chooseP = true;
            } else if (!haveP && haveD) {
                chooseP = false;
            } else {
                // Both are ready.
                const bool forcedPrefill = remoteDecodeStreak[k] >= MAX_DECODE_STREAK;
                const bool globalPrefillPressure =
                    arrivalQ.size() >= (size_t)max(2, 2 * K) ||
                    ppostQ.size() >= (size_t)max(2, K) ||
                    pprocQ[k].size() > dprocQ[k].size() * 2;

                if (forcedPrefill) {
                    chooseP = true;
                } else if (globalPrefillPressure) {
                    chooseP = true;
                } else if (!throughputMode()) {
                    // Mixed mode: compare the current pressure components.
                    chooseP = rt > rp;
                } else {
                    // Throughput mode: decode wins unless the remote itself is
                    // carrying an obvious prefill backlog.
                    chooseP = false;
                }
            }

            if (chooseP) {
                const int rid = pprocQ[k].front();
                pprocQ[k].pop_front();
                Req& r = req[rid];

                int ls = 0;
                int le = numLayers;

                if (r.chunks == 2) {
                    const int mid = max(1, numLayers / 2);
                    if (r.piece == 0) {
                        le = mid;
                    } else {
                        ls = mid;
                        le = numLayers;
                    }
                }

                if (le <= ls) {
                    ls = 0;
                    le = numLayers;
                    r.chunks = 1;
                    r.piece = 0;
                }

                const double frac = double(le - ls) / double(numLayers);
                const double dur = S + table.lookup(1, r.Lin) * frac;
                remoteEnd[k] = now + dur;
                remoteDecodeStreak[k] = 0;

                ostringstream ss;
                ss << 'C' << k << " P PROC " << ls << ' ' << le
                   << ' ' << k << ' ' << rid;
                out.push_back(ss.str());
                remoteFree[k] = 0;
            }
            else {
                const int b = chooseOutputBatch(now, (int)dprocQ[k].size(), true);
                ostringstream ss;
                ss << 'C' << k << " D PROC " << k << ' ' << b;
                for (int i = 0; i < b; ++i) {
                    ss << ' ' << dprocQ[k].front();
                    dprocQ[k].pop_front();
                }
                out.push_back(ss.str());
                remoteFree[k] = 0;
                remoteEnd[k] = now + busy(4, b);
                ++remoteDecodeStreak[k];
            }
        }

        cout << out.size() << '\n';
        for (const string& line : out) cout << line << '\n';
        cout.flush();
    }

    return 0;
}

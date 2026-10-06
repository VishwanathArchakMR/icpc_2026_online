#include <bits/stdc++.h>
using namespace std;

// Edge-Cloud Collaborative Scheduling — adaptive score-driven scheduler
// -----------------------------------------------------------------------
// Goal: keep the legal state machine of the strong 12.77k baseline, while
// adapting only scheduling policy to the test's actual scoring weights and
// observed waiting pressure.
//
// Key ideas:
//   1) Preserve one-piece P PROC. No protocol-risky chunking.
//   2) Use wtp/wc + SLO1/SLO2 instead of fixed priorities only.
//   3) Track live lower bounds for TDR and TPOT pressure.
//   4) In throughput mode, retain large D batches and D-first scheduling.
//   5) In waiting-sensitive mode, allow prefill to preempt decode starvation.
//   6) Choose D batch sizes adaptively from the supplied task-time table.
//   7) Improve remote assignment using active/queued load, not just finished
//      request count.
//
// This version intentionally does NOT predict future timestamps and never
// idles deliberately. It only chooses among tasks legal in the current frame.
// -----------------------------------------------------------------------

struct TaskTimeTable {
    vector<pair<long long, double>> col[6];
    // 0 P PRE, 1 P PROC, 2 P POST, 3 D PRE, 4 D PROC, 5 D POST

    void addRow(long long bs, const array<double, 6>& v) {
        for (int c = 0; c < 6; ++c) {
            if (v[c] >= 0.0) col[c].push_back({bs, v[c]});
        }
    }

    void finalize() {
        for (int c = 0; c < 6; ++c) {
            sort(col[c].begin(), col[c].end());
        }
    }

    bool has(int c) const { return !col[c].empty(); }

    double lookup(int c, long long bs) const {
        const auto& v = col[c];
        if (v.empty()) return 0.0;
        if (bs <= v.front().first) return v.front().second;
        if (bs >= v.back().first) return v.back().second;

        auto it = upper_bound(
            v.begin(), v.end(), make_pair(bs, numeric_limits<double>::infinity())
        );
        auto hi = *it;
        auto lo = *prev(it);
        if (hi.first == lo.first) return lo.second;
        double f = double(bs - lo.first) / double(hi.first - lo.first);
        return lo.second + f * (hi.second - lo.second);
    }
};

enum Phase {
    P_NONE = 0,
    P_ARRIVED,
    P_WAIT_UP_PRE,
    P_READY_PROC,
    P_WAIT_DOWN_PRE,
    P_READY_POST,
    P_READY_DECODE,
    P_WAIT_UP_DEC,
    P_READY_DPROC,
    P_WAIT_DOWN_DEC,
    P_READY_DPOST,
    P_FINISHED
};

struct Req {
    long long Lin = -1;
    int remote = -1;
    Phase phase = P_NONE;

    // Scheduling/score observations.
    double arrivalTime = -1.0;
    double firstReadyTime = -1.0;  // P POST TDN time (TDR endpoint)
    double lastTokenTime = -1.0;
    bool hasToken = false;
};

static inline double clamp01(double x) {
    return max(0.0, min(1.0, x));
}

int main() {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    int K;
    double S_cost, latency_in_ms, bandwidth_gbps;
    long long bytes_per_token;
    int num_layers;

    if (!(cin >> K >> S_cost >> latency_in_ms >> bandwidth_gbps
              >> bytes_per_token >> num_layers)) {
        return 0;
    }

    double SLO1, SLO2, tp_UB, tp_base, dist_base, w_tp, w_c;
    cin >> SLO1 >> SLO2 >> tp_UB >> tp_base >> dist_base >> w_tp >> w_c;

    // Defensive normalization; the statement guarantees wtp + wc = 1.
    double wsum = w_tp + w_c;
    if (wsum > 0.0) {
        w_tp /= wsum;
        w_c /= wsum;
    } else {
        w_tp = 1.0;
        w_c = 0.0;
    }

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

    vector<Req> req;
    vector<int> unfinishedLoad(K, 0); // assigned but not FINISHED

    bool local_free = true;
    vector<char> remote_free(K, 1);

    deque<int> arrived_queue;
    vector<deque<int>> ready_proc_by_remote(K);
    deque<int> ready_post_queue;
    vector<int> ready_decode;
    vector<vector<int>> ready_dproc(K);
    vector<int> ready_dpost;

    // Observed metric accumulators.
    long double tdr_sum = 0.0L;
    long long tdr_count = 0;
    long double tpot_sum = 0.0L;
    long long tpot_count = 0;
    long long seen_requests = 0;
    long long produced_tokens = 0;
    double earliest_arrival = -1.0;

    auto ensureSize = [&](int rid) {
        if ((int)req.size() <= rid) req.resize(rid + 1);
    };

    auto queueSizeP = [&]() -> long long {
        long long s = (long long)arrived_queue.size() + (long long)ready_post_queue.size();
        for (const auto& q : ready_proc_by_remote) s += (long long)q.size();
        return s;
    };

    auto queueSizeD = [&]() -> long long {
        long long s = (long long)ready_decode.size() + (long long)ready_dpost.size();
        for (const auto& q : ready_dproc) s += (long long)q.size();
        return s;
    };

    // Estimate current *lower bounds* on the two waiting metrics using the
    // current timestamp. These are intentionally optimistic: unfinished work
    // can only make the eventual waits stay the same or increase.
    auto estimateWaitPressure = [&](double now) {
        double est_tdr = 0.0;
        if (seen_requests > 0) {
            long double sum = tdr_sum;
            long long cnt = tdr_count;
            for (size_t rid = 0; rid < req.size(); ++rid) {
                const Req& r = req[rid];
                if (r.arrivalTime < 0.0) continue;
                if (r.firstReadyTime < 0.0 && r.phase != P_FINISHED) {
                    sum += max(0.0, now - r.arrivalTime);
                    ++cnt;
                }
            }
            if (cnt > 0) est_tdr = double(sum / (long double)cnt);
        }

        double est_tpot = 0.0;
        {
            long double sum = tpot_sum;
            long long cnt = tpot_count;
            for (size_t rid = 0; rid < req.size(); ++rid) {
                const Req& r = req[rid];
                if (!r.hasToken || r.lastTokenTime < 0.0) continue;
                if (r.phase != P_FINISHED) {
                    // Current open gap is a lower bound on its eventual next gap.
                    sum += max(0.0, now - r.lastTokenTime);
                    ++cnt;
                }
            }
            if (cnt > 0) est_tpot = double(sum / (long double)cnt);
        }

        double tdrRatio = (SLO1 > 0.0 ? est_tdr / SLO1 : 0.0);
        double tpotRatio = (SLO2 > 0.0 ? est_tpot / SLO2 : 0.0);

        // Lower threshold when distbase==0 because then waiting score is
        // binary: missing either SLO gives zero waiting component.
        double trigger = (dist_base == 0.0 ? 0.65 : 0.90);
        double tdrUrg = max(0.0, (tdrRatio - trigger) / max(1e-9, 1.0 - trigger));
        double tpotUrg = max(0.0, (tpotRatio - trigger) / max(1e-9, 1.0 - trigger));

        return tuple<double, double, double, double>(tdrRatio, tpotRatio,
                                                     clamp01(tdrUrg), clamp01(tpotUrg));
    };

    auto maxPendingFirstAge = [&](double now) -> double {
        double best = 0.0;
        for (const Req& r : req) {
            if (r.arrivalTime < 0.0 || r.firstReadyTime >= 0.0 || r.phase == P_FINISHED) continue;
            best = max(best, max(0.0, now - r.arrivalTime));
        }
        return best;
    };

    auto maxOpenTokenGap = [&](double now) -> double {
        double best = 0.0;
        for (const Req& r : req) {
            if (!r.hasToken || r.lastTokenTime < 0.0 || r.phase == P_FINISHED) continue;
            best = max(best, max(0.0, now - r.lastTokenTime));
        }
        return best;
    };

    // Candidate batch sizes. Keeping this tiny makes the per-frame decision
    // cheap even on very large tests.
    auto chooseBatch = [&](int column, int n, double now) -> int {
        if (n <= 1) return n;
        if (!table.has(column)) return n;

        auto [tdrRatio, tpotRatio, tdrUrg, tpotUrg] = estimateWaitPressure(now);
        (void)tdrRatio;
        (void)tpotRatio;

        // Strong waiting pressure should shorten local/remote decode tasks.
        double waitWeight = clamp01(w_c + 0.35 * max(tdrUrg, tpotUrg));
        double tpWeight = clamp01(1.0 - waitWeight);

        // If the waiting component is binary, being even slightly close to
        // the SLO is valuable; bias strongly toward shorter batches.
        if (dist_base == 0.0 && w_c > 0.0 && (tdrUrg > 0.0 || tpotUrg > 0.0)) {
            waitWeight = max(waitWeight, 0.80);
            tpWeight = 1.0 - waitWeight;
        }

        vector<int> cand;
        cand.reserve(10);
        const int fixed[] = {1, 2, 4, 8, 16, 32, 64};
        for (int b : fixed) if (b <= n) cand.push_back(b);
        if (cand.empty() || cand.back() != n) cand.push_back(n);

        double t1 = max(table.lookup(column, 1), 1e-9);
        int bestB = 1;
        double bestScore = -1e100;

        for (int b : cand) {
            double dur = max(table.lookup(column, b), 1e-9);
            // Relative throughput efficiency versus batch=1.
            double eff = (double(b) / dur) / (1.0 / t1);
            // Relative single-task latency.
            double lat = dur / t1;

            // Smooth utility. Throughput likes b/dur; waiting likes 1/dur.
            // Exponents are deliberately mild to avoid wild test-to-test swings.
            double score = tpWeight * log1p(max(0.0, eff))
                         + waitWeight * (1.0 / lat)
                         + 0.03 * log1p((double)b);

            // When the TPOT SLO is under pressure, penalize long D tasks.
            if (tpotUrg > 0.0 && (column == 3 || column == 4 || column == 5)) {
                score -= 0.55 * tpotUrg * max(0.0, lat - 1.0);
            }

            // When TDR is under pressure, do not create a huge local decode
            // task that keeps the edge busy while new requests wait.
            if (tdrUrg > 0.0 && (column == 3 || column == 5)) {
                score -= 0.35 * tdrUrg * max(0.0, lat - 1.0);
            }

            if (score > bestScore + 1e-12) {
                bestScore = score;
                bestB = b;
            }
        }

        // In pure throughput mode retain the baseline's "all ready now" rule:
        // it is the strongest known behavior for the high-wtp workloads.
        if (w_c < 0.12 && tdrUrg < 0.20 && tpotUrg < 0.20) return n;

        // In pure waiting mode, never choose a larger batch when the exact
        // same predicted duration is available at a smaller size.
        return min(bestB, n);
    };

    auto chooseRemote = [&]() -> int {
        // Preserve the proven 12.77k baseline's primary invariant: never move
        // a request away from the least-loaded remote merely because of a
        // speculative queue estimate. Only use queue state as a tie-break.
        int best = 0;
        for (int k = 1; k < K; ++k) {
            if (unfinishedLoad[k] < unfinishedLoad[best]) {
                best = k;
                continue;
            }
            if (unfinishedLoad[k] > unfinishedLoad[best]) continue;

            long long tieK = 2LL * (long long)ready_dproc[k].size()
                           + 1LL * (long long)ready_proc_by_remote[k].size()
                           + (remote_free[k] ? 0LL : 1LL);
            long long tieB = 2LL * (long long)ready_dproc[best].size()
                           + 1LL * (long long)ready_proc_by_remote[best].size()
                           + (remote_free[best] ? 0LL : 1LL);
            if (tieK < tieB) best = k;
        }
        return best;
    };

    auto popFirst = [](deque<int>& q) -> int {
        int x = q.front();
        q.pop_front();
        return x;
    };

    auto emitGroup = [&](ostringstream& oss, const vector<int>& ids) {
        for (int id : ids) oss << ' ' << id;
    };

    string firstToken;
    while (cin >> firstToken) {
        if (firstToken == "END") break;
        double now = stod(firstToken);

        int e;
        cin >> e;

        set<int> finThisFrame;
        vector<int> frameDpostIds;

        for (int i = 0; i < e; ++i) {
            string kind;
            cin >> kind;

            if (kind == "ARR") {
                int rid;
                long long lin;
                cin >> rid >> lin;
                ensureSize(rid);

                Req& r = req[rid];
                r.Lin = lin;
                r.phase = P_ARRIVED;
                r.arrivalTime = now;
                r.firstReadyTime = -1.0;
                r.lastTokenTime = -1.0;
                r.hasToken = false;

                arrived_queue.push_back(rid);
                ++seen_requests;
                if (earliest_arrival < 0.0) earliest_arrival = now;
            }
            else if (kind == "TDN") {
                string server, maj, step;
                cin >> server >> maj >> step;

                bool isLocal = (server == "E");
                int remoteIdx = -1;
                if (!isLocal) remoteIdx = stoi(server.substr(1));

                if (maj == "P") {
                    if (step == "PRE") {
                        int remote, rid;
                        double dur;
                        cin >> remote >> rid >> dur;
                        req[rid].remote = remote;
                        req[rid].phase = P_WAIT_UP_PRE;
                        local_free = true;
                    }
                    else if (step == "PROC") {
                        int ls, le, remote, rid;
                        double dur;
                        cin >> ls >> le >> remote >> rid >> dur;
                        (void)ls; (void)le; (void)dur;
                        req[rid].phase = P_WAIT_DOWN_PRE;
                        if (remoteIdx >= 0 && remoteIdx < K) remote_free[remoteIdx] = 1;
                    }
                    else { // P POST
                        int remote, rid;
                        double dur;
                        cin >> remote >> rid >> dur;
                        (void)remote; (void)dur;

                        req[rid].phase = P_READY_DECODE;
                        req[rid].firstReadyTime = now;
                        if (req[rid].arrivalTime >= 0.0) {
                            tdr_sum += (long double)(now - req[rid].arrivalTime);
                            ++tdr_count;
                        }
                        ready_decode.push_back(rid);
                        local_free = true;
                    }
                }
                else { // D
                    if (step == "PRE") {
                        int minus1, m;
                        cin >> minus1 >> m;
                        vector<int> ids(m);
                        for (int& id : ids) cin >> id;
                        double dur;
                        cin >> dur;
                        (void)minus1; (void)dur;
                        for (int rid : ids) req[rid].phase = P_WAIT_UP_DEC;
                        local_free = true;
                    }
                    else if (step == "PROC") {
                        int remote, m;
                        cin >> remote >> m;
                        vector<int> ids(m);
                        for (int& id : ids) cin >> id;
                        double dur;
                        cin >> dur;
                        (void)remote; (void)dur;
                        for (int rid : ids) req[rid].phase = P_WAIT_DOWN_DEC;
                        if (remoteIdx >= 0 && remoteIdx < K) remote_free[remoteIdx] = 1;
                    }
                    else { // D POST
                        int minus1, m;
                        cin >> minus1 >> m;
                        vector<int> ids(m);
                        for (int& id : ids) cin >> id;
                        double dur;
                        cin >> dur;
                        (void)minus1; (void)dur;
                        local_free = true;
                        frameDpostIds.insert(frameDpostIds.end(), ids.begin(), ids.end());
                    }
                }
            }
            else if (kind == "XDN") {
                string dir;
                int remote;
                long long size;
                string sub;
                int m;
                cin >> dir >> remote >> size >> sub >> m;
                (void)size;

                vector<int> ids(m);
                for (int& id : ids) cin >> id;

                if (sub == "PRE") {
                    int rid = ids[0]; // input stage has m=1
                    if (dir == "UP") {
                        req[rid].phase = P_READY_PROC;
                        ready_proc_by_remote[req[rid].remote].push_back(rid);
                    }
                    else {
                        req[rid].phase = P_READY_POST;
                        ready_post_queue.push_back(rid);
                    }
                }
                else { // DEC
                    if (dir == "UP") {
                        for (int rid : ids) {
                            req[rid].phase = P_READY_DPROC;
                            ready_dproc[remote].push_back(rid);
                        }
                    }
                    else {
                        for (int rid : ids) {
                            req[rid].phase = P_READY_DPOST;
                            ready_dpost.push_back(rid);
                        }
                    }
                }
            }
            else if (kind == "FIN") {
                int rid;
                cin >> rid;
                finThisFrame.insert(rid);
            }
        }

        // Resolve D POSTs only after reading FIN events from the same frame.
        for (int rid : frameDpostIds) {
            req[rid].hasToken = true;
            if (req[rid].lastTokenTime >= 0.0) {
                tpot_sum += (long double)(now - req[rid].lastTokenTime);
                ++tpot_count;
            }
            req[rid].lastTokenTime = now;
            ++produced_tokens;

            if (finThisFrame.count(rid)) {
                req[rid].phase = P_FINISHED;
                if (req[rid].remote >= 0 && req[rid].remote < K) {
                    --unfinishedLoad[req[rid].remote];
                }
            } else {
                req[rid].phase = P_READY_DECODE;
                ready_decode.push_back(rid);
            }
        }

        vector<string> outLines;

        auto [tdrRatio, tpotRatio, tdrUrg, tpotUrg] = estimateWaitPressure(now);
        double oldestTdrAge = maxPendingFirstAge(now);
        double oldestTpotGap = maxOpenTokenGap(now);

        // A frame-level latency mode is entered when the waiting-weight is
        // meaningful and either target is under visible pressure.
        bool latencyMode = false;
        if (w_c > 0.0) {
            double trigger = (dist_base == 0.0 ? 0.65 : 0.90);
            latencyMode = (tdrRatio > trigger || tpotRatio > trigger ||
                           tdrUrg > 0.0 || tpotUrg > 0.0);
        }

        // In binary waiting-score tests, aggressively protect the SLOs.
        if (dist_base == 0.0 && w_c > 0.20 &&
            (tdrRatio > 0.55 || tpotRatio > 0.55)) {
            latencyMode = true;
        }

        // ---------- Local scheduling ----------
        if (local_free) {
            bool havePrefill = !arrived_queue.empty() || !ready_post_queue.empty();
            bool haveDecode = !ready_dpost.empty() || !ready_decode.empty();

            // Weights below are not score constants; they are only tie-breaks.
            // The main switch is the actual observed TDR/TPOT pressure.
            bool tdrDominates = (tdrUrg + 0.15 * max(0.0, tdrRatio - 1.0)) >
                                (tpotUrg + 0.15 * max(0.0, tpotRatio - 1.0));

            // D POST is the most latency-sensitive decode stage: results are
            // already transferred and one local task immediately creates tokens.
            // Keep it first unless TDR is clearly becoming the bottleneck.
            bool preferPrefill = latencyMode && havePrefill && tdrDominates;

            if (preferPrefill && !ready_post_queue.empty()) {
                int rid = popFirst(ready_post_queue);
                ostringstream oss;
                oss << "E P POST " << req[rid].remote << ' ' << rid;
                outLines.push_back(oss.str());
                local_free = false;
            }
            else if (preferPrefill && !arrived_queue.empty()) {
                int rid = popFirst(arrived_queue);
                int remote = chooseRemote();
                req[rid].remote = remote;
                ++unfinishedLoad[remote];

                ostringstream oss;
                oss << "E P PRE " << remote << ' ' << rid;
                outLines.push_back(oss.str());
                local_free = false;
            }
            else if (!ready_dpost.empty()) {
                int b = chooseBatch(5, (int)ready_dpost.size(), now);
                b = max(1, min(b, (int)ready_dpost.size()));
                vector<int> ids;
                ids.reserve(b);
                for (int i = 0; i < b; ++i) ids.push_back(ready_dpost[i]);
                ready_dpost.erase(ready_dpost.begin(), ready_dpost.begin() + b);

                ostringstream oss;
                oss << "E D POST -1 " << ids.size();
                emitGroup(oss, ids);
                outLines.push_back(oss.str());
                local_free = false;
            }
            else if (!ready_decode.empty()) {
                int b = chooseBatch(3, (int)ready_decode.size(), now);
                b = max(1, min(b, (int)ready_decode.size()));
                vector<int> ids;
                ids.reserve(b);
                for (int i = 0; i < b; ++i) ids.push_back(ready_decode[i]);
                ready_decode.erase(ready_decode.begin(), ready_decode.begin() + b);

                ostringstream oss;
                oss << "E D PRE -1 " << ids.size();
                emitGroup(oss, ids);
                outLines.push_back(oss.str());
                local_free = false;
            }
            else if (!ready_post_queue.empty()) {
                int rid = popFirst(ready_post_queue);
                ostringstream oss;
                oss << "E P POST " << req[rid].remote << ' ' << rid;
                outLines.push_back(oss.str());
                local_free = false;
            }
            else if (!arrived_queue.empty()) {
                int rid = popFirst(arrived_queue);
                int remote = chooseRemote();
                req[rid].remote = remote;
                ++unfinishedLoad[remote];

                ostringstream oss;
                oss << "E P PRE " << remote << ' ' << rid;
                outLines.push_back(oss.str());
                local_free = false;
            }
        }

        // ---------- Remote scheduling ----------
        for (int k = 0; k < K; ++k) {
            if (!remote_free[k]) continue;

            bool haveD = !ready_dproc[k].empty();
            bool haveP = !ready_proc_by_remote[k].empty();

            if (!haveD && !haveP) continue;

            // Throughput mode keeps the strong baseline D-first rule.
            bool chooseP = false;
            if (haveP && !haveD) {
                chooseP = true;
            } else if (haveP && haveD) {
                if (latencyMode) {
                    // P PROC is the route to first-token readiness, so let it
                    // pass D PROC when the TDR side is under greater pressure.
                    chooseP = (tdrUrg + 0.10 * max(0.0, tdrRatio - 1.0)) >
                              (tpotUrg + 0.10 * max(0.0, tpotRatio - 1.0));
                }
                // Even in latency mode, if a very old decode gap is active,
                // do not starve decode.
                if (oldestTpotGap > 0.95 * SLO2 && SLO2 > 0.0) chooseP = false;
            }

            if (chooseP) {
                int rid = popFirst(ready_proc_by_remote[k]);
                ostringstream oss;
                oss << "C" << k << " P PROC 0 " << num_layers << ' ' << k << ' ' << rid;
                outLines.push_back(oss.str());
                remote_free[k] = 0;
            }
            else {
                int b = chooseBatch(4, (int)ready_dproc[k].size(), now);
                b = max(1, min(b, (int)ready_dproc[k].size()));

                vector<int> ids;
                ids.reserve(b);
                for (int i = 0; i < b; ++i) ids.push_back(ready_dproc[k][i]);
                ready_dproc[k].erase(ready_dproc[k].begin(), ready_dproc[k].begin() + b);

                ostringstream oss;
                oss << "C" << k << " D PROC " << k << ' ' << ids.size();
                emitGroup(oss, ids);
                outLines.push_back(oss.str());
                remote_free[k] = 0;
            }
        }

        cout << outLines.size() << '\n';
        for (const string& s : outLines) cout << s << '\n';
        cout.flush();
    }

    return 0;
}

#include <bits/stdc++.h>
using namespace std;

// Edge–Cloud Collaborative Scheduling — V3
// ---------------------------------------------------------------
// This version keeps the proven legal state machine of the 12.77k baseline
// and the strongest gains from V2, but fixes several mathematical issues:
//
// 1) Remote assignment is based on an estimated completion path, not request
//    count.  The estimate includes P PRE, the shared UP FIFO, remote P PROC,
//    the shared DOWN FIFO, and P POST.
//
// 2) Decode batch selection uses S + execution time, not execution time alone.
//    This matters because every task pays schedule cost S.
//
// 3) D PRE batching estimates the complete local→remote→compute→downlink path
//    for the candidate group, including the fact that the two link directions
//    are FIFO and that cloud workers run in parallel.
//
// 4) Waiting pressure uses the SAME METRICS AS THE SCORE: mean TDR and pooled
//    mean TPOT.  We do not let one old request dominate the scheduler merely
//    because it has the largest age.
//
// 5) Optional two-piece P PROC is enabled only when it is plausibly useful:
//    the remote already has decode work and the full prefill is substantially
//    longer than a decode task.  The earlier 4-way unconditional chunking was
//    a regression; this is deliberately conservative.
//
// 6) Throughput-heavy tests retain the strongest baseline behavior: aggressive
//    all-ready batching. Mixed/latency-heavy tests use adaptive batch sizes.
//
// All choices remain reactive. No future event is required for legality.
// ---------------------------------------------------------------

struct TaskTimeTable {
    vector<pair<long long,double>> col[6];

    void addRow(long long bs, const array<double,6>& v) {
        for (int c = 0; c < 6; ++c)
            if (v[c] >= 0.0) col[c].push_back({bs, v[c]});
    }

    void finalize() {
        for (int c = 0; c < 6; ++c)
            sort(col[c].begin(), col[c].end());
    }

    double lookup(int c, long long bs) const {
        const auto& v = col[c];
        if (v.empty()) return 0.0;
        if (bs <= v.front().first) return v.front().second;
        if (bs >= v.back().first) return v.back().second;

        auto it = upper_bound(v.begin(), v.end(),
                              make_pair(bs, numeric_limits<double>::infinity()));
        auto hi = *it;
        auto lo = *prev(it);
        if (hi.first == lo.first) return lo.second;
        double f = double(bs - lo.first) / double(hi.first - lo.first);
        return lo.second + f * (hi.second - lo.second);
    }

    bool has(int c) const { return !col[c].empty(); }
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
    double firstReady = -1.0;
    double lastToken = -1.0;

    // Selective 2-piece prefill state.
    unsigned char chunks = 1;
    unsigned char piece = 0; // next piece index

    bool hasToken = false;
    bool finThisFrame = false;
};

static inline double clamp01(double x) {
    return max(0.0, min(1.0, x));
}

int main() {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    int K;
    double S, latency_ms, bandwidth_gbps;
    long long bytes_per_token;
    int num_layers;

    if (!(cin >> K >> S >> latency_ms >> bandwidth_gbps
              >> bytes_per_token >> num_layers)) return 0;

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

    // ------------------------------------------------------------------
    // Reactive state
    // ------------------------------------------------------------------
    vector<Req> req;
    req.reserve(2048);

    // Number of unfinished requests assigned to each cloud.
    vector<int> assigned(K, 0);

    // Current/estimated free time of cloud processors.
    // These estimates are corrected to exact time on every TDN.
    vector<double> remoteEst(K, 0.0);
    vector<char> remoteFree(K, 1);

    bool localFree = true;

    deque<int> arrived;
    vector<deque<int>> readyP(K);
    deque<int> readyPPost;
    deque<int> readyDPre;
    vector<deque<int>> readyDProc(K);
    deque<int> readyDPost;

    // Virtual FIFO endpoints for the two shared transfer directions.
    // They are synchronized whenever the corresponding pending queue empties.
    double upEnd = 0.0;
    double downEnd = 0.0;
    int upPending = 0;
    int downPending = 0;

    // Observed waiting metrics.
    long double tdrSum = 0.0L;
    long long tdrCnt = 0;
    long double tpotSum = 0.0L;
    long long tpotCnt = 0;
    long long seen = 0;

    auto ensure = [&](int rid) {
        if (rid >= (int)req.size()) req.resize(rid + 1);
    };

    auto transferTime = [&](long long len) -> double {
        const double dataBytes = double(len) * double(bytes_per_token);
        return latency_ms + 8.0 * dataBytes / (bandwidth_gbps * 1e6);
    };

    auto prefillDur = [&](int col, long long lin) -> double {
        return S + table.lookup(col, lin);
    };

    auto decodeDur = [&](int col, long long m) -> double {
        return S + table.lookup(col, m);
    };

    // ------------------------------------------------------------------
    // Current lower-bound waiting metrics.
    // These intentionally match the official mean metrics rather than using
    // max-age as the primary objective.
    // ------------------------------------------------------------------
    auto waitingRatios = [&](double now) {
        long double tdr = tdrSum;
        long long tc = tdrCnt;

        for (const Req& r : req) {
            if (r.arrival < 0.0 || r.phase == FINISHED) continue;
            if (r.firstReady < 0.0) {
                tdr += max(0.0, now - r.arrival);
                ++tc;
            }
        }

        long double tpot = tpotSum;
        long long pc = tpotCnt;
        for (const Req& r : req) {
            if (!r.hasToken || r.lastToken < 0.0 || r.phase == FINISHED) continue;
            tpot += max(0.0, now - r.lastToken);
            ++pc;
        }

        const double meanTdr = tc ? double(tdr / tc) : 0.0;
        const double meanTpot = pc ? double(tpot / pc) : 0.0;

        return pair<double,double>(
            SLO1 > 0.0 ? meanTdr / SLO1 : 0.0,
            SLO2 > 0.0 ? meanTpot / SLO2 : 0.0
        );
    };

    // Effective waiting weight.
    // If the lower bound is already far beyond an SLO, the corresponding
    // waiting score may be impossible to rescue; in that case throughput is
    // more valuable. Otherwise retain the official wc weight.
    auto effectiveWC = [&](double now) {
        double w = wc;
        auto [r1, r2] = waitingRatios(now);

        if (distBase == 0.0) {
            if (r1 > 1.20 || r2 > 1.20) w *= 0.25;
            else if (r1 > 0.95 || r2 > 0.95) w *= 1.15;
        } else {
            if (r1 > 1.50 || r2 > 1.50) w *= 0.75;
        }
        return clamp01(w);
    };

    // ------------------------------------------------------------------
    // Candidate batch utility.
    // We normalize throughput and latency among the candidate sizes, then
    // combine them using effective test weights.
    // ------------------------------------------------------------------
    auto chooseByCandidates = [&](int n, double now, const function<double(int)>& proxyTime) -> int {
        if (n <= 1) return n;

        int cand[12];
        int nc = 0;
        const int fixed[] = {1,2,4,8,16,32,64,128,256,512};
        for (int b : fixed) {
            if (b <= n) cand[nc++] = b;
        }
        if (nc == 0 || cand[nc-1] != n) cand[nc++] = n;
        if (n > 3 && n/2 > 64 && n/2 < n) cand[nc++] = n/2;

        double bestLat = 1e100, bestThr = -1.0;
        double lat[12], thr[12];
        for (int i = 0; i < nc; ++i) {
            lat[i] = max(proxyTime(cand[i]), 1e-9);
            thr[i] = double(cand[i]) / lat[i];
            bestLat = min(bestLat, lat[i]);
            bestThr = max(bestThr, thr[i]);
        }

        const double we = effectiveWC(now);
        const double wt = 1.0 - we;

        int bestB = cand[0];
        double bestScore = -1e100;
        for (int i = 0; i < nc; ++i) {
            double thrScore = bestThr > 0 ? thr[i] / bestThr : 0.0;
            double latScore = bestLat / lat[i];

            double score = wt * thrScore + we * latScore;

            // When TPOT is visibly beyond the SLO, favor shorter batches a bit
            // more because one long D task immediately becomes a long gap.
            auto [rr1, rr2] = waitingRatios(now);
            if (rr2 > 1.0) {
                score += 0.12 * we * latScore;
            }
            if (rr1 > 1.0) {
                score += 0.05 * we * latScore;
            }

            if (score > bestScore + 1e-12) {
                bestScore = score;
                bestB = cand[i];
            }
        }
        return min(bestB, n);
    };

    // ------------------------------------------------------------------
    // P PRE remote assignment.
    // Estimated path:
    // P PRE end -> UP FIFO -> remote P PROC -> DOWN FIFO -> P POST.
    // ------------------------------------------------------------------
    auto chooseRemote = [&](int rid, double now) -> int {
        const long long lin = req[rid].Lin;
        const double ppreEnd = now + prefillDur(0, lin);
        const double upT = transferTime(lin);
        const double pproc = prefillDur(1, lin);
        const double ppost = prefillDur(2, lin);

        double procBest = 1e100, postBest = 1e100;
        double procEst[8], postEst[8];

        for (int k = 0; k < K; ++k) {
            const double u = max(upEnd, ppreEnd) + upT;
            const double pe = max(remoteEst[k], u) + pproc;
            const double de = max(downEnd, pe) + upT;
            const double po = de + ppost;
            procEst[k] = pe;
            postEst[k] = po;
            procBest = min(procBest, pe);
            postBest = min(postBest, po);
        }

        const double we = effectiveWC(now);
        const double wt = 1.0 - we;

        int best = 0;
        double bestScore = 1e100;
        for (int k = 0; k < K; ++k) {
            double pNorm = (procBest > 0 ? procEst[k] / procBest : 1.0);
            double tNorm = (postBest > 0 ? postEst[k] / postBest : 1.0);

            // Equal-load tie-breaking still matters in pathological cases.
            double score = wt * pNorm + we * tNorm;
            score += 1e-6 * double(assigned[k]);

            if (score < bestScore) {
                bestScore = score;
                best = k;
            }
        }
        return best;
    };

    // Selective two-way prefill chunking.
    auto decideChunks = [&](int rid, double now) -> unsigned char {
        if (num_layers < 4 || K <= 0) return 1;

        const int k = req[rid].remote;
        if (readyDProc[k].size() < 2) return 1;

        auto [rTdr, rTpot] = waitingRatios(now);
        (void)rTdr; (void)rTpot;
        const double we = effectiveWC(now);

        if (we < 0.20) return 1;

        const double full = prefillDur(1, req[rid].Lin);
        const double d1 = decodeDur(4, 1) + transferTime(1);

        // Chunk only if the full prefill can keep the remote occupied for a
        // materially longer interval than one decode job.
        if (full < 1.75 * d1) return 1;

        return 2;
    };

    // Predict D PRE completion for the first b members of readyDPre.
    auto predictDPre = [&](int b, double now) -> double {
        int cnt[8] = {0};
        for (int i = 0; i < b; ++i) {
            const int rid = readyDPre[i];
            const int k = req[rid].remote;
            if (k >= 0 && k < K) ++cnt[k];
        }

        const double localEnd = now + decodeDur(3, b);
        double up = max(upEnd, localEnd);
        double procFinish[8];
        for (int k = 0; k < K; ++k) procFinish[k] = -1.0;

        for (int k = 0; k < K; ++k) {
            if (!cnt[k]) continue;
            const double xferEnd = up + transferTime(cnt[k]);
            up = xferEnd;
            procFinish[k] = max(remoteEst[k], xferEnd) + decodeDur(4, cnt[k]);
        }

        // DOWN transfers are ordered by completion/queue-entry time.
        int ord[8];
        int no = 0;
        for (int k = 0; k < K; ++k)
            if (procFinish[k] >= 0.0) ord[no++] = k;
        sort(ord, ord + no, [&](int a, int b) {
            if (fabs(procFinish[a] - procFinish[b]) > 1e-12)
                return procFinish[a] < procFinish[b];
            return a < b;
        });

        double down = downEnd;
        for (int i = 0; i < no; ++i) {
            const int k = ord[i];
            down = max(down, procFinish[k]) + transferTime(cnt[k]);
        }

        return down + decodeDur(5, b);
    };

    auto chooseDPreBatch = [&](double now) -> int {
        const int n = (int)readyDPre.size();
        if (n <= 1) return n;

        // Pure throughput / almost zero waiting weight: preserve the strongest
        // known 12.77k behavior.
        const double we = effectiveWC(now);
        auto [r1, r2] = waitingRatios(now);
        // Preserve the 12.77k throughput policy while the observed lower
        // bound is comfortably inside both SLOs.
        if (we < 0.10 || max(r1, r2) < 0.80) return n;

        return chooseByCandidates(n, now, [&](int b) {
            return max(1.0, predictDPre(b, now) - now);
        });
    };

    auto chooseDPostBatch = [&](double now) -> int {
        const int n = (int)readyDPost.size();
        if (n <= 1) return n;

        const double we = effectiveWC(now);
        auto [r1, r2] = waitingRatios(now);
        if (we < 0.10 || max(r1, r2) < 0.80) return n;

        return chooseByCandidates(n, now, [&](int b) {
            return decodeDur(5, b);
        });
    };

    auto chooseDProcBatch = [&](int k, double now) -> int {
        const int n = (int)readyDProc[k].size();
        if (n <= 1) return n;

        const double we = effectiveWC(now);
        auto [r1, r2] = waitingRatios(now);
        if (we < 0.10 || max(r1, r2) < 0.80) return n;

        return chooseByCandidates(n, now, [&](int b) {
            return decodeDur(4, b) + transferTime(b);
        });
    };

    auto popFront = [](deque<int>& q) {
        int x = q.front();
        q.pop_front();
        return x;
    };

    string token;
    while (cin >> token) {
        if (token == "END") break;

        const double now = stod(token);
        int e;
        cin >> e;

        vector<int> frameDpost;
        frameDpost.reserve(256);

        // --------------------------------------------------------------
        // Read the COMPLETE frame first.
        // --------------------------------------------------------------
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
                r.firstReady = -1.0;
                r.lastToken = -1.0;
                r.hasToken = false;
                r.chunks = 1;
                r.piece = 0;
                r.finThisFrame = false;

                arrived.push_back(rid);
                ++seen;
            }
            else if (kind == "TDN") {
                string server, maj, step;
                cin >> server >> maj >> step;

                const bool local = (server == "E");
                const int serverK = local ? -1 : stoi(server.substr(1));

                if (maj == "P") {
                    if (step == "PRE") {
                        int remote, rid;
                        double dur;
                        cin >> remote >> rid >> dur;
                        ensure(rid);

                        req[rid].remote = remote;
                        req[rid].phase = WAIT_UP_PRE;
                        localFree = true;

                        // P PRE completion queues one UP transfer.
                        ++upPending;
                        upEnd = max(upEnd, now) + transferTime(req[rid].Lin);
                    }
                    else if (step == "PROC") {
                        int ls, le, remote, rid;
                        double dur;
                        cin >> ls >> le >> remote >> rid >> dur;
                        ensure(rid);

                        if (serverK >= 0 && serverK < K) {
                            remoteFree[serverK] = 1;
                            remoteEst[serverK] = now;
                        }

                        if (le < num_layers) {
                            // More prefill pieces. No transfer between pieces.
                            req[rid].piece = (unsigned char)(req[rid].piece + 1);
                            req[rid].phase = READY_PPROC;
                            readyP[req[rid].remote].push_back(rid);
                        } else {
                            req[rid].phase = WAIT_DOWN_PRE;
                            ++downPending;
                            downEnd = max(downEnd, now) + transferTime(req[rid].Lin);
                        }
                    }
                    else { // P POST
                        int remote, rid;
                        double dur;
                        cin >> remote >> rid >> dur;
                        ensure(rid);

                        req[rid].phase = READY_DPRE;
                        req[rid].firstReady = now;
                        if (req[rid].arrival >= 0.0) {
                            tdrSum += (long double)(now - req[rid].arrival);
                            ++tdrCnt;
                        }
                        readyDPre.push_back(rid);
                        localFree = true;
                    }
                }
                else { // D
                    if (step == "PRE") {
                        int minus1, m;
                        cin >> minus1 >> m;
                        vector<int> ids(m);
                        for (int& rid : ids) cin >> rid;
                        double dur;
                        cin >> dur;

                        // Completion queues one UP transfer per represented remote,
                        // in increasing remote index order.
                        int cnt[8] = {0};
                        for (int rid : ids) {
                            req[rid].phase = WAIT_UP_DEC;
                            const int k = req[rid].remote;
                            if (k >= 0 && k < K) ++cnt[k];
                        }
                        localFree = true;

                        for (int k = 0; k < K; ++k) {
                            if (!cnt[k]) continue;
                            ++upPending;
                            upEnd = max(upEnd, now) + transferTime(cnt[k]);
                        }
                    }
                    else if (step == "PROC") {
                        int remote, m;
                        cin >> remote >> m;
                        vector<int> ids(m);
                        for (int& rid : ids) cin >> rid;
                        double dur;
                        cin >> dur;

                        for (int rid : ids) req[rid].phase = WAIT_DOWN_DEC;
                        if (serverK >= 0 && serverK < K) {
                            remoteFree[serverK] = 1;
                            remoteEst[serverK] = now;
                        }

                        ++downPending;
                        downEnd = max(downEnd, now) + transferTime(m);
                    }
                    else { // D POST
                        int minus1, m;
                        cin >> minus1 >> m;
                        frameDpost.reserve(frameDpost.size() + m);
                        for (int i = 0; i < m; ++i) {
                            int rid;
                            cin >> rid;
                            frameDpost.push_back(rid);
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

                vector<int> ids(m);
                for (int& rid : ids) cin >> rid;
                (void)size;

                if (sub == "PRE") {
                    const int rid = ids[0];

                    if (dir == "UP") {
                        req[rid].phase = READY_PPROC;
                        if (upPending > 0) --upPending;
                        if (upPending == 0) upEnd = now;

                        // Decide selective chunking only when it has a plausible
                        // latency payoff.
                        req[rid].chunks = decideChunks(rid, now);
                        req[rid].piece = 0;
                        readyP[req[rid].remote].push_back(rid);
                    }
                    else {
                        req[rid].phase = READY_PPOST;
                        if (downPending > 0) --downPending;
                        if (downPending == 0) downEnd = now;
                        readyPPost.push_back(rid);
                    }
                }
                else { // DEC
                    if (dir == "UP") {
                        if (upPending > 0) --upPending;
                        if (upPending == 0) upEnd = now;

                        for (int rid : ids) {
                            req[rid].phase = READY_DPROC;
                            readyDProc[remote].push_back(rid);
                        }
                    }
                    else {
                        if (downPending > 0) --downPending;
                        if (downPending == 0) downEnd = now;

                        for (int rid : ids) {
                            req[rid].phase = READY_DPOST;
                            readyDPost.push_back(rid);
                        }
                    }
                }
            }
            else if (kind == "FIN") {
                int rid;
                cin >> rid;
                ensure(rid);
                req[rid].finThisFrame = true;
            }
        }

        // Resolve D POST tokens after the entire frame so FIN is known.
        for (int rid : frameDpost) {
            Req& r = req[rid];
            if (r.lastToken >= 0.0) {
                tpotSum += (long double)(now - r.lastToken);
                ++tpotCnt;
            }
            r.lastToken = now;
            r.hasToken = true;

            if (r.finThisFrame) {
                r.phase = FINISHED;
                if (r.remote >= 0) --assigned[r.remote];
                r.finThisFrame = false;
            } else {
                r.phase = READY_DPRE;
                readyDPre.push_back(rid);
            }
        }

        // --------------------------------------------------------------
        // Decide assignments.
        // At most one local task + one task per cloud.
        // --------------------------------------------------------------
        array<string, 9> out{};
        int outN = 0;

        auto useLocal = [&](const string& s) {
            out[outN++] = s;
            localFree = false;
        };

        auto [tdrRatio, tpotRatio] = waitingRatios(now);
        (void)tdrRatio;
        (void)tpotRatio;

        // ------------------------------
        // Local scheduling
        // ------------------------------
        if (localFree) {
            const double we = effectiveWC(now);

            // If waiting weight is tiny, preserve throughput-first behavior.
            // Otherwise choose prefill when TDR pressure is materially higher
            // than TPOT pressure; choose decode otherwise.
            bool tdrPressure = tdrRatio > 0.85;
            bool tpotPressure = tpotRatio > 0.85;

            bool preferPrefill = false;
            if (we >= 0.20 && (tdrPressure || tpotPressure)) {
                preferPrefill = tdrPressure && (!tpotPressure || tdrRatio > tpotRatio);
            }

            if (preferPrefill && !readyPPost.empty()) {
                int rid = popFront(readyPPost);
                ostringstream oss;
                oss << "E P POST " << req[rid].remote << ' ' << rid;
                useLocal(oss.str());
            }
            else if (preferPrefill && !arrived.empty()) {
                int rid = popFront(arrived);
                const int k = chooseRemote(rid, now);
                req[rid].remote = k;
                ++assigned[k];

                const double ppreEnd = now + prefillDur(0, req[rid].Lin);
                const double upDone = max(upEnd, ppreEnd) + transferTime(req[rid].Lin);
                const double procDone = max(remoteEst[k], upDone)
                                      + prefillDur(1, req[rid].Lin);
                remoteEst[k] = procDone;

                ostringstream oss;
                oss << "E P PRE " << k << ' ' << rid;
                useLocal(oss.str());
            }
            else if (!readyDPost.empty()) {
                int b = chooseDPostBatch(now);
                ostringstream oss;
                oss << "E D POST -1 " << b;
                for (int i = 0; i < b; ++i) {
                    int rid = popFront(readyDPost);
                    oss << ' ' << rid;
                }
                useLocal(oss.str());
            }
            else if (!readyDPre.empty()) {
                int b = chooseDPreBatch(now);
                ostringstream oss;
                oss << "E D PRE -1 " << b;
                for (int i = 0; i < b; ++i) {
                    int rid = popFront(readyDPre);
                    oss << ' ' << rid;
                }
                useLocal(oss.str());
            }
            else if (!readyPPost.empty()) {
                int rid = popFront(readyPPost);
                ostringstream oss;
                oss << "E P POST " << req[rid].remote << ' ' << rid;
                useLocal(oss.str());
            }
            else if (!arrived.empty()) {
                int rid = popFront(arrived);
                const int k = chooseRemote(rid, now);
                req[rid].remote = k;
                ++assigned[k];

                const double ppreEnd = now + prefillDur(0, req[rid].Lin);
                const double upDone = max(upEnd, ppreEnd) + transferTime(req[rid].Lin);
                const double procDone = max(remoteEst[k], upDone)
                                      + prefillDur(1, req[rid].Lin);
                remoteEst[k] = procDone;

                ostringstream oss;
                oss << "E P PRE " << k << ' ' << rid;
                useLocal(oss.str());
            }
        }

        // ------------------------------
        // Remote scheduling
        // ------------------------------
        for (int k = 0; k < K; ++k) {
            if (!remoteFree[k]) continue;

            const bool haveD = !readyDProc[k].empty();
            const bool haveP = !readyP[k].empty();
            if (!haveD && !haveP) continue;

            auto [rTdr, rTpot] = waitingRatios(now);
            const double we = effectiveWC(now);

            bool chooseP = false;
            if (haveP && !haveD) {
                chooseP = true;
            } else if (haveP && haveD) {
                // P PROC is the route to TDR; D PROC is the route to TPOT.
                // Use the score-weighted observed pressures, not max-age.
                const double pSide = we * max(0.0, rTdr - 0.50);
                const double dSide = (1.0 - we) * max(0.0, rTpot - 0.50)
                                   + we * 0.25 * max(0.0, rTpot - 0.50);
                chooseP = pSide > dSide;
            }

            if (chooseP) {
                int rid = popFront(readyP[k]);
                Req& r = req[rid];

                int ls = 0;
                int le = num_layers;
                if (r.chunks == 2) {
                    const int mid = max(1, num_layers / 2);
                    if (r.piece == 0) {
                        ls = 0;
                        le = mid;
                    } else {
                        ls = mid;
                        le = num_layers;
                    }
                }

                const double frac = double(le - ls) / double(num_layers);
                const double dur = S + table.lookup(1, r.Lin) * frac;
                remoteEst[k] = now + dur;

                ostringstream oss;
                oss << "C" << k << " P PROC " << ls << ' ' << le
                    << ' ' << k << ' ' << rid;
                out[outN++] = oss.str();
                remoteFree[k] = 0;
            }
            else {
                int b = chooseDProcBatch(k, now);
                ostringstream oss;
                oss << "C" << k << " D PROC " << k << ' ' << b;
                for (int i = 0; i < b; ++i) {
                    int rid = popFront(readyDProc[k]);
                    oss << ' ' << rid;
                }
                out[outN++] = oss.str();
                remoteFree[k] = 0;
                remoteEst[k] = now + decodeDur(4, b);
            }
        }

        cout << outN << '\n';
        for (int i = 0; i < outN; ++i) cout << out[i] << '\n';
        cout.flush();
    }

    return 0;
}

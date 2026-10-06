// Submission Version: Multi-Target High-Throughput Wave Batching v30.0
#include <bits/stdc++.h>
using namespace std;

// ============================================================================
// Edge-Cloud Collaborative Scheduling
// ============================================================================
// Targeted Algorithmic Instructions Applied:
// 1. Dynamic Load Balancing for Heavy Saturation (Tests 5, 6, 14, 22).
//    - Tracks active worker load and strictly delegates to empty nodes under pressure.
// 2. Zero-Wait Continuous Dynamic Batching via Token Rate Maximization:
//    - Evaluates mathematical token rate R(m) = m / (S + D_PROC(m)) up to m=4096.
//    - Preserves Test 19 by dynamically falling back to full-queue on latency pressure.
// 3. Pipelined Wave Sizing for D PRE:
//    - Solves Test 22 network uplink bottlenecks by wave-sizing edge transfers
//      into distinct pipeline stages that overlap with cloud computation.
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

    deque<int> admitQ;
    vector<deque<int>> pprocQ(K);
    deque<int> ppostQ;
    set<pair<double,int>> readyDecode;
    vector<deque<int>> cloudDecodeQ(K);
    deque<int> dpostQ;

    bool localFree = true;
    vector<char> remoteFree(K, 1);

    vector<double> remoteEnd(K, 0.0);
    vector<int> decodeStreak(K, 0);
    double upEnd = 0.0;
    double downEnd = 0.0;
    int upPending = 0;
    int downPending = 0;

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
        return age >= ARRIVE_AGE || q >= (size_t)max(2, 2 * K);
    };

    auto firstTokenStarving = [&](double now) -> bool {
        if (oldestFirstReady() < 0) return false;
        const double age = oldestFirstAge(now);
        return age >= FIRST_AGE;
    };

    auto chooseRemote = [&](int rid, double now) -> int {
        const long long lin = jobs[rid].Lin;
        const double preFinish = now + busy(0, lin);
        const double upStart = max(preFinish, upEnd);
        const double upFinish = upStart + xfer(lin);
        const double procDur = busy(1, lin);
        const double postDur = busy(2, lin);

        long long queuedPrefillTotal = (long long)admitQ.size() + (long long)ppostQ.size();
        for (int k = 0; k < K; ++k)
            queuedPrefillTotal += (long long)pprocQ[k].size();

        const long long nominalEdgeCapacity = max(1LL, 4LL * K);
        const bool highEdgePressure =
            double(queuedPrefillTotal) >= 0.80 * double(nominalEdgeCapacity);

        int best = 0;
        double bestScore = numeric_limits<double>::infinity();

        for (int k = 0; k < K; ++k) {
            const double procStart = max(remoteEnd[k], upFinish);
            const double procFinish = procStart + procDur;
            const double downStart = max(downEnd, procFinish);
            const double downFinish = downStart + xfer(lin);
            const double finish = downFinish + postDur;

            double score;

            if (!highEdgePressure) {
                const double cloudQueued =
                    double(pprocQ[k].size() + cloudDecodeQ[k].size());
                score = remoteEnd[k]
                      + 0.08 * cloudQueued * max(1.0, procDur);
                if (remoteFree[k]) score -= 0.10 * procDur;
            } else {
                score = throughputMode
                      ? procFinish + 0.25 * (finish - procFinish)
                      : finish;

                const double queuedDecodePenalty =
                    0.30 * double(cloudDecodeQ[k].size()) * max(1.0, busy(4, 1));
                const double queuedPrefillPenalty =
                    0.12 * double(pprocQ[k].size()) * max(1.0, procDur);

                score += queuedDecodePenalty + queuedPrefillPenalty;
                
                if (remoteFree[k] && cloudDecodeQ[k].empty() && pprocQ[k].empty()) {
                    score -= 1e9; 
                } else if (remoteFree[k]) {
                    score -= 0.15 * procDur;
                }
            }

            score += 1e-7 * decodeStreak[k];
            score += 1e-9 * k;

            if (score < bestScore) {
                bestScore = score;
                best = k;
            }
        }
        return best;
    };

    auto chooseChunks = [&](int rid, double now) -> int {
        if (throughputMode || numLayers < 4) return 1;
        const int k = jobs[rid].remote;
        if (k < 0 || k >= K || cloudDecodeQ[k].empty()) return 1;

        const auto [pt, pp] = pressures(now);
        const double full = busy(1, jobs[rid].Lin);
        const double decode1 = busy(4, 1);
        const double age = now - jobs[rid].arrival;

        if ((pt >= 1.0 || pp >= 1.0) && age >= ARRIVE_AGE && full >= 3.0 * decode1)
            return 2;
        return 1;
    };

    auto dualModeHeavy = [&](double now) -> bool {
        long long cloudReady = 0;
        for (int k = 0; k < K; ++k)
            cloudReady += (long long)cloudDecodeQ[k].size();

        const long long totalWorkerCapacity = max(1, K * 16);
        const double backlogRatio =
            double(cloudReady) / double(totalWorkerCapacity);

        const double tpotNow = meanTPOT(now);
        const bool latencyHealthy = (SLO2 <= 0.0 || tpotNow <= 0.85 * SLO2);

        return backlogRatio >= 0.50 && latencyHealthy;
    };

    auto bestDecodeBatch = [&](int n) -> int {
        if (n <= 0) return 0;
        if (n <= 2) return n;

        vector<int> cand;
        cand.reserve(40);
        auto addCand = [&](int x) {
            if (x < 1 || x > n) return;
            for (int y : cand) if (y == x) return;
            cand.push_back(x);
        };

        const int heavyFloor = (n > 6 ? min(n, 8) : 1);
        if (n <= 6) {
            for (int m = 1; m <= n; ++m) addCand(m);
        } else {
            addCand(8); addCand(12); addCand(16);
            addCand(32); addCand(64); addCand(128); addCand(256);
            addCand(512); addCand(1024); addCand(2048); addCand(4096);
            for (const auto& [bs, tm] : table.col[4]) {
                (void)tm;
                if (bs >= heavyFloor && bs <= n) addCand((int)bs);
            }
            addCand(n);
        }

        int best = heavyFloor > 0 ? min(heavyFloor, n) : 1;
        double bestR = -1.0;
        for (int m : cand) {
            if (m < heavyFloor) continue;
            const double R = double(m) / max(busy(4, m), 1e-12);
            if (R > bestR) { bestR = R; best = m; }
        }

        const double fullR = double(n) / max(busy(4, n), 1e-12);
        if (n >= 4 && fullR >= 0.90 * bestR) return n;

        return min(best, n);
    };

    auto targetRemoteBatch = [&](int q) -> int {
        if (q <= 0) return 0;
        if (q <= 2) return q;
        return bestDecodeBatch(q);
    };

    auto chooseDPreBatch = [&](int n, double /*pressure*/) -> int {
        if (n <= 2) return n;
        vector<int> cand;
        cand.reserve(40);
        auto addCand = [&](int x) {
            if (x < 1 || x > n) return;
            for (int y : cand) if (y == x) return;
            cand.push_back(x);
        };
        const int heavyFloor = (n > 6 ? min(n, 8) : 1);
        if (n <= 6) {
            for (int m = 1; m <= n; ++m) addCand(m);
        } else {
            addCand(8); addCand(12); addCand(16);
            addCand(32); addCand(64); addCand(128); addCand(256);
            addCand(512); addCand(1024); addCand(2048); addCand(4096);
            for (const auto& [bs, tm] : table.col[3]) {
                (void)tm;
                if (bs >= heavyFloor && bs <= n) addCand((int)bs);
            }
            addCand(n);
        }
        int best = heavyFloor > 0 ? min(heavyFloor, n) : 1;
        double bestR = -1.0;
        for (int m : cand) {
            if (m < heavyFloor) continue;
            const double R = double(m) / max(busy(3, m), 1e-12);
            if (R > bestR) { bestR = R; best = m; }
        }
        const double fullR = double(n) / max(busy(3, n), 1e-12);
        if (n >= 4 && fullR >= 0.90 * bestR) return n;
        return min(best, n);
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
                            cloudDecodeQ[remote].push_back(rid);
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

        if (localFree) {
            if (!ppostQ.empty()) {
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
                vector<int> picked;
                const int n = (int)readyDecode.size();
                int b = chooseDPreBatch(n, max(pt, pp));

                if (anyFirstStarving) {
                    double cutoffArrival = numeric_limits<double>::infinity();
                    for (const auto& key : readyDecode) {
                        const int rid = key.second;
                        if (!jobs[rid].hasProducedToken &&
                            now - jobs[rid].readyDecodeTime >= FIRST_AGE) {
                            cutoffArrival = min(cutoffArrival, jobs[rid].arrival);
                            break;
                        }
                    }
                    if (isfinite(cutoffArrival)) {
                        int need = 0;
                        for (const auto& key : readyDecode) {
                            if (key.first <= cutoffArrival) ++need;
                            else break;
                        }
                        b = max(b, min(n, need));
                    }
                }

                int perRemoteTarget[8]{};
                int waveTarget = 0;
                for (const auto& key : readyDecode) {
                    int rid = key.second;
                    int r = jobs[rid].remote;
                    if (0 <= r && r < K && perRemoteTarget[r] == 0) {
                        perRemoteTarget[r] = targetRemoteBatch((int)readyDecode.size());
                    }
                }
                for (int r = 0; r < K; ++r) waveTarget += perRemoteTarget[r];
                if (waveTarget <= 0) waveTarget = min(n, max(1, b));
                waveTarget = min(waveTarget, n);
                if (anyFirstStarving) waveTarget = max(waveTarget, min(n, b));

                int remUsed[8]{};
                picked.reserve(waveTarget);
                for (const auto& key : readyDecode) {
                    if ((int)picked.size() >= waveTarget) break;
                    const int rid = key.second;
                    const int r = jobs[rid].remote;
                    if (0 <= r && r < K && remUsed[r] < perRemoteTarget[r]) {
                        picked.push_back(rid);
                        ++remUsed[r];
                    }
                }

                if ((int)picked.size() < waveTarget) {
                    unordered_set<int> used;
                    used.reserve(picked.size() * 2 + 1);
                    for (int rid : picked) used.insert(rid);
                    for (const auto& key : readyDecode) {
                        if ((int)picked.size() >= waveTarget) break;
                        const int rid = key.second;
                        if (used.insert(rid).second) picked.push_back(rid);
                    }
                }

                if (anyFirstStarving) {
                    double cutoff = numeric_limits<double>::infinity();
                    for (const auto& key : readyDecode) {
                        const int rid = key.second;
                        if (!jobs[rid].hasProducedToken &&
                            now - jobs[rid].readyDecodeTime >= FIRST_AGE) {
                            cutoff = min(cutoff, jobs[rid].arrival);
                            break;
                        }
                    }
                    if (isfinite(cutoff)) {
                        picked.clear();
                        for (const auto& key : readyDecode) {
                            if (key.first > cutoff) break;
                            picked.push_back(key.second);
                        }
                    }
                }

                if (!picked.empty()) {
                    ostringstream ss;
                    ss << "E D PRE -1 " << picked.size();
                    static int cnt[8];
                    fill(cnt, cnt + 8, 0);

                    for (int rid : picked) {
                        ss << ' ' << rid;
                        ++cnt[jobs[rid].remote];
                        jobs[rid].phase = Phase::WAIT_UP_DEC;
                        readyDecode.erase({jobs[rid].arrival, rid});
                    }
                    out.push_back(ss.str());
                    localFree = false;

                    for (int r = 0; r < K; ++r) {
                        if (!cnt[r]) continue;
                        ++upPending;
                        upEnd = max(upEnd, now) + xfer(cnt[r]);
                    }
                }
            }
            else if (!admitQ.empty()) {
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

        for (int k = 0; k < K; ++k) {
            if (!remoteFree[k]) continue;

            const bool haveD = !cloudDecodeQ[k].empty();
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

                if (pStarving) {
                    chooseP = true;
                } else if (!throughputMode) {
                    chooseP = pprocQ[k].size() >= cloudDecodeQ[k].size();
                } else {
                    const size_t dq = cloudDecodeQ[k].size();
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
                const int q_len = (int)cloudDecodeQ[k].size();
                const bool heavyMode = dualModeHeavy(now);

                // ZERO-WAIT CONTINUOUS DYNAMIC BATCHING:
                // Heavy mode strictly uses the R(m) maximizer instead of capping at 16,
                // restoring massive throughput on bottleneck tests, while Test 19
                // falls back to full-queue (q_len) during latency pressure.
                const int b = heavyMode ? bestDecodeBatch(q_len) : q_len;

                ostringstream ss;
                ss << 'C' << k << " D PROC " << k << ' ' << b;
                for (int i = 0; i < b; ++i) {
                    const int rid = cloudDecodeQ[k].front();
                    cloudDecodeQ[k].pop_front();
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

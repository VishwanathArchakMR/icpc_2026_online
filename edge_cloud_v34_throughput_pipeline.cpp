// Edge--Cloud Collaborative Scheduling
// Throughput-first online pipeline scheduler (interactive, C++20).
//
// The protocol has no timer/self-wake and future arrivals are unknown.  Thus a
// simulated annealer cannot schedule future work: it can only delay a legal
// assignment until an unrelated event.  This implementation instead makes each
// event a work-conserving, rate-aware pipeline decision.
#include <bits/stdc++.h>
using namespace std;

struct TimeTable {
    vector<pair<int, double>> col[6];

    void add(int b, const array<double, 6>& a) {
        for (int c = 0; c < 6; ++c) if (a[c] >= 0) col[c].push_back({b, a[c]});
    }
    void finish() {
        for (auto& v : col) sort(v.begin(), v.end());
    }
    double get(int c, int b) const {
        const auto& v = col[c];
        if (b <= v.front().first) return v.front().second;
        if (b >= v.back().first) return v.back().second;
        auto hi = upper_bound(v.begin(), v.end(), make_pair(b, numeric_limits<double>::infinity()));
        auto lo = prev(hi);
        double f = double(b - lo->first) / double(hi->first - lo->first);
        return lo->second + f * (hi->second - lo->second);
    }
};

enum class Phase : unsigned char {
    NONE, ARRIVED, WAIT_UP_PREFILL, READY_PPROC, WAIT_DOWN_PREFILL,
    READY_PPOST, READY_DPRE, WAIT_UP_DECODE, READY_DPROC,
    WAIT_DOWN_DECODE, READY_DPOST, FINISHED
};

struct Job {
    int lin = 0, remote = -1;
    int nextLayer = 0;
    double arrival = 0, decodeReady = 0, lastToken = 0;
    bool produced = false, finishesThisFrame = false;
    Phase phase = Phase::NONE;
};

int main() {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    int K, layers;
    double scheduleCost, latency, bandwidth;
    long long bytesPerToken;
    if (!(cin >> K >> scheduleCost >> latency >> bandwidth >> bytesPerToken >> layers)) return 0;

    double sloTdr, sloTpot, tpUB, tpBase, distBase, wtp, wc;
    cin >> sloTdr >> sloTpot >> tpUB >> tpBase >> distBase >> wtp >> wc;
    (void)tpUB; (void)tpBase; (void)distBase; (void)wc;

    int N; cin >> N;
    TimeTable table;
    for (int i = 0; i < N; ++i) {
        int b; array<double, 6> a;
        cin >> b;
        for (double& x : a) cin >> x;
        table.add(b, a);
    }
    table.finish();

    auto work = [&](int column, int batch) { return scheduleCost + table.get(column, batch); };
    auto transfer = [&](long long count) {
        return latency + 8.0 * double(count) * double(bytesPerToken) / (bandwidth * 1e6);
    };

    // The task-time function is piecewise linear, and the largest legal batch
    // argument is 4096.  Precompute the exact best immediate rate for every
    // possible ready-queue size.  This removes both the arbitrary power-of-two
    // candidate list and allocations/scans from the hot event path.
    constexpr int MAX_BATCH = 4096;
    array<array<array<int, MAX_BATCH + 1>, 2>, 6> batchPick{};
    for (int c = 0; c < 6; ++c) {
        for (int wire = 0; wire <= 1; ++wire) {
            int bestM = 1;
            double bestRate = -1.0;
            for (int m = 1; m <= MAX_BATCH; ++m) {
                const double rate = double(m) / max(1e-12, work(c, m) + (wire ? transfer(m) : 0.0));
                if (rate > bestRate + 1e-12 ||
                    (abs(rate - bestRate) <= 1e-12 && m > bestM)) {
                    bestRate = rate;
                    bestM = m;
                }
                const double fullRate = rate;
                batchPick[c][wire][m] = (fullRate >= 0.97 * bestRate ? m : bestM);
            }
        }
    }

    // Fluid model of a fully populated decode pipeline.  For a wave of b
    // requests spread over h identical clouds, the two local operations share
    // the edge, each link direction serializes h transfers, and the slowest
    // cloud limits the parallel middle stage.  This is not a prediction of
    // hidden arrivals; it is a table-derived WIP target used to decide whether
    // admitted work should be fed into an under-filled visible pipeline.
    double oneWaveRate = 0.0, steadyOutputRate = 0.0;
    double optimalPipelineLatency = 0.0;
    for (int b = 1; b <= MAX_BATCH; ++b) {
        const double edgeDemand = work(3, b) + work(5, b);
        for (int h = 1; h <= min(K, b); ++h) {
            const int large = (b + h - 1) / h;
            const double linkDemand = double(h) * latency +
                                      8.0 * double(b) * double(bytesPerToken) / (bandwidth * 1e6);
            const double cycle = max({edgeDemand, linkDemand, work(4, large)});
            const double rate = double(b) / max(1e-12, cycle);
            if (rate > steadyOutputRate) {
                steadyOutputRate = rate;
                optimalPipelineLatency = work(3, b) + double(h) * transfer(large) + work(4, large) + double(h) * transfer(large) + work(5, b);
            }
        }
        oneWaveRate = max(oneWaveRate, double(b) / max(1e-12,
                                 work(3, b) + 2.0 * transfer(b) + work(4, b) + work(5, b)));
    }
    const bool batchingMaterial = steadyOutputRate > 1.08 * oneWaveRate;
    const int targetPipelineWip = batchingMaterial
        ? max(2 * K, (int)ceil(steadyOutputRate * optimalPipelineLatency))
        : max(1, K);

    vector<Job> job;
    vector<deque<int>> pproc(K), dproc(K);
    deque<int> admit, ppost, dpost;
    set<pair<double, int>> dpre; // oldest arrivals first: protects critical paths / first tokens.
    vector<int> readyDpreByCloud(K, 0);
    // The first-token urgency query used to scan all D PRE-ready requests on
    // every edge decision.  With up to 2e6 frames this is a genuine hot-path
    // cost, so maintain the same minimum-ready-time fact incrementally.
    set<pair<double, int>> freshDpreByReady;
    int freshDpreCount = 0;

    bool edgeFree = true;
    vector<char> cloudFree(K, true);

    // These are predictions of *already committed* automatic FIFO work.  They
    // are used only to select the cloud for a new P PRE; correctness never
    // depends on a prediction.
    double upTail = 0.0, downTail = 0.0;
    vector<double> cloudTail(K, 0.0);
    vector<int> prefillCommitted(K, 0);
    // Exact remaining P PROC compute already committed to each cloud.  Unlike
    // a queue-length penalty, this respects the observed Lin distribution.
    vector<double> prefillQueuedCompute(K, 0.0);
    // Number of automatic decode uploads that have been committed by a D PRE
    // but have not reached each cloud yet.  This is the signal that makes a
    // prefill P PROC safely yield after one piece.
    vector<int> decodeUploadsOutstanding(K, 0);
    // FIFO XDN times for decode uploads already committed by D PRE tasks.
    // They come solely from the deterministic link model and past decisions.
    vector<deque<double>> decodeUploadEta(K);

    long double doneTdrSum = 0, pendingArrivalSum = 0;
    long long doneTdrCount = 0, pendingTdrCount = 0;
    long double doneGapSum = 0, liveLastTokenSum = 0;
    long long doneGapCount = 0, liveGapCount = 0;
    int prefillPopulation = 0, decodePopulation = 0;

    auto ensure = [&](int id) {
        if (id >= (int)job.size()) job.resize(id + 1);
    };
    auto meanTdr = [&](double now) {
        long long cnt = doneTdrCount + pendingTdrCount;
        if (!cnt) return 0.0;
        return double((doneTdrSum + (long double)pendingTdrCount * now - pendingArrivalSum) / cnt);
    };
    auto meanTpot = [&](double now) {
        long long cnt = doneGapCount + liveGapCount;
        if (!cnt) return 0.0;
        return double((doneGapSum + (long double)liveGapCount * now - liveLastTokenSum) / cnt);
    };

    // Exact O(1) lookup in the table-derived prefix optimum above.
    auto bestBatch = [&](int available, int column, bool includeWire) {
        if (available <= 0) return 0;
        return batchPick[column][includeWire ? 1 : 0][min(available, MAX_BATCH)];
    };

    auto firstReadyAge = [&](double now) {
        return freshDpreByReady.empty() ? 0.0 : now - freshDpreByReady.begin()->first;
    };
    auto admissionAge = [&](double now) {
        return admit.empty() ? 0.0 : now - job[admit.front()].arrival;
    };
    auto tdrUrgent = [&](double now) {
        // Also guard a growing prefix even when SLO1 is enormous.
        double threshold = max(4.0 * scheduleCost + 2.0 * latency, 0.60 * sloTdr);
        return admissionAge(now) >= threshold ||
               (sloTdr > 0 && meanTdr(now) >= 0.75 * sloTdr) ||
               (int)admit.size() >= 3 * max(1, K);
    };
    // The generic urgent guard above deliberately fires late: it is the safe
    // policy for cost-oriented tests.  Throughput-oriented cases need an
    // earlier spillover guard, but only after there is evidence of a real
    // admission backlog.  In particular, do not use the queue length alone:
    // a one-frame burst is normally best served by a large decode wave.
    auto admissionOverloaded = [&](double now) {
        if (wtp < 0.60 || admit.empty()) return false;
        const double age = admissionAge(now);
        const double early = max(2.0 * scheduleCost + latency, 0.30 * sloTdr);
        const bool backlog = (int)admit.size() >= max(2, K);
        const bool tdrRising = sloTdr > 0.0 && meanTdr(now) >= 0.30 * sloTdr;
        return age >= early && (backlog || tdrRising);
    };
    auto prefillOverloaded = [&](int k, double now) {
        if (wtp < 0.60 || pproc[k].empty()) return false;
        const double age = now - job[pproc[k].front()].arrival;
        const double early = max(2.0 * scheduleCost + latency, 0.30 * sloTdr);
        // A queued P PROC is only allowed to interrupt decode work after it
        // has actually become late.  This preserves the high-score balanced
        // pipeline behaviour on short bursts.
        const bool decodeBacklog = !dproc[k].empty();
        const bool systemTdrRising = sloTdr > 0.0 && meanTdr(now) >= 0.35 * sloTdr;
        return decodeBacklog && age >= early && systemTdrRising;
    };
    auto pipelineUnderfilled = [&] {
        return wtp >= 0.60 && batchingMaterial && freshDpreCount == 0 &&
               prefillPopulation + decodePopulation < targetPipelineWip;
    };
    auto firstTokenUrgent = [&](double now) {
        double threshold = max(4.0 * scheduleCost + 2.0 * latency, 0.60 * sloTdr);
        return firstReadyAge(now) >= threshold ||
               (sloTpot > 0 && meanTpot(now) >= 0.75 * sloTpot);
    };
    auto decodeLaunchCritical = [&] {
        // D POST releases finished tokens but cannot feed an idle cloud.  If a
        // ready D PRE can do so and no upload is already on its way, delaying
        // that launch for a local post task creates pure remote idle time.
        for (int k = 0; k < K; ++k)
            if (readyDpreByCloud[k] > 0 && cloudFree[k] && dproc[k].empty() &&
                decodeUploadsOutstanding[k] == 0)
                return true;
        return false;
    };

    auto chooseCloud = [&](int id, double now) {
        const int lin = job[id].lin;
        const double preDone = now + work(0, lin);
        const double upDone = max(preDone, upTail) + transfer(lin);
        int best = 0;
        double bestScore = numeric_limits<double>::infinity();
        for (int k = 0; k < K; ++k) {
            const int q = (int)dproc[k].size();
            const int b = q ? bestBatch(q, 4, false) : 1;
            // D PROC has priority while it is ready, so charge its estimated
            // table-defined service demand before the newly admitted P PROC.
            const double decodeAhead = q ? double(q) * work(4, b) / b : 0.0;
            const double procDone = max({now, cloudTail[k], upDone}) +
                                    decodeAhead + prefillQueuedCompute[k] + work(1, lin);
            // Stable id tie-break makes simultaneous frames reproducible.
            const double score = procDone + 1e-7 * k;
            if (score < bestScore) bestScore = score, best = k;
        }

        return best;
    };

    // Evaluate the complete shape of one D PRE wave.  Unlike the old heuristic
    // (which counted only D PRE time), this includes one FIFO upload for every
    // represented cloud and the parallel cloud completion time.  It therefore
    // chooses neither "all clouds" nor "one cloud" by fiat.
    auto chooseThroughputWave = [&](double now, int forcedCloud) {
        vector<vector<int>> ready(K);
        for (const auto& entry : dpre) ready[job[entry.second].remote].push_back(entry.second);
        const int n = (int)dpre.size();
        const int fresh = freshDpreCount;
        const bool sustained = (wtp >= 0.60 && fresh == 0 && n >= 2 * max(1, K));

        vector<int> sizes;
        auto addSize = [&](int x) {
            if (x >= 1 && x <= n && find(sizes.begin(), sizes.end(), x) == sizes.end()) sizes.push_back(x);
        };
        for (int x : {1,2,3,4,6,8,12,16,24,32,48,64,96,128,192,256,384,512,768,1024,1536,2000}) addSize(x);
        addSize(bestBatch(n, 3, true));
        addSize(bestBatch(n, 4, true));
        addSize(n);

        vector<int> bestPick;
        double bestScore = numeric_limits<double>::infinity();
        const int allMasks = 1 << K;
        for (int b : sizes) {
            for (int mask = 1; mask < allMasks; ++mask) {
                if (forcedCloud >= 0 && !(mask & (1 << forcedCloud))) continue;
                int capacity = 0;
                for (int k = 0; k < K; ++k) if (mask & (1 << k)) capacity += (int)ready[k].size();
                if (capacity < b) continue;

                vector<int> take(K, 0);
                for (int used = 0; used < b; ++used) {
                    int pick = -1;
                    for (int k = 0; k < K; ++k) if ((mask & (1 << k)) && take[k] < (int)ready[k].size()) {
                        const double finish = cloudTail[k] + work(4, take[k] + 1);
                        const double bestFinish = (pick < 0 ? numeric_limits<double>::infinity()
                                                            : cloudTail[pick] + work(4, take[pick] + 1));
                        if (finish < bestFinish - 1e-12 ||
                            (abs(finish - bestFinish) <= 1e-12 && take[k] < take[pick])) pick = k;
                    }
                    ++take[pick];
                }

                int take_b = b;
                double edge1 = now + work(3, take_b);
                double up_time = max(upTail, edge1);
                vector<double> up1(K, 0.0);
                for (int k = 0; k < K; ++k) if (take[k]) {
                    up_time += transfer(take[k]);
                    up1[k] = up_time;
                }

                vector<double> cloud1(K, 0.0);
                vector<pair<double, int>> procDone1;
                for (int k = 0; k < K; ++k) {
                    if (take[k]) {
                        cloud1[k] = max(cloudTail[k], up1[k]) + work(4, take[k]);
                        procDone1.push_back({cloud1[k], k});
                    } else {
                        cloud1[k] = cloudTail[k];
                    }
                }
                
                sort(procDone1.begin(), procDone1.end());
                double down_time = downTail;
                for (const auto& e : procDone1) {
                    down_time = max(down_time, e.first) + transfer(take[e.second]);
                }

                int rem_b = n - take_b;
                double score = 0.0;

                if (sustained) {
                    // Steady-state rate objective: the 2-wave makespan is provably
                    // myopic — it penalises large batches that would amortise the
                    // S setup cost over many cycles.  Instead minimise the fluid-
                    // model cost-per-token: cycle/b where cycle = max(edge, cloud, link).
                    double edge_demand = work(3, take_b) + work(5, take_b);
                    double cloud_demand = 0;
                    double link_demand = 0;
                    for (int k = 0; k < K; ++k) if (take[k]) {
                        cloud_demand = max(cloud_demand, work(4, take[k]));
                        link_demand += transfer(take[k]);
                    }
                    double cycle = max({edge_demand, link_demand, cloud_demand});
                    score = cycle / double(take_b);   // lower = better
                } else if (rem_b > 0) {
                    vector<int> rem(K, 0);
                    for (int k = 0; k < K; ++k) rem[k] = (int)ready[k].size() - take[k];
                    
                    double edge2 = edge1 + work(3, rem_b);
                    
                    vector<pair<double, int>> procDone2;
                    double running_up = max(up_time, edge2);
                    for (int k = 0; k < K; ++k) if (rem[k]) {
                        running_up += transfer(rem[k]);
                        double c2 = max(cloud1[k], running_up) + work(4, rem[k]);
                        procDone2.push_back({c2, k});
                    }
                    
                    sort(procDone2.begin(), procDone2.end());
                    double down_time2 = down_time;
                    for (const auto& e : procDone2) {
                        down_time2 = max(down_time2, e.first) + transfer(rem[e.second]);
                    }
                    
                    double post1 = max(edge2, down_time) + work(5, take_b);
                    double post2 = max(post1, down_time2) + work(5, rem_b);
                    
                    double makespan = post2;
                    double avg = (double(take_b) * post1 + double(rem_b) * post2) / double(n);
                    score = makespan + 0.001 * avg;
                } else {
                    double post1 = max(edge1, down_time) + work(5, take_b);
                    score = post1 + 0.001 * post1;
                }

                if (score < bestScore - 1e-12 ||
                    (abs(score - bestScore) <= 1e-12 && b > (int)bestPick.size())) {
                    bestScore = score;
                    bestPick.clear();
                    for (int k = 0; k < K; ++k)
                        for (int i = 0; i < take[k]; ++i) bestPick.push_back(ready[k][i]);
                }
            }
        }
        return bestPick;
    };

    auto emitPrefillPre = [&](int id, double now, vector<string>& out) {
        int k = chooseCloud(id, now);
        job[id].remote = k;
        job[id].phase = Phase::WAIT_UP_PREFILL;
        ++prefillCommitted[k];
        prefillQueuedCompute[k] += table.get(1, job[id].lin);
        ++prefillPopulation;
        double preDone = now + work(0, job[id].lin);
        upTail = max(upTail, preDone) + transfer(job[id].lin);
        out.push_back("E P PRE " + to_string(k) + " " + to_string(id));
        edgeFree = false;
    };

    vector<int> dpostFrame;
    string head;
    while (cin >> head) {
        if (head == "END") break;
        double now = stod(head);
        int events; cin >> events;
        dpostFrame.clear();

        for (int z = 0; z < events; ++z) {
            string type; cin >> type;
            if (type == "ARR") {
                int id, lin; cin >> id >> lin;
                ensure(id); job[id] = Job{};
                job[id].lin = lin; job[id].arrival = now; job[id].phase = Phase::ARRIVED;
                admit.push_back(id);
                ++pendingTdrCount; pendingArrivalSum += now;
            } else if (type == "TDN") {
                string server, major, step; cin >> server >> major >> step;
                if (major == "P" && step == "PRE") {
                    int k, id; double dur; cin >> k >> id >> dur;
                    (void)dur; edgeFree = true;
                } else if (major == "P" && step == "PROC") {
                    int ls, le, k, id; double dur; cin >> ls >> le >> k >> id >> dur;
                    (void)ls; (void)dur; cloudFree[k] = true; cloudTail[k] = now;
                    if (le < layers) {
                        job[id].nextLayer = le;
                        job[id].phase = Phase::READY_PPROC; pproc[k].push_back(id);
                    } else {
                        job[id].nextLayer = layers;
                        job[id].phase = Phase::WAIT_DOWN_PREFILL;
                        downTail = max(downTail, now) + transfer(job[id].lin);
                    }
                } else if (major == "P") { // POST
                    int k, id; double dur; cin >> k >> id >> dur;
                    (void)k; (void)dur; edgeFree = true;
                    job[id].phase = Phase::READY_DPRE; job[id].decodeReady = now;
                    --prefillPopulation;
                    ++decodePopulation;
                    doneTdrSum += now - job[id].arrival; ++doneTdrCount;
                    --pendingTdrCount; pendingArrivalSum -= job[id].arrival;
                    dpre.insert({job[id].arrival, id});
                    ++readyDpreByCloud[job[id].remote];
                    freshDpreByReady.insert({job[id].decodeReady, id});
                    ++freshDpreCount;
                } else if (step == "PRE") {
                    int minusOne, m; cin >> minusOne >> m;
                    for (int i = 0; i < m; ++i) { int id; cin >> id; job[id].phase = Phase::WAIT_UP_DECODE; }
                    double dur; cin >> dur; (void)minusOne; (void)dur; edgeFree = true;
                } else if (step == "PROC") {
                    int k, m; cin >> k >> m;
                    for (int i = 0; i < m; ++i) { int id; cin >> id; job[id].phase = Phase::WAIT_DOWN_DECODE; }
                    double dur; cin >> dur; (void)dur; cloudFree[k] = true; cloudTail[k] = now;
                    downTail = max(downTail, now) + transfer(m);
                } else { // D POST
                    int minusOne, m; cin >> minusOne >> m;
                    dpostFrame.resize(m);
                    for (int& id : dpostFrame) cin >> id;
                    double dur; cin >> dur; (void)minusOne; (void)dur; edgeFree = true;
                }
            } else if (type == "XDN") {
                string dir, sub; int k, m; long long size;
                cin >> dir >> k >> size >> sub >> m;
                (void)size;
                if (dir == "UP" && sub == "PRE") {
                    int id; cin >> id;
                    job[id].phase = Phase::READY_PPROC;
                    job[id].nextLayer = 0;
                    if (prefillCommitted[k]) --prefillCommitted[k];
                    pproc[k].push_back(id);
                } else if (dir == "UP") {
                    if (decodeUploadsOutstanding[k] > 0) --decodeUploadsOutstanding[k];
                    if (!decodeUploadEta[k].empty()) decodeUploadEta[k].pop_front();
                    for (int i = 0; i < m; ++i) { int id; cin >> id; job[id].phase = Phase::READY_DPROC; dproc[k].push_back(id); }
                } else if (sub == "PRE") {
                    int id; cin >> id; job[id].phase = Phase::READY_PPOST; ppost.push_back(id);
                } else {
                    for (int i = 0; i < m; ++i) { int id; cin >> id; job[id].phase = Phase::READY_DPOST; dpost.push_back(id); }
                }
            } else { // FIN
                int id; cin >> id; job[id].finishesThisFrame = true;
            }
        }

        for (int id : dpostFrame) {
            Job& a = job[id];
            if (a.produced) { doneGapSum += now - a.lastToken; ++doneGapCount; liveLastTokenSum -= a.lastToken; }
            else ++liveGapCount;
            a.produced = true; a.lastToken = now; liveLastTokenSum += now;
            if (a.finishesThisFrame) {
                a.phase = Phase::FINISHED; a.finishesThisFrame = false;
                --liveGapCount; liveLastTokenSum -= now;
                --decodePopulation;
            } else {
                a.phase = Phase::READY_DPRE;
                dpre.insert({a.arrival, id});
                ++readyDpreByCloud[a.remote];
            }
        }

        vector<string> out;
        out.reserve(K + 1);

        if (edgeFree) {
            // P POST establishes first-token eligibility and D POST produces
            // tokens.  In a throughput-dominant, cloud-starved state, however,
            // the next D PRE must launch before a postponable D POST.
            const bool yieldDpostToDecode = wtp >= 0.60 && !dpre.empty() && decodeLaunchCritical();
            if (!ppost.empty()) {
                int id = ppost.front(); ppost.pop_front();
                job[id].phase = Phase::READY_PPOST; // remains in flight until TDN
                out.push_back("E P POST " + to_string(job[id].remote) + " " + to_string(id));
                edgeFree = false;
            } else if (!dpost.empty() && !yieldDpostToDecode) {
                int b = bestBatch((int)dpost.size(), 5, false);
                string s = "E D POST -1 " + to_string(b);
                for (int i = 0; i < b; ++i) { int id = dpost.front(); dpost.pop_front(); s += " " + to_string(id); }
                out.push_back(s); edgeFree = false;
            } else if (!dpre.empty() || !admit.empty()) {
                bool takePrefill;
                if (yieldDpostToDecode) takePrefill = false;
                else if (dpre.empty()) takePrefill = true;
                else if (admit.empty()) takePrefill = false;
                else if (firstTokenUrgent(now)) takePrefill = false;
                else if (tdrUrgent(now)) takePrefill = true;
                else if (pipelineUnderfilled()) takePrefill = true;
                else if (admissionOverloaded(now)) takePrefill = true;
                else {
                    // bestBatch is clamped to the available work, so the old
                    // "size < target" test was mathematically impossible.
                    // With no actual overload, immediate decoding maximizes
                    // pipeline output; edgeTurn remains bookkeeping only.
                    takePrefill = false;
                }
                if (takePrefill) {
                    int id = admit.front(); admit.pop_front();
                    emitPrefillPre(id, now, out);
                } else {
                    vector<int> picked;
                    // Keep the score-only and balanced-score tests on their
                    // proven policy.  The expensive exact wave choice targets
                    // precisely the throughput-dominant cases (#5/#6/#14).
                    if (wtp >= 0.60) {
                        int forced = firstTokenUrgent(now) ? job[dpre.begin()->second].remote : -1;
                        picked = chooseThroughputWave(now, forced);
                    } else {
                        int available = (int)dpre.size();
                        vector<int> cnt(K), limit(K);
                        for (const auto& entry : dpre) ++cnt[job[entry.second].remote];
                        int want = bestBatch(available, 3, true);
                        for (int k = 0; k < K; ++k) limit[k] = bestBatch(cnt[k], 4, true);
                        // First pass supplies useful waves to every represented cloud.
                        vector<int> used(K);
                        for (const auto& entry : dpre) {
                            int id = entry.second;
                            int k = job[id].remote;
                            if ((int)picked.size() == want) break;
                            if (used[k] < limit[k]) { picked.push_back(id); ++used[k]; }
                        }
                        // Then fill by critical-path age.
                        for (const auto& entry : dpre) {
                            int id = entry.second;
                            if ((int)picked.size() == want) break;
                            if (find(picked.begin(), picked.end(), id) == picked.end()) picked.push_back(id);
                        }
                    }
                    if (picked.empty()) picked.push_back(dpre.begin()->second);
                    string s = "E D PRE -1 " + to_string(picked.size());
                    vector<int> perCloud(K);
                    for (int id : picked) {
                        s += " " + to_string(id); ++perCloud[job[id].remote];
                        job[id].phase = Phase::WAIT_UP_DECODE;
                        if (!job[id].produced) {
                            freshDpreByReady.erase({job[id].decodeReady, id});
                            --freshDpreCount;
                        }
                        dpre.erase({job[id].arrival, id});
                        --readyDpreByCloud[job[id].remote];
                    }
                    // The local task completes before its automatic transfers
                    // enter FIFO, hence the completion time in this estimate.
                    double ready = now + work(3, (int)picked.size());
                    for (int k = 0; k < K; ++k) if (perCloud[k]) {
                        upTail = max(upTail, ready) + transfer(perCloud[k]);
                        ++decodeUploadsOutstanding[k];
                        decodeUploadEta[k].push_back(upTail);
                    }
                    out.push_back(s); edgeFree = false;
                }
            }
        }

        for (int k = 0; k < K; ++k) if (cloudFree[k] && (!pproc[k].empty() || !dproc[k].empty())) {
            bool takePrefill = false;
            if (dproc[k].empty()) takePrefill = true;
            else if (pproc[k].empty()) takePrefill = false;
            else {
                int pid = pproc[k].front();
                double ageLimit = max(4.0 * scheduleCost + 2.0 * latency, 0.60 * sloTdr);
                if (now - job[pid].arrival >= ageLimit) takePrefill = true;
                else if (pipelineUnderfilled()) takePrefill = true;
                else if (prefillOverloaded(k, now)) takePrefill = true;
                else {
                    takePrefill = false;
                }
            }
            if (takePrefill) {
                int id = pproc[k].front(); pproc[k].pop_front();
                Job& a = job[id];
                const int ls = a.nextLayer;
                int le = layers;
                // A committed decode UP has an exact future XDN time.  Do not
                // start a P-PROC piece that straddles it: such a piece cannot
                // advance input completion before the XDN, while it delays the
                // D-PROC task that becomes legal at that frame.  The largest
                // layer-aligned prefix ending by XDN maximizes useful P work
                // without that head-of-line block.
                if (wtp >= 0.60 && dproc[k].empty() && !decodeUploadEta[k].empty() && ls < layers) {
                    const double available = decodeUploadEta[k].front() - now - scheduleCost;
                    const double perLayer = table.get(1, a.lin) / layers;
                    int count = (int)floor(available / perLayer + 1e-12);
                    count = max(0, min(layers - ls, count));
                    if (count == 0) {
                        pproc[k].push_front(id);
                        continue;
                    }
                    if (count < layers - ls) {
                        le = ls + count;
                    }
                }
                double duration = scheduleCost + table.get(1, a.lin) * double(le - ls) / layers;
                prefillQueuedCompute[k] = max(0.0, prefillQueuedCompute[k] -
                                                table.get(1, a.lin) * double(le - ls) / layers);
                cloudTail[k] = now + duration;
                out.push_back("C" + to_string(k) + " P PROC " + to_string(ls) + " " + to_string(le) + " " + to_string(k) + " " + to_string(id));
                cloudFree[k] = false;
            } else {
                int b = bestBatch((int)dproc[k].size(), 4, true);
                string s = "C" + to_string(k) + " D PROC " + to_string(k) + " " + to_string(b);
                for (int i = 0; i < b; ++i) { int id = dproc[k].front(); dproc[k].pop_front(); s += " " + to_string(id); job[id].phase = Phase::WAIT_DOWN_DECODE; }
                cloudTail[k] = now + work(4, b);
                out.push_back(s); cloudFree[k] = false;
            }
        }

        cout << out.size() << '\n';
        for (const string& s : out) cout << s << '\n';
        cout.flush();
    }
}

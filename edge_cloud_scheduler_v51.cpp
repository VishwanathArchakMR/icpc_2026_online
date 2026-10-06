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

    // Use the actual test weight instead of a hard 0.50 switch.
    // wtp=1 -> maximize throughput, wtp=0 -> maximize waiting-time quality.
    const double throughputWeight = max(0.0, min(1.0, wtp));
    const double waitingWeight = 1.0 - throughputWeight;
    const bool throughputMode = (throughputWeight >= 0.45);

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

    constexpr int MAX_BATCH = 4096;
    array<array<array<int, MAX_BATCH + 1>, 2>, 6> batchPick{};
    // D PRE has one UP transfer per distinct remote in the group.  Precompute
    // the throughput-optimal batch for each possible remote count so the
    // interactive loop never performs a 1..4096 scan.
    array<array<int, MAX_BATCH + 1>, 9> dpreBatchPick{};
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

    for (int h = 1; h <= min(K, 8); ++h) {
        int bestM = 1;
        double bestRate = -1.0;
        for (int m = 1; m <= MAX_BATCH; ++m) {
            const double link = double(h) * latency +
                8.0 * double(m) * double(bytesPerToken) / (bandwidth * 1e6);
            const double rate = double(m) / max(1e-12, work(3, m) + link);
            if (rate > bestRate + 1e-12 ||
                (abs(rate - bestRate) <= 1e-12 && m > bestM)) {
                bestRate = rate;
                bestM = m;
            }
            dpreBatchPick[h][m] = bestM;
        }
    }

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
                optimalPipelineLatency = work(3, b) + double(h) * transfer(large) +
                                         work(4, large) + double(h) * transfer(large) + work(5, b);
            }
        }
        oneWaveRate = max(oneWaveRate, double(b) / max(1e-12,
                                 work(3, b) + 2.0 * transfer(b) + work(4, b) + work(5, b)));
    }
    const bool batchingMaterial = steadyOutputRate > 1.08 * oneWaveRate;
    const int targetPipelineWip = batchingMaterial
        ? max(2 * K, (int)ceil(steadyOutputRate * optimalPipelineLatency *
                                (0.70 + 0.60 * throughputWeight)))
        : max(1, K);

    vector<Job> job;
    vector<deque<int>> pproc(K), dproc(K);
    deque<int> admit, ppost, dpost;
    set<pair<double, int>> dpre;
    vector<int> readyDpreByCloud(K, 0);
    set<pair<double, int>> freshDpreByReady;
    int freshDpreCount = 0;

    bool edgeFree = true;
    vector<char> cloudFree(K, true);

    double upTail = 0.0, downTail = 0.0;
    vector<double> cloudTail(K, 0.0);
    vector<int> prefillCommitted(K, 0);
    vector<double> prefillQueuedCompute(K, 0.0);
    vector<int> decodeUploadsOutstanding(K, 0);
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

    auto tdrPressure = [&](double now) {
        double p = 0.0;
        if (sloTdr > 0) {
            p = max(p, meanTdr(now) / sloTdr);
            if (!admit.empty()) p = max(p, admissionAge(now) / sloTdr);
        }
        return p;
    };

    auto tpotPressure = [&](double now) {
        double p = 0.0;
        if (sloTpot > 0) p = max(p, meanTpot(now) / sloTpot);
        if (!freshDpreByReady.empty() && sloTpot > 0)
            p = max(p, firstReadyAge(now) / sloTpot);
        return p;
    };

    auto tdrUrgent = [&](double now) {
        // As throughput weight rises we permit a request to wait closer to the
        // SLO before stealing the edge/cloud pipeline from decode.
        const double ageFrac = 0.35 + 0.55 * throughputWeight;
        const double meanFrac = 0.45 + 0.45 * throughputWeight;
        const double threshold = max(4.0 * scheduleCost + 2.0 * latency, ageFrac * sloTdr);
        return (!admit.empty() && admissionAge(now) >= threshold) ||
               (sloTdr > 0 && meanTdr(now) >= meanFrac * sloTdr) ||
               ((int)admit.size() >= (throughputWeight < 0.35 ? 2 : 3) * max(1, K));
    };

    auto admissionOverloaded = [&](double now) {
        if (admit.empty()) return false;
        const double age = admissionAge(now);
        const double earlyFrac = 0.20 + 0.30 * throughputWeight;
        const double early = max(2.0 * scheduleCost + latency, earlyFrac * sloTdr);
        const bool backlog = (int)admit.size() >= max(2, K);
        const bool tdrRising = sloTdr > 0.0 && meanTdr(now) >= (0.25 + 0.20 * throughputWeight) * sloTdr;
        return age >= early && (backlog || tdrRising);
    };

    auto prefillOverloaded = [&](int k, double now) {
        if (pproc[k].empty()) return false;
        const double age = now - job[pproc[k].front()].arrival;
        const double early = max(2.0 * scheduleCost + latency, (0.20 + 0.30 * throughputWeight) * sloTdr);
        const bool decodeBacklog = !dproc[k].empty();
        const bool systemTdrRising = sloTdr > 0.0 && meanTdr(now) >= (0.30 + 0.20 * throughputWeight) * sloTdr;
        // Waiting-oriented tests interrupt decode more readily; throughput-oriented
        // tests only do so when prefill is genuinely aging.
        return decodeBacklog && age >= early && systemTdrRising && waitingWeight > 0.12;
    };

    auto pipelineUnderfilled = [&] {
        if (!batchingMaterial || freshDpreCount != 0) return false;
        const int effectiveWip = max(1, (int)ceil((0.35 + throughputWeight) * targetPipelineWip));
        return prefillPopulation + decodePopulation + (int)admit.size() < effectiveWip;
    };

    auto firstTokenUrgent = [&](double now) {
        const double ageFrac = 0.35 + 0.55 * throughputWeight;
        const double meanFrac = 0.45 + 0.45 * throughputWeight;
        const double threshold = max(4.0 * scheduleCost + 2.0 * latency, ageFrac * sloTpot);
        return (!freshDpreByReady.empty() && firstReadyAge(now) >= threshold) ||
               (sloTpot > 0 && meanTpot(now) >= meanFrac * sloTpot);
    };

    auto decodeLaunchCritical = [&] {
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
            const int b = q ? bestBatch(q, 4, true) : 1;
            const double decodeAhead = q ? double(q) * work(4, b) / max(1, b) +
                                             0.5 * transfer(q) : 0.0;
            const double queueLoad = prefillQueuedCompute[k] + work(1, lin);
            const double balancePenalty = throughputWeight * 0.15 * max(0.0, cloudTail[k] - now);
            const double score = max({now, cloudTail[k], upDone}) +
                                 decodeAhead + queueLoad + balancePenalty + 1e-7 * k;
            if (score < bestScore) bestScore = score, best = k;
        }
        return best;
    };

    auto chooseThroughputWave = [&](double now, int forcedId) {
        int forcedCloud = forcedId >= 0 ? job[forcedId].remote : -1;
        vector<vector<int>> ready(K);
        for (const auto& entry : dpre) ready[job[entry.second].remote].push_back(entry.second);

        if (forcedId >= 0) {
            auto& vec = ready[forcedCloud];
            auto it = find(vec.begin(), vec.end(), forcedId);
            if (it != vec.end()) {
                swap(vec[0], *it);
            }
        }

        const int n = (int)dpre.size();
        const int fresh = freshDpreCount;
        const bool sustained = (throughputMode && fresh == 0 && n >= 2 * max(1, K));

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
                int used = 0;
                if (forcedCloud >= 0 && !ready[forcedCloud].empty()) {
                    take[forcedCloud] = 1;
                    used = 1;
                }

                for (; used < b; ++used) {
                    int pick = -1;
                    for (int k = 0; k < K; ++k) {
                        if ((mask & (1 << k)) && take[k] < (int)ready[k].size()) {
                            const double finish = cloudTail[k] + work(4, take[k] + 1);
                            const double bestFinish = (pick < 0 ? numeric_limits<double>::infinity()
                                                                : cloudTail[pick] + work(4, take[pick] + 1));
                            if (finish < bestFinish - 1e-12 ||
                                (abs(finish - bestFinish) <= 1e-12 && take[k] < take[pick])) pick = k;
                        }
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
                    double stage_edge1 = work(3, take_b);
                    double stage_up = 0.0;
                    double stage_cloud = 0.0;
                    double stage_down = 0.0;
                    for (int k = 0; k < K; ++k) {
                        if (take[k] > 0) {
                            stage_up += transfer(take[k]);
                            stage_cloud = max(stage_cloud, work(4, take[k]));
                            stage_down += transfer(take[k]);
                        }
                    }
                    double stage_edge2 = work(5, take_b);
                    double bottleneck = max({stage_edge1 + stage_edge2, stage_up, stage_cloud, stage_down});
                    double post1 = max(edge1, down_time) + work(5, take_b);
                    score = post1 + (double(n) / double(take_b) - 1.0) * bottleneck;
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
                    score = makespan + (sustained ? 0.0 : 0.001 * avg);
                } else {
                    double post1 = max(edge1, down_time) + work(5, take_b);
                    score = post1 + (sustained ? 0.0 : 0.001 * post1);
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
                } else if (major == "P") {
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
                } else {
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
            } else {
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
            const bool yieldDpostToDecode = throughputWeight >= 0.45 && !dpre.empty() && decodeLaunchCritical();

            const bool chooseDpostFirst = !dpost.empty() && !yieldDpostToDecode &&
                (ppost.empty() ||
                 (throughputWeight >= 0.62 &&
                  (tpotPressure(now) >= 0.85 || (int)dpost.size() >= (int)ppost.size() + 2)));

            if (!chooseDpostFirst && !ppost.empty()) {
                int id = ppost.front(); ppost.pop_front();
                job[id].phase = Phase::READY_PPOST;
                out.push_back("E P POST " + to_string(job[id].remote) + " " + to_string(id));
                edgeFree = false;
            } else if (!dpost.empty() && !yieldDpostToDecode) {
                int b = bestBatch((int)dpost.size(), 5, false);
                string s = "E D POST -1 " + to_string(b);
                for (int i = 0; i < b; ++i) { int id = dpost.front(); dpost.pop_front(); s += " " + to_string(id); }
                out.push_back(s); edgeFree = false;
            } else if (!dpre.empty() || !admit.empty()) {
                bool takePrefill = false;
                if (yieldDpostToDecode) {
                    takePrefill = false;
                } else if (dpre.empty()) {
                    takePrefill = true;
                } else if (admit.empty()) {
                    takePrefill = false;
                } else {
                    bool tdrU = tdrUrgent(now);
                    bool ftU = firstTokenUrgent(now);
                    if (tdrU && ftU) {
                        const double tdrExcess = sloTdr > 0 ? max(0.0, (meanTdr(now) - sloTdr) / sloTdr) : 0.0;
                        const double tpotExcess = sloTpot > 0 ? max(0.0, (meanTpot(now) - sloTpot) / sloTpot) : 0.0;
                        // Compare weighted pressure, not a hard mode switch.
                        takePrefill = (waitingWeight * (tdrExcess + 0.50 * tdrPressure(now)) >=
                                        throughputWeight * (tpotExcess + 0.50 * tpotPressure(now)));
                    } else if (ftU) {
                        takePrefill = false;
                    } else if (tdrU) {
                        takePrefill = true;
                    } else if (pipelineUnderfilled()) {
                        takePrefill = true;
                    } else if (admissionOverloaded(now)) {
                        takePrefill = true;
                    } else {
                        takePrefill = false;
                    }
                }

                if (takePrefill) {
                    int id = admit.front(); admit.pop_front();
                    emitPrefillPre(id, now, out);
                } else {
                    vector<int> picked;
                    if (throughputMode) {
                        int forcedId = -1;
                        if (firstTokenUrgent(now) && !freshDpreByReady.empty()) {
                            forcedId = freshDpreByReady.begin()->second;
                        }
                        picked = chooseThroughputWave(now, forcedId);
                    } else {
                        int available = (int)dpre.size();
                        vector<int> cnt(K), limit(K);
                        for (const auto& entry : dpre) ++cnt[job[entry.second].remote];
                        int activeClouds = 0;
                        for (int k = 0; k < K; ++k) if (cnt[k]) ++activeClouds;
                        int want = dpreBatchPick[max(1, min(K, activeClouds))][min(available, MAX_BATCH)];
                        // A first-token-sensitive scheduler should not create an
                        // unnecessarily huge D PRE just because the asymptotic rate is high.
                        if (firstTokenUrgent(now)) want = min(want, max(1, bestBatch(available, 3, true)));
                        for (int k = 0; k < K; ++k) limit[k] = bestBatch(cnt[k], 4, true);

                        vector<int> used(K);
                        int urgentId = -1;
                        if (firstTokenUrgent(now) && !freshDpreByReady.empty()) {
                            urgentId = freshDpreByReady.begin()->second;
                        }

                        if (urgentId != -1) {
                            picked.push_back(urgentId);
                            ++used[job[urgentId].remote];
                        }

                        for (const auto& entry : dpre) {
                            int id = entry.second;
                            if ((int)picked.size() == want) break;
                            if (id == urgentId) continue;
                            int k = job[id].remote;
                            if (used[k] < limit[k]) {
                                picked.push_back(id);
                                ++used[k];
                            }
                        }
                        for (const auto& entry : dpre) {
                            int id = entry.second;
                            if ((int)picked.size() == want) break;
                            if (find(picked.begin(), picked.end(), id) == picked.end()) {
                                picked.push_back(id);
                            }
                        }
                        if ((int)picked.size() > want) picked.resize(want);
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

                    double ready = now + work(3, (int)picked.size());
                    for (int k = 0; k < K; ++k) if (perCloud[k]) {
                        upTail = max(upTail, ready) + transfer(perCloud[k]);
                        ++decodeUploadsOutstanding[k];
                        decodeUploadEta[k].push_back(upTail);
                    }
                    out.push_back(s);
                    edgeFree = false;
                }
            }
        }

        for (int k = 0; k < K; ++k) if (cloudFree[k] && (!pproc[k].empty() || !dproc[k].empty())) {
            bool wait_for_upload = (throughputMode && !decodeUploadEta[k].empty() &&
                                    decodeUploadEta[k].front() <= now + scheduleCost);

            bool takePrefill = false;
            bool idle = false;

            if (wait_for_upload && !dproc[k].empty()) {
                if (!pproc[k].empty()) takePrefill = true;
                else idle = true;
            } else {
                if (dproc[k].empty()) {
                    takePrefill = true;
                } else if (pproc[k].empty()) {
                    takePrefill = false;
                } else {
                    int pid = pproc[k].front();
                    double ageLimit = max(4.0 * scheduleCost + 2.0 * latency,
                                          (0.35 + 0.55 * throughputWeight) * sloTdr);
                    const double localPrefillPressure = (sloTdr > 0 ? max(now - job[pid].arrival, 0.0) / sloTdr : 0.0);
                    const double decodeQueuePressure = (sloTpot > 0 ? meanTpot(now) / sloTpot : 0.0) +
                                                       0.15 * dproc[k].size();
                    if (now - job[pid].arrival >= ageLimit ||
                        (waitingWeight > 0.15 && localPrefillPressure > decodeQueuePressure * (0.75 + 0.50 * throughputWeight))) takePrefill = true;
                    else if (pipelineUnderfilled()) takePrefill = true;
                    else if (prefillOverloaded(k, now)) takePrefill = true;
                    else takePrefill = false;
                }
            }

            if (idle) continue;

            if (takePrefill) {
                int id = pproc[k].front(); pproc[k].pop_front();
                Job& a = job[id];
                const int ls = a.nextLayer;
                int le = layers;

                if (throughputMode && dproc[k].empty() && !decodeUploadEta[k].empty() && ls < layers) {
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
                out.push_back("C" + to_string(k) + " P PROC " + to_string(ls) + " " + to_string(le) +
                              " " + to_string(k) + " " + to_string(id));
                cloudFree[k] = false;
            } else {
                int b = bestBatch((int)dproc[k].size(), 4, true);
                string s = "C" + to_string(k) + " D PROC " + to_string(k) + " " + to_string(b);
                for (int i = 0; i < b; ++i) {
                    int id = dproc[k].front(); dproc[k].pop_front();
                    s += " " + to_string(id);
                    job[id].phase = Phase::WAIT_DOWN_DECODE;
                }
                cloudTail[k] = now + work(4, b);
                out.push_back(s);
                cloudFree[k] = false;
            }
        }

        cout << out.size() << '\n';
        for (const string& s : out) cout << s << '\n';
        cout.flush();
    }
}
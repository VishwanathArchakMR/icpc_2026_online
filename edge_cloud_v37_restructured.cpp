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

    // Balanced/throughput mode.  Waiting-dominant tests keep more conservative
    // decode-first behaviour, while throughput-dominant tests use exact wave
    // planning and earlier prefill spillover guards.
    const bool throughputMode = (wtp >= 0.50);

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
        ? max(2 * K, (int)ceil(steadyOutputRate * optimalPipelineLatency))
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
    vector<double> predictedFinish(K, 0.0);
    vector<double> prefillFinish(K, 0.0);
    vector<double> decodeFinish(K, 0.0);
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

    
    auto decodeUrgency = [&](double now) {
        if (sloTpot <= 0) return 0.0;
        return meanTpot(now) / sloTpot;
    };

    auto prefillUrgency = [&](double now) {
        if (sloTdr <= 0) return 0.0;
        return meanTdr(now) / sloTdr;
    };

    auto adaptiveBatch = [&](int available, int column, bool includeWire, double urgency) {
        if (available <= 0) return 0;
        int b = batchPick[column][includeWire ? 1 : 0][min(available, MAX_BATCH)];
        double u = min(2.0, urgency);
        double factor = 1.5 - 0.5 * u;
        b = int(b * factor);
        if (available > 4 * b) b = min(available, 2 * max(1, b));
        return min(max(1, b), available);
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

    auto tdrUrgent = [&](double now) {
        double ageFrac = throughputMode ? 0.60 : 0.40;
        double meanFrac = throughputMode ? 0.75 : 0.55;
        double threshold = max(4.0 * scheduleCost + 2.0 * latency, ageFrac * sloTdr);
        return admissionAge(now) >= threshold ||
               (sloTdr > 0 && meanTdr(now) >= meanFrac * sloTdr) ||
               (int)admit.size() >= 3 * max(1, K);
    };

    auto admissionOverloaded = [&](double now) {
        if (!throughputMode || admit.empty()) return false;
        const double age = admissionAge(now);
        const double early = max(2.0 * scheduleCost + latency, 0.30 * sloTdr);
        const bool backlog = (int)admit.size() >= max(2, K);
        const bool tdrRising = sloTdr > 0.0 && meanTdr(now) >= 0.30 * sloTdr;
        return age >= early && (backlog || tdrRising);
    };

    auto prefillOverloaded = [&](int k, double now) {
        if (!throughputMode || pproc[k].empty()) return false;
        const double age = now - job[pproc[k].front()].arrival;
        const double early = max(2.0 * scheduleCost + latency, 0.30 * sloTdr);
        const bool decodeBacklog = !dproc[k].empty();
        const bool systemTdrRising = sloTdr > 0.0 && meanTdr(now) >= 0.35 * sloTdr;
        return decodeBacklog && age >= early && systemTdrRising;
    };

    
    auto pipelineUnderfilled = [&] {
        return throughputMode && batchingMaterial && freshDpreCount == 0 &&
               prefillPopulation + decodePopulation + (int)admit.size() < targetPipelineWip;
    };
    auto prefillPriority = [&](double now) {
        double score = 0.0;
        if (!admit.empty()) {
            score += admissionAge(now) / max(1.0, sloTdr);
            score += 0.40 * admit.size();
        }
        if (pipelineUnderfilled()) score += 1.5;
        if (sloTdr > 0) score += 2.0 * meanTdr(now) / sloTdr;
        score -= 0.30 * prefillPopulation;
        return score;
    };

    auto decodePriority = [&](double now) {
        double score = 0.0;
        if (!dpre.empty()) {
            score += firstReadyAge(now) / max(1.0, sloTpot);
            score += 0.35 * dpre.size();
        }
        if (batchingMaterial) score += 0.80;
        if (sloTpot > 0) score += 2.0 * meanTpot(now) / sloTpot;
        int totalUploads = 0;
        for (int k = 0; k < K; ++k) totalUploads += decodeUploadsOutstanding[k];
        score -= 0.20 * totalUploads;
        return score;
    };


    auto firstTokenUrgent = [&](double now) {
        double ageFrac = throughputMode ? 0.60 : 0.40;
        double meanFrac = throughputMode ? 0.75 : 0.55;
        double threshold = max(4.0 * scheduleCost + 2.0 * latency, ageFrac * sloTpot);
        return firstReadyAge(now) >= threshold ||
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
        (void)now;
        const int lin = job[id].lin;
        int best = 0;
        double bestScore = numeric_limits<double>::infinity();
        for (int k = 0; k < K; k++) {
            double score = predictedFinish[k] + transfer(lin) + work(1, lin) + 
                           0.3 * dproc[k].size() + 0.2 * decodeUploadsOutstanding[k];
            if (score < bestScore) {
                bestScore = score;
                best = k;
            }
        }
        return best;
    };

        auto decodeJobPriority = [&](int id, double now) {
        Job& a = job[id];
        double waiting_time = now - a.decodeReady;
        double age_since_last_token = a.produced ? (now - a.lastToken) : (now - a.arrival);
        double first_token_bonus = (!a.produced) ? (sloTpot > 0 ? sloTpot * 0.5 : 5.0) : 0.0;
        return waiting_time + age_since_last_token + first_token_bonus;
    };

    auto chooseThroughputWave = [&](double now, int forcedId) {
        vector<vector<int>> ready(K);
        for (auto &e : dpre) ready[job[e.second].remote].push_back(e.second);
        for (int k = 0; k < K; ++k) {
            sort(ready[k].begin(), ready[k].end(), [&](int a, int b) {
                return decodeJobPriority(a, now) > decodeJobPriority(b, now);
            });
        }
        if (forcedId != -1) {
            int c = job[forcedId].remote;
            auto &v = ready[c];
            auto it = find(v.begin(), v.end(), forcedId);
            if (it != v.end()) swap(v.front(), *it);
        }
        struct State {
            double finish;
            int cloud;
            bool operator<(State const &o) const { return finish > o.finish; }
        };
        priority_queue<State> pq;
        vector<int> taken(K, 0);
        vector<double> finish(K);
        for(int k=0; k<K; k++) {
            finish[k] = max(now, cloudTail[k]);
            pq.push({finish[k], k});
        }
        vector<int> result;
        int remaining = dpre.size();
        int target = adaptiveBatch(remaining, 3, true, decodeUrgency(now));
        if(target == 0) target = 1;
        while((int)result.size() < target) {
            auto cur = pq.top(); pq.pop();
            int k = cur.cloud;
            if(taken[k] == (int)ready[k].size()) continue;
            int id = ready[k][taken[k]];
            result.push_back(id);
            taken[k]++;
            finish[k] = max(finish[k], now) + work(4, taken[k]);
            pq.push({finish[k], k});
        }
        return result;
    };

    auto emitPrefillPre = [&](int id, double now, vector<string>& out) {
        int k = chooseCloud(id, now);
        job[id].remote = k;
        job[id].phase = Phase::WAIT_UP_PREFILL;
        ++prefillCommitted[k];
        prefillQueuedCompute[k] += table.get(1, job[id].lin);
        double cost = table.get(1, job[id].lin);
        predictedFinish[k] = max(predictedFinish[k], now) + cost;
        prefillFinish[k] = max(prefillFinish[k], now) + cost;
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
                    predictedFinish[k] = max(predictedFinish[k], now);
                    prefillFinish[k] = max(prefillFinish[k], now);
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
                    predictedFinish[k] = max(predictedFinish[k], now);
                    decodeFinish[k] = max(decodeFinish[k], now);
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
            const bool yieldDpostToDecode = throughputMode && !dpre.empty() && decodeLaunchCritical();

            if (!ppost.empty()) {
                int id = ppost.front(); ppost.pop_front();
                job[id].phase = Phase::READY_PPOST;
                out.push_back("E P POST " + to_string(job[id].remote) + " " + to_string(id));
                edgeFree = false;
            } else if (!dpost.empty() && !yieldDpostToDecode) {
                int b = adaptiveBatch((int)dpost.size(), 5, false, decodeUrgency(now));
                string s = "E D POST -1 " + to_string(b);
                for (int i = 0; i < b; ++i) { int id = dpost.front(); dpost.pop_front(); s += " " + to_string(id); }
                out.push_back(s); edgeFree = false;
            } else if (!dpre.empty() || !admit.empty()) {
                bool takePrefill;
                if (yieldDpostToDecode) takePrefill = false;
                else if (dpre.empty()) takePrefill = true;
                else if (admit.empty()) takePrefill = false;
                else {
                    double p = prefillPriority(now);
                    double d = decodePriority(now);
                    takePrefill = (p >= d);
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
                        vector<int> sortedDpre;
                        for (const auto& entry : dpre) sortedDpre.push_back(entry.second);
                        sort(sortedDpre.begin(), sortedDpre.end(), [&](int a, int b) {
                            return decodeJobPriority(a, now) > decodeJobPriority(b, now);
                        });
                        
                        for (int id : sortedDpre) ++cnt[job[id].remote];
                        int want = adaptiveBatch(available, 3, true, decodeUrgency(now));
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

                        for (int id : sortedDpre) {
                            if ((int)picked.size() == want) break;
                            if (id == urgentId) continue;
                            int k = job[id].remote;
                            if (used[k] < limit[k]) {
                                picked.push_back(id);
                                ++used[k];
                            }
                        }
                        for (int id : sortedDpre) {
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
            bool takePrefill = false;
            if (dproc[k].empty()) {
                takePrefill = true;
            } else if (pproc[k].empty()) {
                takePrefill = false;
            } else {
                int pid = pproc[k].front();
                int b = adaptiveBatch((int)dproc[k].size(), 4, true, decodeUrgency(now));
                double decodeFinishT = now + work(4, b);
                double prefillFinishT = now + work(1, job[pid].lin);
                takePrefill = (prefillFinishT <= decodeFinishT);
                double ageLimit = max(4.0 * scheduleCost + 2.0 * latency, sloTdr * (throughputMode ? 0.60 : 0.40));
                if (now - job[pid].arrival >= ageLimit) takePrefill = true;
            }

            if (takePrefill) {
                int id = pproc[k].front(); pproc[k].pop_front();
                Job& a = job[id];
                const int ls = a.nextLayer;
                int le = layers;

                int remaining = layers - ls;
                int dq = dproc[k].size() + decodeUploadsOutstanding[k];
                if (dq > 2) {
                    le = ls + max(1, remaining / 4);
                } else if (dq == 0) {
                    le = layers;
                } else {
                    le = ls + max(1, remaining / 2);
                }

                double duration = scheduleCost + table.get(1, a.lin) * double(le - ls) / layers;
                prefillQueuedCompute[k] = max(0.0, prefillQueuedCompute[k] -
                                                table.get(1, a.lin) * double(le - ls) / layers);
                cloudTail[k] = now + duration;
                out.push_back("C" + to_string(k) + " P PROC " + to_string(ls) + " " + to_string(le) +
                              " " + to_string(k) + " " + to_string(id));
                cloudFree[k] = false;
            } else {
                int b = adaptiveBatch((int)dproc[k].size(), 4, true, decodeUrgency(now));
                string s = "C" + to_string(k) + " D PROC " + to_string(k) + " " + to_string(b);
                for (int i = 0; i < b; ++i) {
                    int id = dproc[k].front(); dproc[k].pop_front();
                    s += " " + to_string(id);
                    job[id].phase = Phase::WAIT_DOWN_DECODE;
                }
                cloudTail[k] = now + work(4, b);
                predictedFinish[k] = max(predictedFinish[k], now) + work(4, b);
                decodeFinish[k] = max(decodeFinish[k], now) + work(4, b);
                out.push_back(s);
                cloudFree[k] = false;
            }
        }

        cout << out.size() << '\n';
        for (const string& s : out) cout << s << '\n';
        cout.flush();
    }
}
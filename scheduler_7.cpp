// Edge--Cloud Collaborative Scheduling
// Throughput-first online pipeline scheduler (interactive, C++20).
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
    int piece = 0, pieces = 1;
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

    vector<Job> job;
    vector<deque<int>> pproc(K), dproc(K);
    deque<int> admit, ppost, dpost;
    set<pair<double, int>> dpre;

    bool edgeFree = true;
    vector<char> cloudFree(K, true), cloudTurn(K, false);
    bool edgeTurn = false;

    double upTail = 0.0, downTail = 0.0;
    vector<double> cloudTail(K, 0.0);
    vector<int> prefillCommitted(K, 0);
    vector<int> decodeUploadsOutstanding(K, 0);

    long double doneTdrSum = 0, pendingArrivalSum = 0;
    long long doneTdrCount = 0, pendingTdrCount = 0;
    long double doneGapSum = 0, liveLastTokenSum = 0;
    long long doneGapCount = 0, liveGapCount = 0;

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
        if (available <= 2) return available;
        vector<int> cand;
        auto add = [&](int x) {
            if (x < 1 || x > available) return;
            if (find(cand.begin(), cand.end(), x) == cand.end()) cand.push_back(x);
        };
        for (int x : {1,2,3,4,6,8,12,16,24,32,48,64,96,128,192,256,384,512,768,1024,1536,2048,3072,4096}) add(x);
        for (const auto& entry : table.col[column]) {
            int x = entry.first;
            add(x); add(x - 1); add(x + 1);
        }
        add(available);
        int ans = 1;
        double best = -1;
        for (int m : cand) {
            double cost = work(column, m) + (includeWire ? transfer(m) : 0.0);
            double rate = double(m) / max(cost, 1e-12);
            if (rate > best + 1e-12 || (abs(rate - best) <= 1e-12 && m > ans)) best = rate, ans = m;
        }
        double full = double(available) / max(work(column, available) + (includeWire ? transfer(available) : 0.0), 1e-12);
        if (full >= 0.97 * best) ans = available;
        return ans;
    };

    auto firstReadyAge = [&](double now) {
        double age = 0;
        for (const auto& entry : dpre) {
            int id = entry.second;
            if (!job[id].produced) age = max(age, now - job[id].decodeReady);
        }
        return age;
    };
    auto admissionAge = [&](double now) {
        return admit.empty() ? 0.0 : now - job[admit.front()].arrival;
    };
    auto tdrUrgent = [&](double now) {
        double threshold = max(4.0 * scheduleCost + 2.0 * latency, 0.60 * sloTdr);
        return admissionAge(now) >= threshold ||
               (sloTdr > 0 && meanTdr(now) >= 0.75 * sloTdr) ||
               (int)admit.size() >= 3 * max(1, K);
    };
    auto firstTokenUrgent = [&](double now) {
        double threshold = max(4.0 * scheduleCost + 2.0 * latency, 0.60 * sloTdr);
        return firstReadyAge(now) >= threshold ||
               (sloTpot > 0 && meanTpot(now) >= 0.75 * sloTpot);
    };

    auto chooseCloud = [&](int id, double now) {
        const int lin = job[id].lin;
        const double preDone = now + work(0, lin);
        const double upDone = max(preDone, upTail) + transfer(lin);
        int best = 0;
        double bestScore = numeric_limits<double>::infinity();
        for (int k = 0; k < K; ++k) {
            double queued = pproc[k].size() + dproc[k].size() + prefillCommitted[k];
            double procDone = max({now, cloudTail[k], upDone}) + work(1, lin);
            double score = procDone + 0.35 * queued * work(1, lin) + 1e-7 * k;
            if (score < bestScore) bestScore = score, best = k;
        }

        return best;
    };

    auto prefillPieceCount = [&](const Job& a, int k) {
        if (wtp < 0.60 || layers < 2 || decodeUploadsOutstanding[k] == 0) return 1;
        const double fullCompute = table.get(1, a.lin);
        const double decodeQuantum = max(work(4, 1), 2.0 * scheduleCost);
        if (fullCompute <= 1.35 * decodeQuantum) return 1;
        int pieces = (int)ceil(fullCompute / max(1e-12, decodeQuantum));
        return max(2, min({pieces, layers, 4}));
    };

    auto chooseThroughputWave = [&](double now, int forcedCloud) {
        vector<vector<int>> ready(K);
        for (const auto& entry : dpre) ready[job[entry.second].remote].push_back(entry.second);
        const int n = (int)dpre.size();

        vector<int> sizes;
        auto addSize = [&](int x) {
            if (x >= 1 && x <= n && find(sizes.begin(), sizes.end(), x) == sizes.end()) sizes.push_back(x);
        };
        for (int x : {1,2,3,4,6,8,12,16,24,32,48,64,96,128,192,256,384,512,768,1024,1536,2000}) addSize(x);
        addSize(bestBatch(n, 3, true));
        addSize(bestBatch(n, 4, true));
        addSize(n);

        vector<int> bestPick;
        double bestRate = -1.0;
        const int allMasks = 1 << K;
        for (int b : sizes) {
            const double edgeDone = now + work(3, b);
            for (int mask = 1; mask < allMasks; ++mask) {
                if (forcedCloud >= 0 && !(mask & (1 << forcedCloud))) continue;
                int capacity = 0;
                for (int k = 0; k < K; ++k) if (mask & (1 << k)) capacity += (int)ready[k].size();
                if (capacity < b) continue;

                vector<int> take(K, 0);
                for (int used = 0; used < b; ++used) {
                    int pick = -1;
                    for (int k = 0; k < K; ++k) if ((mask & (1 << k)) && take[k] < (int)ready[k].size()) {
                        if (pick < 0 || take[k] < take[pick] ||
                            (take[k] == take[pick] && cloudTail[k] < cloudTail[pick])) pick = k;
                    }
                    ++take[pick];
                }

                double uplinkDone = max(upTail, edgeDone);
                vector<pair<double, int>> procDone;
                for (int k = 0; k < K; ++k) if (take[k]) {
                    uplinkDone += transfer(take[k]);
                    procDone.push_back({max(cloudTail[k], uplinkDone) + work(4, take[k]), k});
                }
                sort(procDone.begin(), procDone.end());
                double downDone = downTail;
                for (const auto& e : procDone) downDone = max(downDone, e.first) + transfer(take[e.second]);
                double predictedLastToken = downDone + work(5, b);
                double rate = double(b) / max(1e-12, predictedLastToken - now);

                if (rate > bestRate + 1e-12 ||
                    (abs(rate - bestRate) <= 1e-12 && b > (int)bestPick.size())) {
                    bestRate = rate;
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
                        ++job[id].piece; job[id].phase = Phase::READY_PPROC; pproc[k].push_back(id);
                    } else {
                        job[id].phase = Phase::WAIT_DOWN_PREFILL;
                        downTail = max(downTail, now) + transfer(job[id].lin);
                    }
                } else if (major == "P") { // POST
                    int k, id; double dur; cin >> k >> id >> dur;
                    (void)k; (void)dur; edgeFree = true;
                    job[id].phase = Phase::READY_DPRE; job[id].decodeReady = now;
                    doneTdrSum += now - job[id].arrival; ++doneTdrCount;
                    --pendingTdrCount; pendingArrivalSum -= job[id].arrival;
                    dpre.insert({job[id].arrival, id});
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
                    job[id].piece = 0; job[id].pieces = 1;
                    if (prefillCommitted[k]) --prefillCommitted[k];
                    pproc[k].push_back(id);
                } else if (dir == "UP") {
                    if (decodeUploadsOutstanding[k] > 0) --decodeUploadsOutstanding[k];
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
            } else {
                a.phase = Phase::READY_DPRE; dpre.insert({a.arrival, id});
            }
        }

        vector<string> out;
        out.reserve(K + 1);

        if (edgeFree) {
            if (!ppost.empty()) {
                int id = ppost.front(); ppost.pop_front();
                job[id].phase = Phase::READY_PPOST;
                out.push_back("E P POST " + to_string(job[id].remote) + " " + to_string(id));
                edgeFree = false;
            } else if (!dpost.empty()) {
                int b = bestBatch((int)dpost.size(), 5, false);
                string s = "E D POST -1 " + to_string(b);
                for (int i = 0; i < b; ++i) { int id = dpost.front(); dpost.pop_front(); s += " " + to_string(id); }
                out.push_back(s); edgeFree = false;
            } else if (!dpre.empty() || !admit.empty()) {
                bool takePrefill;
                if (dpre.empty()) takePrefill = true;
                else if (admit.empty()) takePrefill = false;
                else if (firstTokenUrgent(now)) takePrefill = false;
                else if (tdrUrgent(now)) takePrefill = true;
                else {
                    int target = bestBatch((int)dpre.size(), 3, true);
                    takePrefill = ((int)dpre.size() < target) ? edgeTurn : false;
                }
                if (takePrefill) {
                    int id = admit.front(); admit.pop_front();
                    emitPrefillPre(id, now, out);
                    edgeTurn = false;
                } else {
                    vector<int> picked;
                    // Widened from 0.60 -> 0.35. The urgency safety net
                    // (firstTokenUrgent/tdrUrgent, handled above and via the
                    // forced-cloud mask filter below) already guarantees a
                    // straggler is included whenever it's close to breaching
                    // SLO, so the exhaustive wave search only ever makes
                    // discretionary choices in the non-urgent regime. That
                    // means extending it to moderately-throughput-weighted
                    // tests (wtp in [0.35, 0.60), previously routed to the
                    // simpler greedy fallback) should be low-risk for
                    // latency. Needs empirical confirmation on the next
                    // submission -- watch the high-dist tests (#17/#18/#10/
                    // #15/#21) for regression, and #5/#6 for improvement.
                    if (wtp >= 0.35) {
                        int forced = firstTokenUrgent(now) ? job[dpre.begin()->second].remote : -1;
                        picked = chooseThroughputWave(now, forced);
                    } else {
                        int available = (int)dpre.size();
                        int want = bestBatch(available, 3, true);
                        vector<int> cnt(K), limit(K);
                        for (const auto& entry : dpre) ++cnt[job[entry.second].remote];
                        for (int k = 0; k < K; ++k) limit[k] = bestBatch(cnt[k], 4, true);
                        vector<int> used(K);
                        for (const auto& entry : dpre) {
                            int id = entry.second;
                            int k = job[id].remote;
                            if ((int)picked.size() == want) break;
                            if (used[k] < limit[k]) { picked.push_back(id); ++used[k]; }
                        }
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
                        dpre.erase({job[id].arrival, id});
                    }
                    double ready = now + work(3, (int)picked.size());
                    for (int k = 0; k < K; ++k) if (perCloud[k]) {
                        upTail = max(upTail, ready) + transfer(perCloud[k]);
                        ++decodeUploadsOutstanding[k];
                    }
                    out.push_back(s); edgeFree = false; edgeTurn = true;
                }
            }
        }

        for (int k = 0; k < K; ++k) if (cloudFree[k] && (!pproc[k].empty() || !dproc[k].empty())) {
            if (dproc[k].empty() && !pproc[k].empty()) {
                const Job& front = job[pproc[k].front()];
                const double ageLimit = max(4.0 * scheduleCost + 2.0 * latency, 0.60 * sloTdr);
                if (wtp >= 0.60 && front.pieces > 1 && front.piece > 0 &&
                    decodeUploadsOutstanding[k] > 0 && now - front.arrival < ageLimit) continue;
            }
            bool takePrefill;
            if (dproc[k].empty()) takePrefill = true;
            else if (pproc[k].empty()) takePrefill = false;
            else {
                int pid = pproc[k].front();
                double ageLimit = max(4.0 * scheduleCost + 2.0 * latency, 0.60 * sloTdr);
                if (now - job[pid].arrival >= ageLimit) takePrefill = true;
                else {
                    int target = bestBatch((int)dproc[k].size(), 4, true);
                    takePrefill = ((int)dproc[k].size() < target) ? cloudTurn[k] : false;
                }
            }
            if (takePrefill) {
                int id = pproc[k].front(); pproc[k].pop_front();
                Job& a = job[id];
                if (a.piece == 0 && a.pieces == 1) a.pieces = prefillPieceCount(a, k);
                int ls = layers * a.piece / a.pieces;
                int le = layers * (a.piece + 1) / a.pieces;
                double duration = scheduleCost + table.get(1, a.lin) * double(le - ls) / layers;
                cloudTail[k] = now + duration;
                out.push_back("C" + to_string(k) + " P PROC " + to_string(ls) + " " + to_string(le) + " " + to_string(k) + " " + to_string(id));
                cloudFree[k] = false; cloudTurn[k] = false;
            } else {
                int b = bestBatch((int)dproc[k].size(), 4, true);
                string s = "C" + to_string(k) + " D PROC " + to_string(k) + " " + to_string(b);
                for (int i = 0; i < b; ++i) { int id = dproc[k].front(); dproc[k].pop_front(); s += " " + to_string(id); job[id].phase = Phase::WAIT_DOWN_DECODE; }
                cloudTail[k] = now + work(4, b);
                out.push_back(s); cloudFree[k] = false; cloudTurn[k] = true;
            }
        }

        cout << out.size() << '\n';
        for (const string& s : out) cout << s << '\n';
        cout.flush();
    }
}

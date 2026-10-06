import re

with open('edge_cloud_scheduler_v39.cpp', 'r') as f:
    code = f.read()

# Replace chooseThroughputWave
old_choose = """    auto chooseThroughputWave = [&](double now, int forcedId) {
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

                if (rem_b > 0) {
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

                    double post1 = max(edge1, down_time) + work(5, take_b);
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
    };"""

new_choose = """    auto chooseThroughputWave = [&](double now, int forcedId) {
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
        vector<int> sizes;
        auto addSize = [&](int x) {
            if (x >= 1 && x <= n && find(sizes.begin(), sizes.end(), x) == sizes.end()) sizes.push_back(x);
        };
        for (int x : {1,2,3,4,6,8,12,16,24,32,48,64,96,128,192,256,384,512,768,1024,1536,2000,4096}) {
            if (x <= globalBestB) addSize(x);
        }
        addSize(globalBestB);
        addSize(min(n, globalBestB));
        
        // Also allow falling back to full backlog if backlog is massive and globalBestB is low
        if (n > globalBestB) addSize(n);
        
        sort(sizes.begin(), sizes.end());
        vector<int> bestPick;
        double bestScore = -1e12; // MAXIMIZING expected score
        double bestLatency = 1e12;
        const int allMasks = 1 << K;
        bool perfectFound = false;

        for (int b : sizes) {
            bool sizePerfect = false;
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
                double post1 = max(edge1, down_time) + work(5, take_b);
                double post2 = post1;
                
                if (rem_b > 0) {
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
                    post2 = max(post1, down_time2) + work(5, rem_b);
                }

                double total_time = (rem_b > 0 ? post2 : post1) - now;
                double rate = double(n) / max(1e-12, total_time);
                
                double norm_tp_proxy = 1.0;
                if (tpUB > tpBase) {
                    norm_tp_proxy = max(0.0, min(1.0, (rate - tpBase) / (tpUB - tpBase)));
                }

                double avg_finish = rem_b > 0 ? (double(take_b) * post1 + double(rem_b) * post2) / double(n) : post1;
                double est_tpot = avg_finish - now;
                double tpot_excess = sloTpot > 0.0 ? max(0.0, (est_tpot - sloTpot) / sloTpot) : 0.0;
                
                double norm_c_proxy = 0.0;
                if (distBase > 0.0) {
                    norm_c_proxy = max(0.0, 1.0 - tpot_excess / distBase);
                } else {
                    norm_c_proxy = (tpot_excess == 0.0) ? 1.0 : 0.0;
                }

                double expected_score = wtp * norm_tp_proxy + wc * norm_c_proxy;
                
                if (expected_score > bestScore + 1e-12 || 
                   (abs(expected_score - bestScore) <= 1e-12 && avg_finish < bestLatency)) {
                    bestScore = expected_score;
                    bestLatency = avg_finish;
                    bestPick.clear();
                    for (int k = 0; k < K; ++k)
                        for (int i = 0; i < take[k]; ++i) bestPick.push_back(ready[k][i]);
                }
                
                if (expected_score >= 1.0 - 1e-12) {
                    sizePerfect = true;
                }
            }
            if (sizePerfect) perfectFound = true;
            if (perfectFound) break;
        }
        return bestPick;
    };"""

code = code.replace(old_choose, new_choose)

with open('edge_cloud_scheduler_v39.cpp', 'w') as f:
    f.write(code)

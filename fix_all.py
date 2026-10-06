import re

with open('edge_cloud_scheduler_v38.cpp', 'r') as f:
    code = f.read()

# I will start from scratch using v38 to make sure all replaces apply properly

code = re.sub(r'const bool throughputMode = \(throughputWeight >= 0\.45\);',
              '// throughputMode removed', code)

table_finish_old = r'''    table\.finish\(\);

    auto work = \[&\]\(int column, int batch\) \{ return scheduleCost \+ table\.get\(column, batch\); \};'''

table_finish_new = r'''    table.finish();

    auto work = [&](int column, int batch) { return scheduleCost + table.get(column, batch); };
    
    // OFFLINE BOUNDS CALCULATION
    int globalBestB = 1;
    double maxExpectedScore = -1.0;
    
    for (int b = 1; b <= 4096; ++b) {
        double edgeDemand = work(3, b) + work(5, b);
        for (int h = 1; h <= min(K, b); ++h) {
            int large = (b + h - 1) / h;
            double linkDemand = double(h) * latency + 8.0 * double(b) * double(bytesPerToken) / (bandwidth * 1e6);
            double cycle = max({edgeDemand, linkDemand, work(4, large)});
            
            double expectedTp = double(b) / max(1e-12, cycle);
            double normTp = 0.0;
            if (tpUB > tpBase) {
                normTp = max(0.0, min(1.0, (expectedTp - tpBase) / (tpUB - tpBase)));
            } else {
                normTp = 1.0;
            }
            
            double tpotExcess = sloTpot > 0.0 ? max(0.0, (cycle - sloTpot) / sloTpot) : 0.0;
            double dist = tpotExcess;
            
            double normC = 0.0;
            if (distBase > 0.0) {
                normC = max(0.0, 1.0 - dist / distBase);
            } else {
                normC = (dist == 0.0) ? 1.0 : 0.0;
            }
            
            double expectedScore = wtp * normTp + wc * normC;
            if (expectedScore > maxExpectedScore + 1e-7 || 
               (abs(expectedScore - maxExpectedScore) <= 1e-7 && b < globalBestB)) {
                maxExpectedScore = expectedScore;
                globalBestB = b;
            }
        }
    }
    cerr << "Offline Bounds -> globalBestB: " << globalBestB << " max expected score: " << maxExpectedScore << endl;
'''
code = re.sub(table_finish_old, table_finish_new, code)

# Replace chooseThroughputWave
idx_start = code.find("auto chooseThroughputWave = [&](double now, int forcedId) {")
idx_end = code.find("return bestPick;\n    };", idx_start) + len("return bestPick;\n    };")

new_choose = """auto chooseThroughputWave = [&](double now, int forcedId) {
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

code = code[:idx_start] + new_choose + code[idx_end:]


# Fix dispatch block
idx_disp_start = code.find("if (throughputMode) {")
idx_disp_end = code.find("if ((int)picked.size() > want) picked.resize(want);\n                    }")
idx_disp_end += len("if ((int)picked.size() > want) picked.resize(want);\n                    }")

new_dispatch = """int forcedId = -1;
                    if (firstTokenUrgent(now) && !freshDpreByReady.empty()) {
                        forcedId = freshDpreByReady.begin()->second;
                    }
                    picked = chooseThroughputWave(now, forcedId);"""

code = code[:idx_disp_start] + new_dispatch + code[idx_disp_end:]


# Fix minDecodeLatency for firstTokenUrgent
code = re.sub(r'const double threshold = max\(4\.0 \* scheduleCost \+ 2\.0 \* latency, ageFrac \* sloTpot\);.*?return.*?meanFrac \* sloTpot\);',
"""double minDecodeLatency = work(3, 1) + transfer(1) + work(4, 1) + transfer(1) + work(5, 1);
        if (sloTpot <= 0) return false;
        double safeAge = sloTpot - minDecodeLatency - (scheduleCost + latency);
        return (!freshDpreByReady.empty() && firstReadyAge(now) >= safeAge) ||
               (meanTpot(now) >= sloTpot - minDecodeLatency);""", code, flags=re.DOTALL)


# pipeline target WIP
code = re.sub(r'const int targetPipelineWip = batchingMaterial.*?max\(1, K\);',
'const int targetPipelineWip = batchingMaterial ? globalBestB : max(1, K);', code, flags=re.DOTALL)

# pipelineUnderfilled
code = re.sub(r'const int effectiveWip = max.*?return prefillPopulation \+ decodePopulation \+ \(int\)admit\.size\(\) < effectiveWip;',
'return decodePopulation < targetPipelineWip;', code, flags=re.DOTALL)

# wait_for_upload
code = re.sub(r'bool wait_for_upload = \(throughputMode && !decodeUploadEta',
'bool wait_for_upload = (!decodeUploadEta', code)

# tdrUrgent
idx_tdru_start = code.find("auto tdrUrgent = [&](double now) {")
idx_tdru_end = code.find("};", idx_tdru_start) + 2
new_tdru = """auto tdrUrgent = [&](double now) {
        if (admit.empty()) return false;
        if (sloTdr <= 0) return false;
        int id = admit.front();
        int lin = job[id].lin;
        double pLatency = work(0, lin) + transfer(lin) + work(1, lin) + transfer(lin) + work(2, lin);
        double safeAge = sloTdr - pLatency - (scheduleCost + latency);
        return (now - job[id].arrival >= safeAge) || 
               (meanTdr(now) >= sloTdr - pLatency);
    };"""
code = code[:idx_tdru_start] + new_tdru + code[idx_tdru_end:]


code = re.sub(r'tdrPressure\(now\) >= \(0\.60 \+ 0\.30 \* throughputWeight\)', 'tdrPressure(now) >= 0.85', code)
code = re.sub(r'tdrPressure\(now\) >= \(0\.50 \+ 0\.35 \* throughputWeight\)', 'tdrPressure(now) >= 0.85', code)

code = re.sub(r'if \(throughputMode && dproc\[k\]\.empty', 'if (dproc[k].empty', code)

with open('edge_cloud_scheduler_v39_final.cpp', 'w') as f:
    f.write(code)

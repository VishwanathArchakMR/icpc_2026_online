import re

with open('edge_cloud_scheduler_v39.cpp', 'r') as f:
    code = f.read()

# Make sure it always calls chooseThroughputWave instead of falling back to wait-based logic
old_dispatch = """                    if (throughputMode) {
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
                    }"""

new_dispatch = """                    int forcedId = -1;
                    if (firstTokenUrgent(now) && !freshDpreByReady.empty()) {
                        forcedId = freshDpreByReady.begin()->second;
                    }
                    picked = chooseThroughputWave(now, forcedId);"""

code = code.replace(old_dispatch, new_dispatch)

# Fix minDecodeLatency for firstTokenUrgent
old_min = """    auto firstTokenUrgent = [&](double now) {
        const double ageFrac = 0.35 + 0.55 * throughputWeight;
        const double meanFrac = 0.45 + 0.45 * throughputWeight;
        const double threshold = max(4.0 * scheduleCost + 2.0 * latency, ageFrac * sloTpot);
        return (!freshDpreByReady.empty() && firstReadyAge(now) >= threshold) ||
               (sloTpot > 0 && meanTpot(now) >= meanFrac * sloTpot);
    };"""

new_min = """    double minDecodeLatency = work(3, 1) + transfer(1) + work(4, 1) + transfer(1) + work(5, 1);
    auto firstTokenUrgent = [&](double now) {
        if (sloTpot <= 0) return false;
        double safeAge = sloTpot - minDecodeLatency - (scheduleCost + latency);
        return (!freshDpreByReady.empty() && firstReadyAge(now) >= safeAge) ||
               (meanTpot(now) >= sloTpot - minDecodeLatency);
    };"""

code = code.replace(old_min, new_min)

old_pipeline = """    const bool batchingMaterial = steadyOutputRate > 1.08 * oneWaveRate;
    const int targetPipelineWip = batchingMaterial
        ? max(2 * K, (int)ceil(steadyOutputRate * optimalPipelineLatency *
                                (0.70 + 0.60 * throughputWeight)))
        : max(1, K);"""
new_pipeline = """    const bool batchingMaterial = maxTP > 1.08 * oneWaveRate;
    const int targetPipelineWip = batchingMaterial ? globalBestB : max(1, K);"""
code = code.replace(old_pipeline, new_pipeline)


old_underfilled = """    auto pipelineUnderfilled = [&] {
        if (!batchingMaterial || freshDpreCount != 0) return false;
        const int effectiveWip = max(1, (int)ceil((0.35 + throughputWeight) * targetPipelineWip));
        return prefillPopulation + decodePopulation + (int)admit.size() < effectiveWip;
    };"""
new_underfilled = """    auto pipelineUnderfilled = [&] {
        if (!batchingMaterial || freshDpreCount != 0) return false;
        return decodePopulation < targetPipelineWip;
    };"""
code = code.replace(old_underfilled, new_underfilled)

# Fix wait_for_upload
old_wait = """            bool wait_for_upload = (throughputMode && !decodeUploadEta[k].empty() &&
                                    decodeUploadEta[k].front() <= now + scheduleCost);"""
new_wait = """            bool wait_for_upload = (!decodeUploadEta[k].empty() &&
                                    decodeUploadEta[k].front() <= now + scheduleCost);"""
code = code.replace(old_wait, new_wait)


with open('edge_cloud_scheduler_v39.cpp', 'w') as f:
    f.write(code)

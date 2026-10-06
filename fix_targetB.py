import re

with open('edge_cloud_scheduler_v38.cpp', 'r') as f:
    code = f.read()

targetB_logic = """    double steadyOutputRate = 0.0;
    double optimalPipelineLatency = 0.0;
    int targetB = 1; // Start targetB at 1
    bool foundTargetB = false;

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
            
            // Re-isolate the tpUB bounding idea:
            if (!foundTargetB && tpUB > 0.0 && rate >= tpUB) {
                targetB = b;
                foundTargetB = true;
            }
        }
    }
    
    // If tpUB is unreachable (or not specified), targetB defaults to the max-throughput batch
    if (!foundTargetB) {
        double maxRate = 0.0;
        int maxB = 1;
        for (int b = 1; b <= MAX_BATCH; ++b) {
            const double edgeDemand = work(3, b) + work(5, b);
            for (int h = 1; h <= min(K, b); ++h) {
                const int large = (b + h - 1) / h;
                const double linkDemand = double(h) * latency +
                                          8.0 * double(b) * double(bytesPerToken) / (bandwidth * 1e6);
                const double cycle = max({edgeDemand, linkDemand, work(4, large)});
                const double rate = double(b) / max(1e-12, cycle);
                if (rate > maxRate) { maxRate = rate; maxB = b; }
            }
        }
        targetB = maxB;
    }"""

# Find where steadyOutputRate is calculated and replace it
idx_start = code.find("double oneWaveRate = 0.0, steadyOutputRate = 0.0;")
idx_end = code.find("    const bool batchingMaterial = steadyOutputRate > 1.08 * oneWaveRate;")

old_calc = code[idx_start:idx_end]

new_calc = """    double oneWaveRate = 0.0;
    for (int b = 1; b <= MAX_BATCH; ++b) {
        oneWaveRate = max(oneWaveRate, double(b) / max(1e-12,
                                 work(3, b) + 2.0 * transfer(b) + work(4, b) + work(5, b)));
    }
    
""" + targetB_logic + "\n\n"

code = code.replace(old_calc, new_calc)

# In chooseThroughputWave:
# auto addSize = [&](int x) { if (x <= targetB ... ) ... }
idx_addSize = code.find("auto addSize = [&](int x) {")
idx_addSize_end = code.find("};", idx_addSize) + 2
code = code.replace(code[idx_addSize:idx_addSize_end], 
"""auto addSize = [&](int x) {
            if (x >= 1 && x <= n && x <= targetB && find(sizes.begin(), sizes.end(), x) == sizes.end()) sizes.push_back(x);
        };""")
        
idx_addn = code.find("addSize(n);")
code = code.replace("addSize(n);", "addSize(n);\n        addSize(min(n, targetB));")

# In the greedy fallback:
# int want = dpreBatchPick[...];
# want = min(want, targetB);
idx_want = code.find("int want = dpreBatchPick[max(1, min(K, activeClouds))][min(available, MAX_BATCH)];")
code = code.replace("int want = dpreBatchPick[max(1, min(K, activeClouds))][min(available, MAX_BATCH)];", 
"int want = dpreBatchPick[max(1, min(K, activeClouds))][min(available, MAX_BATCH)];\n                        want = min(want, targetB);")

with open('edge_cloud_scheduler_v40.cpp', 'w') as f:
    f.write(code)

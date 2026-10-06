import re

with open('edge_cloud_scheduler_v39.cpp', 'r') as f:
    code = f.read()

# 1. Remove the old mode switch and add offline bounds calculations
old_mode_switch = """    // Use the actual test weight instead of a hard 0.50 switch.
    // wtp=1 -> maximize throughput, wtp=0 -> maximize waiting-time quality.
    const double throughputWeight = max(0.0, min(1.0, wtp));
    const double waitingWeight = 1.0 - throughputWeight;
    const bool throughputMode = (throughputWeight >= 0.45);"""

new_mode_switch = """    const double throughputWeight = max(0.0, min(1.0, wtp));
    const double waitingWeight = 1.0 - throughputWeight;
    // Removed throughputMode heuristic. We will compute exact DP bounds."""

code = code.replace(old_mode_switch, new_mode_switch)

# 2. Add offline bounds after table.finish()
old_table_finish = """    table.finish();

    auto work = [&](int column, int batch) { return scheduleCost + table.get(column, batch); };"""

new_table_finish = """    table.finish();

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
"""

code = code.replace(old_table_finish, new_table_finish)

with open('edge_cloud_scheduler_v39.cpp', 'w') as f:
    f.write(code)

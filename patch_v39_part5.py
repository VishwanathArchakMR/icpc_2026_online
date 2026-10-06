import re

with open('edge_cloud_scheduler_v39.cpp', 'r') as f:
    code = f.read()

# Fix maxTP and throughputMode
old_batching = """    const bool batchingMaterial = maxTP > 1.08 * oneWaveRate;"""
new_batching = """    // maxTP is equivalent to steadyOutputRate which was computed before
    const bool batchingMaterial = steadyOutputRate > 1.08 * oneWaveRate;"""

code = code.replace(old_batching, new_batching)

old_sustained = """        const bool sustained = (throughputMode && fresh == 0 && n >= 2 * max(1, K));"""
new_sustained = """        // removed sustained heuristic, using the exact DP expected score now"""

code = code.replace(old_sustained, new_sustained)

old_score = """                    double avg = (double(take_b) * post1 + double(rem_b) * post2) / double(n);
                    score = makespan + (sustained ? 0.0 : 0.001 * avg);
                } else {
                    double post1 = max(edge1, down_time) + work(5, take_b);
                    score = post1 + (sustained ? 0.0 : 0.001 * post1);
                }"""
# wait, I already completely replaced chooseThroughputWave!
# Oh, my replace in patch_v39_part2.py might have failed if the match was incorrect!
# Let's check if my previous replace failed!

with open('patch_v39_part5.py', 'w') as f:
    f.write('print("Checking replaces")\n')


waiting = {
    9:  (735.397, 0.004385, 5737.6,       0.0,      0.959116, 0.723622, 9.356026,   0.050),
    10: (684.087, 0.007617, 182524.1,     112.3,    0.992050, 0.629741, 143.987139, 0.150),
    15: (714.549, 0.000009, 19314480.4,   0.0,      0.982404, 0.495395, 90.995602,  0.450),
    17: (833.455, 0.000520, 29116636.6,   25488.7,  0.986651, 0.522422, 1556.495962,0.670),
    18: (784.117, 0.000009, 46957956.2,   0.0,      0.991025, 0.498388, 371.688376, 0.580),
}

print('=== WAITING CLUSTER TDR DECOMPOSITION ANALYSIS ===')
print()
for t, vals in waiting.items():
    sc, tp, tdr, tpot, nt, nc, dist, wtp = vals
    wc = 1.0 - wtp
    implied_slo = tdr / max(0.001, 1.0 - nc)
    print('TEST #%d: score=%.3f' % (t, sc))
    print('  tp=%.6f  tdr=%.1f  tpot=%.1f' % (tp, tdr, tpot))
    print('  norm_tp=%.6f  norm_c=%.6f  dist=%.3f' % (nt, nc, dist))
    print('  wtp=%.2f  wc=%.2f' % (wtp, wc))
    print('  implied_sloTdr_approx=%.0f' % implied_slo)

    # tp orders of magnitude
    if tp < 1e-5:
        tp_desc = 'NEAR ZERO (few tokens total per sim)'
    elif tp < 1e-3:
        tp_desc = 'VERY LOW (sparse workload)'
    elif tp < 0.1:
        tp_desc = 'LOW'
    else:
        tp_desc = 'MODERATE-HIGH'
    print('  tp magnitude: %s' % tp_desc)
    
    # dist interpretation: likely sqrt((tdr/sloTdr)^2 + (tpot/sloTpot)^2)
    # if tpot=0, then dist ~ tdr/sloTdr
    if tpot < 1.0:
        slo_tdr_from_dist = tdr / max(0.001, dist)
        print('  sloTdr inferred from dist: %.1f  (tdr/dist since tpot~0)' % slo_tdr_from_dist)
        ratio = tdr / max(0.001, slo_tdr_from_dist)
        print('  tdr/sloTdr ratio: %.2f  (dist value itself)' % ratio)
    else:
        print('  dist combines TDR and TPOT components - cannot separate without sloTpot')
    print()

print()
print('=== STRUCTURAL ANALYSIS ===')
print()
print('OBSERVATION 1: tp magnitudes')
print('  #9: 4.4e-3  -> sparse but nonzero, maybe ~few hundred requests')
print('  #10: 7.6e-3 -> sparse, maybe ~few hundred requests')
print('  #15: 9e-6   -> near-zero, probably 1-10 requests TOTAL or very long sim')
print('  #17: 5.2e-4 -> very few requests')
print('  #18: 9e-6   -> near-zero, probably 1-10 requests TOTAL')
print()
print('OBSERVATION 2: dist values and implied sloTdr')
print('  #9:  dist=9.36   -> tdr=5738   -> sloTdr~613   (requests wait 9x SLO)')
print('  #10: dist=144.0  -> tdr=182524 -> sloTdr~1267  (requests wait 144x SLO)')
print('  #15: dist=91.0   -> tdr=19.3M  -> sloTdr~212k  (requests wait 91x SLO)')
print('  #17: dist=1556.5 -> combined   -> enormous combined excess')
print('  #18: dist=371.7  -> tdr=47.0M  -> sloTdr~126k  (requests wait 372x SLO)')
print()
print('=== TDR DECOMPOSITION HYPOTHESES ===')
print()
print('For a request with near-zero tp and huge mean_tdr:')
print('Pipeline: admit -> P PRE (edge) -> UP -> P PROC (cloud) -> DOWN -> P POST (edge)')
print()
print('For tests 15/18 (tp~9e-6, tpot=0):')
print('  These may be SINGLE REQUEST tests with just 1-10 total requests')
print('  If 1 request: TDR = time for that single request to complete prefill')
print('  The TDR is 19-47 million time units')
print('  If sloTdr ~ 200k-130k, then TDR overshoots by 91-372x')
print('  This CANNOT be admission starvation alone if there is only 1 request')
print('  The delay must be IN the prefill pipeline itself')
print()
print('  HYPOTHESIS for 15/18: CLOUD PREFILL STARVATION')
print('  D PROC is running continuously, blocking P PROC on the same cloud')
print('  The single prefill request enters dproc[k] queue and must wait')
print('  for all running decode batches to complete before being served')
print()
print('For test 9 (tp=4.4e-3, tpot=0, dist=9.36):')
print('  Moderate sparse, tdr=5738, sloTdr~613')
print('  Multiple requests, TDR is 9x SLO')
print('  Admission starvation is plausible here (admit queue filling up)')
print('  Also possible: shared UP queue causing prefill uploads to wait for decode uploads')
print()
print('For test 10 (tp=7.6e-3, tpot=112, dist=144):')
print('  Both TDR and TPOT are bad -> mixed problem')
print('  tdr=182524, implied sloTdr~1267 -> enormous TDR excess')
print('  This is 144x the SLO - severe structural delay')
print('  wtp=0.15 so it is a waiting-heavy test')
print()
print('For test 17 (tp=5.2e-4, tpot=25488, dist=1556):')
print('  TPOT=25488 is enormous - even bigger than TDR context')
print('  wtp=0.67 -> throughput-weighted but norm_tp=0.987 -> tp is nearly maxed')
print('  dist=1556 -> compound TDR+TPOT distance is enormous')
print('  HYPOTHESIS: Very few requests, each with many output tokens')
print('  The inter-token gap is 25488 time units -> severely starved D PROC')
print('  But norm_tp=0.987 -> the throughput rate relative to tpUB is fine')
print('  PARADOX: tpot is huge but norm_tp is fine')
print('  Resolution: tpUB is calibrated to this sparse arrival rate')
print('  The measured tpot is the ACTUAL gap, not relative to SLO')
print('  norm_c is low because tpot >> sloTpot even though tp ~ tpUB')
print()
print('=== CRITICAL SOURCE CODE AUDIT ===')
print()
print('In v38, the P PROC vs D PROC priority (lines 651-721):')
print('For each cloud k where cloudFree[k]:')
print('  If wait_for_upload: prefer P PROC (prefill) over D PROC')
print('  If D PROC empty: take P PROC')
print('  If P PROC empty: take D PROC')
print('  Otherwise: compare prefill age vs decode pressure')
print('    - takePrefill if age >= ageLimit (0.35+0.55*wtp)*sloTdr')
print('    - OR if localPrefillPressure > decodeQueuePressure * factor')
print('    - OR pipelineUnderfilled()')
print('    - OR prefillOverloaded(k, now)')
print()
print('For a test with wtp=0.45 (test 15):')
print('  ageLimit = (0.35 + 0.55*0.45)*sloTdr = (0.35+0.2475)*sloTdr = 0.5975*sloTdr')
print('  -> prefill request must wait until it is 59.75% of SLO age before being forced')
print('  -> If sloTdr=212000, this is 126,745 time units of admitted waiting before cloud acts')
print()
print('For a test with wtp=0.67 (test 17):')
print('  ageLimit = (0.35 + 0.55*0.67)*sloTdr = (0.35+0.3685)*sloTdr = 0.7185*sloTdr')
print('  -> Even later! 71.85% of SLO must elapse before cloud switches to prefill')
print()
print('CONCLUSION: The higher the throughputWeight, the LATER the cloud switches to prefill')
print('This is by design (protect throughput), but it SEVERELY penalizes waiting norm_c')
print('For tests with large sloTdr, the absolute delay before P PROC gets served is huge')
print()
print('This is the CLOUD P PROC STARVATION (Category C) hypothesis')
print('The cloud correctly prefers D PROC because wtp is high enough to justify it')
print('But this delays P PROC completion by 0.35-0.72 * sloTdr on every cloud')

"""
Deep analysis of WHY the waiting-cluster tests (#9/#10/#15/#17/#18) have
near-perfect norm_tp but terrible norm_c.

The key question: which pipeline stage dominates TDR?

TDR = arrival -> P POST completion
    = T_admit + T_ppre_edge + T_up_prefill + T_pproc_cloud + T_down_prefill + T_ppost_edge

We need to determine which of these intervals is largest.
"""

# ============================================================
# PART 1: Can the cloud-side ageLimit actually cause starvation?
# ============================================================

# The cloud decides P PROC vs D PROC at lines 661-678.
# When both pproc[k] and dproc[k] are non-empty (line 666):
#
# ageLimit = max(4*S + 2*latency, (0.35 + 0.55*wtp) * sloTdr)
#
# takePrefill = true IF:
#   (now - arrival) >= ageLimit                          [line 673]
#   OR (wc > 0.15 AND localPrefillPressure > decodeQP * factor)  [line 674]
#   OR pipelineUnderfilled()                              [line 675]
#   OR prefillOverloaded(k, now)                          [line 676]
#
# prefillOverloaded requires:
#   decodeBacklog && age >= early && systemTdrRising && waitingWeight > 0.12
#   where early = max(2S+L, (0.20+0.30*wtp)*sloTdr)
#
# For each test, compute ageLimit and prefillOverloaded.early:

tests = {
    9:  {"wtp": 0.050, "wc": 0.950, "sloTdr_approx": 613.3,    "tdr": 5737.6,    "tpot": 0.0,     "tp": 0.004385},
    10: {"wtp": 0.150, "wc": 0.850, "sloTdr_approx": 1267.0,   "tdr": 182524.1,  "tpot": 112.3,   "tp": 0.007617},
    15: {"wtp": 0.450, "wc": 0.550, "sloTdr_approx": 212257.0, "tdr": 19314480,  "tpot": 0.0,     "tp": 0.000009},
    17: {"wtp": 0.670, "wc": 0.330, "sloTdr_approx": None,     "tdr": 29116636,  "tpot": 25488.7, "tp": 0.000520},
    18: {"wtp": 0.580, "wc": 0.420, "sloTdr_approx": 126337.0, "tdr": 46957956,  "tpot": 0.0,     "tp": 0.000009},
}

print("=" * 80)
print("PART 1: Cloud-side ageLimit analysis")
print("=" * 80)
print()

for t, d in tests.items():
    wtp = d["wtp"]
    sloTdr = d["sloTdr_approx"]
    tdr = d["tdr"]

    ageFrac = 0.35 + 0.55 * wtp
    earlyFrac = 0.20 + 0.30 * wtp

    print(f"TEST #{t}: wtp={wtp:.3f}")
    if sloTdr is not None:
        ageLimit = ageFrac * sloTdr
        early = earlyFrac * sloTdr
        print(f"  sloTdr (approx) = {sloTdr:.0f}")
        print(f"  ageLimit = {ageFrac:.4f} * sloTdr = {ageLimit:.0f}")
        print(f"  early (prefillOverloaded) = {earlyFrac:.4f} * sloTdr = {early:.0f}")
        print(f"  mean_tdr = {tdr:.0f}")
        print(f"  ageLimit / sloTdr = {ageFrac:.4f}")
        print(f"  ageLimit / mean_tdr = {ageLimit/tdr:.6f}")
        print(f"  mean_tdr / sloTdr = {tdr/sloTdr:.1f}x")

        # Is ageLimit a small fraction of TDR?
        # If ageLimit << mean_tdr, the cloud IS switching to P PROC relatively early
        # and the starvation is elsewhere
        if ageLimit / tdr < 0.01:
            print(f"  ==> ageLimit is {ageLimit/tdr*100:.2f}% of mean_tdr")
            print(f"  ==> Cloud switches to P PROC EARLY relative to total TDR")
            print(f"  ==> Cloud P PROC starvation is NOT the dominant interval")
        elif ageLimit / tdr < 0.1:
            print(f"  ==> ageLimit is {ageLimit/tdr*100:.2f}% of mean_tdr")
            print(f"  ==> Cloud P PROC wait is a MINOR contributor")
        else:
            print(f"  ==> ageLimit is {ageLimit/tdr*100:.2f}% of mean_tdr")
            print(f"  ==> Cloud P PROC wait is a SIGNIFICANT contributor")
    else:
        print(f"  sloTdr unknown (dist mixes TDR and TPOT)")
        print(f"  ageFrac = {ageFrac:.4f}")
    print()


print("=" * 80)
print("PART 2: But wait — does pproc[k] ever have anything in it?")
print("=" * 80)
print()
print("For a request to be in pproc[k], it must have completed:")
print("  1. admit -> P PRE (edge work)")
print("  2. P PRE -> UP transfer complete (XDN UP PRE)")
print("  3. P PROC partial layer completion (TDN P PROC with le < layers)")
print()
print("Line 488: pproc[k].push_back(id) happens on XDN UP PRE")
print("Line 446: pproc[k].push_back(id) happens on TDN P PROC with le < layers (chunking)")
print()
print("The P PROC decision at line 666 only triggers when BOTH pproc[k]")
print("and dproc[k] are non-empty. If pproc[k] is empty (because there")
print("are very few requests), the decision never fires at all!")
print()
print("For tests #15/#18 with tp ~ 9e-6:")
print("  There may be only 1-10 requests total in the entire simulation")
print("  At any given moment, pproc[k] likely has 0 or 1 items")
print("  dproc[k] may also have 0 items (these are sparse workloads)")
print()
print("IF pproc[k] and dproc[k] are rarely simultaneously non-empty,")
print("THEN the cloud ageLimit never matters — the cloud simply runs")
print("whatever is available.")
print()


print("=" * 80)
print("PART 3: What actually happens with a sparse workload?")
print("=" * 80)
print()
print("For tests 15/18 (tp ~ 9e-6, tpot=0, very few requests):")
print()
print("Scenario: A single request arrives.")
print("  1. ARR -> admit queue")
print("  2. Edge is free -> P PRE fires immediately (or quickly)")
print("     upTail = max(upTail, preDone) + transfer(lin)")
print("  3. Wait for UP transfer to complete (XDN UP PRE)")
print("     -> pproc[k].push_back(id)")
print("  4. Cloud k is free (no dproc, since this is a sparse workload)")
print("     -> Cloud runs P PROC for ALL layers (ls=0, le=layers)")
print("  5. P PROC completes (TDN P PROC)")
print("     -> le == layers -> WAIT_DOWN_PREFILL")
print("     -> downTail = max(downTail, now) + transfer(lin)")
print("  6. Wait for DOWN transfer to complete (XDN DOWN PRE)")
print("     -> ppost.push_back(id)")
print("  7. Edge is free -> P POST fires")
print("  8. P POST completes (TDN P POST)")
print("     -> First token is produced, TDR is complete")
print()
print("In this scenario:")
print("  - No decode competition (no D PRE, no D PROC)")
print("  - P PROC runs immediately without waiting")
print("  - The TDR is just the pipeline latency:")
print("    P PRE time + UP + P PROC time + DOWN + P POST time")
print()
print("But mean_tdr = 19-47 MILLION. This is absurdly large for a")
print("single request traversing an empty pipeline!")
print()
print("THEREFORE: the workload is NOT 'just 1 request'.")
print("There MUST be concurrent decode activity (many output tokens)")
print("from previously-arrived requests.")
print()
print("The timeline is more like:")
print("  - Request A arrives early, prefills, starts decoding")
print("  - Request A produces many tokens over millions of time units")
print("  - During Request A's decode, the edge/cloud/UP/DOWN are busy")
print("  - Request B arrives at some point during A's decode")
print("  - Request B must compete with A's decode for resources")
print()
print("This is the REAL starvation scenario!")


print()
print("=" * 80)
print("PART 4: Tracing Request B through a busy decode pipeline")
print("=" * 80)
print()
print("When Request B arrives while Request A is decoding:")
print()
print("EDGE is continuously cycling:")
print("  D POST -> D PRE -> D POST -> D PRE -> ...")
print("  (serving A's decode output tokens)")
print()
print("CLOUD k is continuously cycling:")
print("  D PROC (batch) -> D PROC (batch) -> ...")
print("  (serving A's decode batches)")
print()
print("UP FIFO is busy with decode uploads (D PRE -> UP)")
print("DOWN FIFO is busy with decode downloads (D PROC -> DOWN)")
print()
print("Request B's pipeline:")
print("  B.1: admit queue (edge busy with D POST/D PRE)")
print("  B.2: P PRE (edge must break from decode cycle to serve B)")
print("  B.3: UP transfer (UP FIFO may be busy with decode uploads)")
print("  B.4: P PROC (cloud must break from D PROC cycle to serve B)")
print("  B.5: DOWN transfer (DOWN FIFO may be busy with decode downloads)")
print("  B.6: P POST (edge must break from decode cycle again)")
print()
print("Every single stage competes with the decode pipeline!")
print("The question is: which competition causes the longest delay?")
print()
print("v47 tested: does the edge break quickly enough for B.2 (P PRE)?")
print("  Result: NO EFFECT on #9/#10/#15/#17/#18")
print("  -> The admit/P PRE path is NOT the bottleneck")
print("  -> Either B.3/B.4/B.5/B.6 dominates, or the request simply")
print("     goes through the pipeline without much delay (and the")
print("     delay is structural/latency-bound, not scheduler-bound)")
print()

print("=" * 80)
print("PART 5: Is the TDR even improvable?")
print("=" * 80)
print()
print("CRITICAL INSIGHT: If mean_tdr is 91x or 372x the SLO...")
print("and if the pipeline minimum latency (P PRE + UP + P PROC + DOWN + P POST)")
print("is itself much larger than the SLO...")
print("then no scheduling change can bring TDR below the SLO.")
print()
print("The waiting component (norm_c) saturates when TDR <= SLO.")
print("If TDR >= SLO always, then norm_c < 1 always.")
print("If the MINIMUM POSSIBLE TDR is X * SLO (X > 1),")
print("then the best achievable norm_c might already be the one we see!")
print()
print("This would explain why NO CHANGE to the scheduler moves #15/#18:")
print("  -> The TDR is structurally determined by pipeline latency")
print("  -> The scheduler cannot reduce it below the physical pipeline time")
print("  -> norm_c ≈ 0.50 is close to the theoretical maximum for that workload")
print()
print("Evidence:")
print("  #15: norm_c = 0.495 ≈ 0.50 (suggests TDR ≈ 2x the optimal)")
print("  #18: norm_c = 0.498 ≈ 0.50 (same)")
print("  #17: norm_c = 0.522 ≈ 0.50 (same)")
print("  #9:  norm_c = 0.724 (different - more room to improve)")
print("  #10: norm_c = 0.630 (some room to improve)")
print()
print("If #15/#17/#18 are at their structural floor (norm_c ≈ 0.50),")
print("then they cannot be improved by ANY scheduling change.")
print("Only #9 and #10 have realistic improvement potential.")
print()
print("For #9: gain_c = 0.95 * (1.0 - 0.724) * 1000 = 262")
print("For #10: gain_c = 0.85 * (1.0 - 0.630) * 1000 = 315")
print("Total realistic gain from waiting cluster: ~577 IF #9/#10 are improvable")
print("If #15/#17/#18 are at structural floor: +0 from them")

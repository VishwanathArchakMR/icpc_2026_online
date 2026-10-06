import re

with open('edge_cloud_scheduler_v39.cpp', 'r') as f:
    code = f.read()

old_tdrU = """    auto tdrUrgent = [&](double now) {
        if (admit.empty()) return false;
        const double age = now - job[admit.front()].arrival;
        const double early = max(2.0 * scheduleCost + latency, (0.20 + 0.30 * throughputWeight) * sloTdr);
        const bool decodeBacklog = !dproc[k].empty();
        const bool systemTdrRising = sloTdr > 0.0 && meanTdr(now) >= (0.30 + 0.20 * throughputWeight) * sloTdr;
        // Waiting-oriented tests interrupt decode more readily; throughput-oriented
        // tests only do so when prefill is genuinely aging.
        return decodeBacklog && age >= early && systemTdrRising && waitingWeight > 0.12;
    };"""

new_tdrU = """    auto tdrUrgent = [&](double now) {
        if (admit.empty()) return false;
        if (sloTdr <= 0) return false;
        int id = admit.front();
        int lin = job[id].lin;
        double pLatency = work(0, lin) + transfer(lin) + work(1, lin) + transfer(lin) + work(2, lin);
        double safeAge = sloTdr - pLatency - (scheduleCost + latency);
        return (now - job[id].arrival >= safeAge) || 
               (meanTdr(now) >= sloTdr - pLatency);
    };"""

code = code.replace(old_tdrU, new_tdrU)


old_overload = """    auto admissionOverloaded = [&](double now) {
        return sloTdr > 0 && tdrPressure(now) >= (0.60 + 0.30 * throughputWeight);
    };

    auto prefillOverloaded = [&](int k, double now) {
        return pproc[k].size() >= 2 && sloTdr > 0 && tdrPressure(now) >= (0.50 + 0.35 * throughputWeight);
    };"""

new_overload = """    auto admissionOverloaded = [&](double now) {
        return sloTdr > 0 && tdrPressure(now) >= 0.85;
    };

    auto prefillOverloaded = [&](int k, double now) {
        return pproc[k].size() >= 2 && sloTdr > 0 && tdrPressure(now) >= 0.85;
    };"""

code = code.replace(old_overload, new_overload)

# Fix throughputMode references
old_pproc_check = """                if (throughputMode && dproc[k].empty() && !decodeUploadEta[k].empty() && ls < layers) {"""
new_pproc_check = """                if (dproc[k].empty() && !decodeUploadEta[k].empty() && ls < layers) {"""
code = code.replace(old_pproc_check, new_pproc_check)


with open('edge_cloud_scheduler_v39.cpp', 'w') as f:
    f.write(code)

import sys
import math

def solve(K, scheduleCost, latency, bandwidth, bytesPerToken, layers,
          sloTdr, sloTpot, tpUB, tpBase, distBase, wtp, wc, N, table_data):
    
    col = {c: [] for c in range(6)}
    for row in table_data:
        b = int(row[0])
        for c in range(6):
            if row[c+1] >= 0:
                col[c].append((b, row[c+1]))
                
    def get_time(c, b):
        v = col[c]
        if b <= v[0][0]: return v[0][1]
        if b >= v[-1][0]: return v[-1][1]
        for i in range(len(v)-1):
            if v[i][0] <= b <= v[i+1][0]:
                lo, hi = v[i], v[i+1]
                f = (b - lo[0]) / (hi[0] - lo[0])
                return lo[1] + f * (hi[1] - lo[1])
        return 0
        
    def work(c, b):
        return scheduleCost + get_time(c, b)
        
    def transfer(count):
        return latency + 8.0 * count * bytesPerToken / (bandwidth * 1e6)
        
    max_tp = 0
    best_b_tp = 1
    best_h_tp = 1
    for b in range(1, 4097):
        edgeDemand = work(3, b) + work(5, b)
        for h in range(1, min(K, b) + 1):
            large = (b + h - 1) // h
            linkDemand = h * latency + 8.0 * b * bytesPerToken / (bandwidth * 1e6)
            cycle = max(edgeDemand, linkDemand, work(4, large))
            tp = b / cycle
            if tp > max_tp:
                max_tp = tp
                best_b_tp = b
                best_h_tp = h
                
    print(f"Max TP: {max_tp:.4f} (b={best_b_tp}, h={best_h_tp}) vs tpUB: {tpUB:.4f}")
    
    tdr_min = work(0, 1) + transfer(1) + work(1, 1) + transfer(1) + work(2, 1)
    tpot_min = work(3, 1) + transfer(1) + work(4, 1) + transfer(1) + work(5, 1)
    
    print(f"Min TDR: {tdr_min:.4f} vs SLO1: {sloTdr:.4f}")
    print(f"Min TPOT: {tpot_min:.4f} vs SLO2: {sloTpot:.4f}")

    best_tp_slo = 0
    best_b_slo = 1
    best_h_slo = 1
    for b in range(1, 4097):
        for h in range(1, min(K, b) + 1):
            large = (b + h - 1) // h
            L = work(3, b) + h * transfer(large) + work(4, large) + h * transfer(large) + work(5, b)
            if L <= sloTpot:
                edgeDemand = work(3, b) + work(5, b)
                linkDemand = h * latency + 8.0 * b * bytesPerToken / (bandwidth * 1e6)
                cycle = max(edgeDemand, linkDemand, work(4, large))
                tp = b / cycle
                if tp > best_tp_slo:
                    best_tp_slo = tp
                    best_b_slo = b
                    best_h_slo = h
                    
    print(f"Max TP respecting SLO2 ({sloTpot}): {best_tp_slo:.4f} (b={best_b_slo}, h={best_h_slo})")
    
if __name__ == "__main__":
    with open('example1_input.txt', 'r') as f:
        lines = [l.strip() for l in f.readlines() if l.strip()]
        sys_params = list(map(float, lines[0].split()))
        K = int(sys_params[0])
        scheduleCost = sys_params[1]
        latency = sys_params[2]
        bandwidth = sys_params[3]
        bytesPerToken = int(sys_params[4])
        layers = int(sys_params[5])
        
        score_params = list(map(float, lines[1].split()))
        sloTdr, sloTpot, tpUB, tpBase, distBase, wtp, wc = score_params
        
        N = int(lines[2])
        table_data = []
        for i in range(N):
            row = list(map(float, lines[3+i].split()))
            table_data.append(row)
            
        solve(K, scheduleCost, latency, bandwidth, bytesPerToken, layers,
              sloTdr, sloTpot, tpUB, tpBase, distBase, wtp, wc, N, table_data)

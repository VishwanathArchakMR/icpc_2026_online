def work(col, b): return 1.0 + {3: 2.0, 4: 16.0, 5: 4.0}[col] * b / b  # simplified
def transfer(b): return 8.0 * b / b
K = 2
ready = [1, 1]
cloudTail = [100.0, 0.0]
now = 0.0

def evaluate(take):
    take_b = sum(take)
    edge1 = now + 1.0 + 2.0
    up_time = edge1
    up1 = [0, 0]
    for k in range(K):
        if take[k]:
            up_time += 8.0
            up1[k] = up_time
    cloud1 = [0, 0]
    for k in range(K):
        if take[k]:
            cloud1[k] = max(cloudTail[k], up1[k]) + 1.0 + 16.0
        else:
            cloud1[k] = cloudTail[k]
    procDone1 = [(cloud1[k], k) for k in range(K) if take[k]]
    procDone1.sort()
    down_time = 0.0
    for t, k in procDone1:
        down_time = max(down_time, t) + 8.0
    
    rem = [ready[k] - take[k] for k in range(K)]
    rem_b = sum(rem)
    if rem_b == 0:
        post1 = max(edge1, down_time) + 1.0 + 4.0
        return post1, post1
    
    edge2 = edge1 + 1.0 + 2.0
    up_time2 = max(up_time, edge2)
    up2 = [0, 0]
    for k in range(K):
        if rem[k]:
            up_time2 += 8.0
            up2[k] = up_time2
    cloud2 = [0, 0]
    procDone2 = []
    for k in range(K):
        if rem[k]:
            cloud2[k] = max(cloud1[k], up2[k]) + 1.0 + 16.0
            procDone2.append((cloud2[k], k))
    procDone2.sort()
    down_time2 = down_time
    for t, k in procDone2:
        down_time2 = max(down_time2, t) + 8.0
    
    post1 = max(edge2, down_time) + 1.0 + 4.0
    post2 = max(post1, down_time2) + 1.0 + 4.0
    
    avg = (take_b * post1 + rem_b * post2) / 2
    return post2, post2 + 0.25 * avg

print("Bundled C0+C1:", evaluate([1, 1]))
print("Split C1 then C0:", evaluate([0, 1]))

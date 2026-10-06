// Edge-Cloud Collaborative Scheduling — reactive greedy scheduler
// -----------------------------------------------------------------------
// Design notes (read before tuning):
//
// This is a *reactive* scheduler: it never predicts timestamps, it only
// reacts to whatever a frame reports (ARR / TDN / XDN / FIN) and then
// issues at most one task per free resource. Correctness (never scoring
// a hard 0) is prioritized over squeezing every last point, per the
// problem's own advice ("first make a scheduler that finishes every
// request legally").
//
// State machine per request (mirrors the problem's lifecycle diagram):
//
//   ARRIVED -> (P PRE issued) -> WAIT_UP_PRE -> (XDN UP PRE) -> READY_PROC
//   -> (P PROC issued) -> WAIT_DOWN_PRE -> (XDN DOWN PRE) -> READY_POST
//   -> (P POST issued) -> READY_DECODE            [TDR stops here]
//   -> (member of D PRE) -> WAIT_UP_DEC -> (XDN UP DEC) -> READY_DPROC[remote]
//   -> (member of D PROC) -> WAIT_DOWN_DEC -> (XDN DOWN DEC) -> READY_DPOST
//   -> (member of D POST) -> token produced -> FIN or back to READY_DECODE
//
// We never track "in-flight" membership explicitly beyond removing a
// request from its ready-queue the moment we issue a task containing it;
// re-entry into a ready-queue only happens on the specific event that
// unlocks the next step, so a request can never be double-scheduled.
//
// Scheduling heuristics:
//  * Local machine (E) priority: D POST > D PRE > P POST > P PRE.
//    Finishing already-computed tokens (D POST) and unblocking decode
//    (D PRE) both push down TPOT/TDR; P POST/P PRE feed the pipeline
//    but are given lower priority so decode never starves behind new
//    prefill work.
//  * Remote machine (Ck) priority: D PROC > P PROC. Keeps existing
//    generations moving before starting new prefill compute, which
//    keeps TPOT low without ever blocking prefill indefinitely (a
//    remote only prefers D PROC when a decode batch is actually ready).
//  * Batching: every time a local/remote resource is free and a
//    ready-set for that step is non-empty, we take *all* currently
//    ready requests as one group. This amortizes the fixed schedule
//    cost S optimally at that instant (grouping never increases S
//    count, and duration grows sub-linearly per the piecewise table),
//    without ever delaying a request that could have gone earlier.
//  * Load balancing for P PRE: assign each newly-arriving request to
//    the remote with the fewest currently-unfinished requests, tying
//    on lowest index. Cheap, and keeps remotes evenly loaded without
//    needing to simulate future timings.
//  * Prefill chunking is intentionally NOT used (always one full
//    [0,num_layers) piece): this is explicitly offered as the safe
//    "first solution" in the statement, and skipping it removes an
//    entire class of possible protocol-violation bugs (gap/order
//    errors) in exchange for a small amount of prefill/decode
//    interleaving opportunity. The task-time table is still parsed in
//    full (as required) and is retained (see `TaskTimeTable`) so this
//    can be extended later without touching the I/O layer.
// -----------------------------------------------------------------------

#include <bits/stdc++.h>
using namespace std;

// ---------- Task-time table: piecewise-linear lookup per column ----------
struct TaskTimeTable {
    // Each column stores (batch_size, time) pairs for rows where that
    // column is not -1, sorted by batch_size.
    vector<pair<long long,double>> col[6]; // prefill_pre, prefill_proc, prefill_post,
                                            // decode_pre, decode_proc, decode_post

    void addRow(long long bs, array<double,6> v) {
        for (int c = 0; c < 6; c++)
            if (v[c] >= 0.0) col[c].push_back({bs, v[c]});
    }
    void finalize() {
        for (int c = 0; c < 6; c++) sort(col[c].begin(), col[c].end());
    }
    // Piecewise-linear lookup; clamps outside range. Not used for legality,
    // only kept available for future duration-aware scheduling decisions.
    double lookup(int c, long long bs) const {
        const auto &v = col[c];
        if (v.empty()) return 0.0;
        if (bs <= v.front().first) return v.front().second;
        if (bs >= v.back().first) return v.back().second;
        auto it = upper_bound(v.begin(), v.end(), make_pair(bs, 1e18));
        auto hi = *it;
        auto lo = *prev(it);
        if (hi.first == lo.first) return lo.second;
        double frac = double(bs - lo.first) / double(hi.first - lo.first);
        return lo.second + frac * (hi.second - lo.second);
    }
};

// ---------- Request bookkeeping ----------
enum Phase {
    P_NONE = 0,
    P_ARRIVED,
    P_PRE_RUNNING,
    P_WAIT_UP_PRE,
    P_READY_PROC,
    P_PROC_RUNNING,
    P_WAIT_DOWN_PRE,
    P_READY_POST,
    P_POST_RUNNING,
    P_READY_DECODE,     // ready for next D PRE (first time or after a D POST)
    P_WAIT_UP_DEC,
    P_READY_DPROC,      // sitting in ready_dproc[remote]
    P_WAIT_DOWN_DEC,
    P_READY_DPOST,
    P_FINISHED
};

struct Req {
    long long Lin = -1;
    int remote = -1;
    Phase phase = P_NONE;
};

int main() {
    ios::sync_with_stdio(false);
    cin.tie(nullptr);

    int K;
    double S_cost; // schedule cost (real-valued per spec), informational only
    double latency_in_ms, bandwidth_gbps;
    long long bytes_per_token;
    int num_layers;
    cin >> K >> S_cost >> latency_in_ms >> bandwidth_gbps >> bytes_per_token >> num_layers;

    double SLO1, SLO2, tp_UB, tp_base, dist_base, w_tp, w_c;
    cin >> SLO1 >> SLO2 >> tp_UB >> tp_base >> dist_base >> w_tp >> w_c;

    int N;
    cin >> N;
    TaskTimeTable table;
    for (int i = 0; i < N; i++) {
        long long bs;
        array<double,6> v;
        cin >> bs >> v[0] >> v[1] >> v[2] >> v[3] >> v[4] >> v[5];
        table.addRow(bs, v);
    }
    table.finalize();

    // ---------- Scheduler state ----------
    vector<Req> req;                 // indexed by rid, grows as ARR events come in
    vector<int> load(K, 0);          // # unfinished requests currently assigned to each remote

    bool local_free = true;
    vector<char> remote_free(K, 1);

    deque<int> arrived_queue;                 // rids waiting to issue P PRE
    vector<deque<int>> ready_proc_by_remote(K); // rids waiting to issue P PROC on remote k
    deque<int> ready_post_queue;              // rids waiting to issue P POST
    vector<int> ready_decode;                 // rids ready to join next D PRE group
    vector<vector<int>> ready_dproc(K);       // rids ready to join D PROC group on remote k
    vector<int> ready_dpost;                  // rids ready to join next D POST group

    auto ensureSize = [&](int rid) {
        if ((int)req.size() <= rid) req.resize(rid + 1);
    };

    auto chooseRemote = [&]() -> int {
        int best = 0;
        for (int k = 1; k < K; k++) if (load[k] < load[best]) best = k;
        return best;
    };

    string line1tok;
    while (cin >> line1tok) {
        if (line1tok == "END") break;
        double t = stod(line1tok);
        (void)t;

        int e;
        cin >> e;

        set<int> finThisFrame;
        vector<int> frameDpostIds; // rids whose D POST TDN arrived this frame

        for (int i = 0; i < e; i++) {
            string kind;
            cin >> kind;

            if (kind == "ARR") {
                int rid; long long lin;
                cin >> rid >> lin;
                ensureSize(rid);
                req[rid].Lin = lin;
                req[rid].phase = P_ARRIVED;
                arrived_queue.push_back(rid);
            }
            else if (kind == "TDN") {
                string server; cin >> server;
                string maj; cin >> maj;   // "P" or "D"
                string step; cin >> step; // "PRE"/"PROC"/"POST"

                bool isLocal = (server == "E");
                int remoteIdx = -1;
                if (!isLocal) remoteIdx = stoi(server.substr(1));

                if (maj == "P") {
                    if (step == "PRE") {
                        int remote, rid; double dur;
                        cin >> remote >> rid >> dur;
                        req[rid].remote = remote; // fixed now (matches our own bookkeeping already set at issue time)
                        req[rid].phase = P_WAIT_UP_PRE;
                        local_free = true;
                    } else if (step == "PROC") {
                        int ls, le, remote, rid; double dur;
                        cin >> ls >> le >> remote >> rid;
                        cin >> dur;
                        req[rid].phase = P_WAIT_DOWN_PRE;
                        remote_free[remoteIdx] = 1;
                    } else { // POST
                        int remote, rid; double dur;
                        cin >> remote >> rid >> dur;
                        req[rid].phase = P_READY_DECODE;
                        ready_decode.push_back(rid);
                        local_free = true;
                    }
                } else { // D
                    if (step == "PRE") {
                        int minus1, m; cin >> minus1 >> m;
                        vector<int> ids(m);
                        for (auto &x : ids) cin >> x;
                        double dur; cin >> dur;
                        for (int rid : ids) req[rid].phase = P_WAIT_UP_DEC;
                        local_free = true;
                    } else if (step == "PROC") {
                        int remote, m; cin >> remote >> m;
                        vector<int> ids(m);
                        for (auto &x : ids) cin >> x;
                        double dur; cin >> dur;
                        for (int rid : ids) req[rid].phase = P_WAIT_DOWN_DEC;
                        remote_free[remoteIdx] = 1;
                    } else { // POST
                        int minus1, m; cin >> minus1 >> m;
                        vector<int> ids(m);
                        for (auto &x : ids) cin >> x;
                        double dur; cin >> dur;
                        local_free = true;
                        // Defer the actual phase transition (token produced /
                        // finished vs. ready-for-next-iteration) until the
                        // whole frame has been read, since FIN for these rids
                        // may appear later in the same frame.
                        frameDpostIds.insert(frameDpostIds.end(), ids.begin(), ids.end());
                    }
                }
            }
            else if (kind == "XDN") {
                string dir; cin >> dir;      // UP / DOWN
                int remote; cin >> remote;
                long long size; cin >> size; (void)size;
                string sub; cin >> sub;      // PRE / DEC
                int m; cin >> m;
                vector<int> ids(m);
                for (auto &x : ids) cin >> x;

                if (sub == "PRE") {
                    // m == 1 always for input-stage transfers
                    int rid = ids[0];
                    if (dir == "UP") {
                        req[rid].phase = P_READY_PROC;
                        ready_proc_by_remote[req[rid].remote].push_back(rid);
                    } else {
                        req[rid].phase = P_READY_POST;
                        ready_post_queue.push_back(rid);
                    }
                } else { // DEC
                    if (dir == "UP") {
                        for (int rid : ids) {
                            req[rid].phase = P_READY_DPROC;
                            ready_dproc[remote].push_back(rid);
                        }
                    } else {
                        for (int rid : ids) {
                            req[rid].phase = P_READY_DPOST;
                            ready_dpost.push_back(rid);
                        }
                    }
                }
            }
            else if (kind == "FIN") {
                int rid; cin >> rid;
                finThisFrame.insert(rid);
            }
        }

        // Now that the full frame (including any FIN lines) has been read,
        // resolve the D POST completions that arrived this frame: each
        // member produces a token; if it also has a FIN this frame it is
        // done, otherwise it becomes ready to join the next D PRE group.
        for (int rid : frameDpostIds) {
            if (finThisFrame.count(rid)) {
                req[rid].phase = P_FINISHED;
                load[req[rid].remote]--;
            } else {
                req[rid].phase = P_READY_DECODE;
                ready_decode.push_back(rid);
            }
        }

        // ---- Decide assignments ----
        vector<string> outLines;

        // Local machine: D POST > D PRE > P POST > P PRE
        if (local_free) {
            if (!ready_dpost.empty()) {
                vector<int> ids;
                ids.swap(ready_dpost);
                ostringstream oss;
                oss << "E D POST -1 " << ids.size();
                for (int id : ids) oss << " " << id;
                outLines.push_back(oss.str());
                local_free = false;
                // NOTE: do not resolve request state here — issuing this task
                // does not complete it. State transitions for these ids happen
                // later, when their D POST TDN actually arrives (see
                // frameDpostIds handling above, in a future frame).
            } else if (!ready_decode.empty()) {
                vector<int> ids;
                ids.swap(ready_decode);
                ostringstream oss;
                oss << "E D PRE -1 " << ids.size();
                for (int id : ids) oss << " " << id;
                outLines.push_back(oss.str());
                local_free = false;
            } else if (!ready_post_queue.empty()) {
                int rid = ready_post_queue.front();
                ready_post_queue.pop_front();
                ostringstream oss;
                oss << "E P POST " << req[rid].remote << " " << rid;
                outLines.push_back(oss.str());
                local_free = false;
            } else if (!arrived_queue.empty()) {
                int rid = arrived_queue.front();
                arrived_queue.pop_front();
                int remote = chooseRemote();
                req[rid].remote = remote;
                load[remote]++;
                ostringstream oss;
                oss << "E P PRE " << remote << " " << rid;
                outLines.push_back(oss.str());
                local_free = false;
            }
        }

        // Remote machines: D PROC > P PROC
        for (int k = 0; k < K; k++) {
            if (!remote_free[k]) continue;
            if (!ready_dproc[k].empty()) {
                vector<int> ids;
                ids.swap(ready_dproc[k]);
                ostringstream oss;
                oss << "C" << k << " D PROC " << k << " " << ids.size();
                for (int id : ids) oss << " " << id;
                outLines.push_back(oss.str());
                remote_free[k] = 0;
            } else if (!ready_proc_by_remote[k].empty()) {
                int rid = ready_proc_by_remote[k].front();
                ready_proc_by_remote[k].pop_front();
                ostringstream oss;
                oss << "C" << k << " P PROC 0 " << num_layers << " " << k << " " << rid;
                outLines.push_back(oss.str());
                remote_free[k] = 0;
            }
        }

        cout << outLines.size() << "\n";
        for (auto &s : outLines) cout << s << "\n";
        cout.flush();
    }

    return 0;
}

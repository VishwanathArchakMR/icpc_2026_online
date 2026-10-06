# ICPC & Huawei Online Challenge: Edge–Cloud Collaborative Scheduler

[![Language: C++](https://img.shields.io/badge/Language-C%2B%2B17%20%2F%20C%2B%2B20-00599C?logo=c%2B%2B)](https://en.cppreference.com/)
[![Python: 3.10+](https://img.shields.io/badge/Analysis-Python%203.10%2B-3776AB?logo=python&logoColor=white)](https://python.org)
[![Challenge: ICPC / Huawei](https://img.shields.io/badge/Competition-ICPC%20%7C%20Huawei%20Online%20Challenge-red)](https://icpc.global)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

High-performance, low-latency, and throughput-maximizing distributed schedulers architected for the **ICPC & Huawei Online Challenge: Edge–Cloud Collaborative Scheduling**.

This repository contains the complete progression of algorithmic architectures, offline micro-oracles, simulation suites, and state-of-the-art C++ interactive schedulers designed to optimize large-scale distributed inference pipelines across an edge coordinator and $K$ cloud worker nodes.

---

## Table of Contents
1. [Problem Architecture & System Formulation](#problem-architecture--system-formulation)
2. [Dual Optimization Objectives & SLOs](#dual-optimization-objectives--slos)
3. [Key Scheduling Challenges & Bottlenecks](#key-scheduling-challenges--bottlenecks)
4. [Algorithmic Architecture & Evolution (v1 – v61)](#algorithmic-architecture--evolution-v1--v61)
5. [Core Architectural Innovations in v61](#core-architectural-innovations-in-v61)
6. [Analytical Tools & Micro-Oracles](#analytical-tools--micro-oracles)
7. [Repository Structure](#repository-structure)
8. [Build & Execution Guide](#build--execution-guide)

---

## Problem Architecture & System Formulation

The challenge models an interactive distributed pipeline where user requests arrive online at an **Edge Device** and must be collaboratively executed between the Edge and $K$ remote **Cloud Workers**.

```
                           ┌──────────────────────────┐
                           │   Incoming User Request  │
                           └─────────────┬────────────┘
                                         ▼
                                  [Edge Node]
                                    P PRE (Prep)
                                         │
                   ┌─────────────────────┴─────────────────────┐
                   │ (Uplink FIFO Queue - Bandwidth & Latency) │
                   ▼                                           ▼
          [Cloud Worker 1]                            [Cloud Worker K]
           P PROC (Prefill)                            P PROC (Prefill)
                   │                                           │
                   └─────────────────────┬─────────────────────┘
                   │ (Downlink FIFO Queue - Bandwidth & Latency)│
                   ▼
                                  [Edge Node]
                                   P POST (Finish Prefill)
                                         │
                             ┌───────────┴───────────┐
                             │   Autoregressive Loop │
                             │  (Repeat for Lout)    │
                             ▼                       │
                        [Edge Node]                  │
                          D PRE (Prepare Token)      │
                             │                       │
           ┌─────────────────┴─────────────────┐     │
           │ (Uplink FIFO Queue)               │     │
           ▼                                   ▼     │
  [Cloud Worker 1]                    [Cloud Worker K]
   D PROC (Decode)                     D PROC (Decode)
           │                                   │
           └─────────────────┬─────────────────┘     │
           │ (Downlink FIFO Queue)                   │
           ▼                                         │
      [Edge Node]                                    │
       D POST (Output Token Emitted) ────────────────┘
```

### 1. Two-Stage Execution Model
Every request consists of two distinct lifecycle phases:
- **Prefill Stage (1-time execution per request):**
  - `P PRE` (Edge): Initial setup and prompt token ingestion.
  - `P PROC` (Cloud Worker $k$): Heavy matrix computations and KV-cache generation.
  - `P POST` (Edge): Post-processing and finalizing prefill. Completion marks the **Time-to-Detect-Response (TDR)**.
- **Decode Stage (Repeated $L_{out}$ times per token):**
  - `D PRE` (Edge): Context preparation for the next autoregressive step.
  - `D PROC` (Cloud Worker $k$): Single-token generation step on the assigned remote worker.
  - `D POST` (Edge): Output token emission. The interval between consecutive tokens defines **Time Per Output Token (TPOT)**.

### 2. Physical Network & Hardware Constraints
- **Asymmetric Hardware Profiles:** Edge node has limited compute capacity; Cloud workers have high parallelism.
- **FIFO Communication Links:** Global Uplink (`UP`) and Downlink (`DOWN`) transfer queues with physical latency $L$ and bandwidth $B$ (MB/s). Transfer time for $M$ units:
  $$\text{Transfer Time}(M) = \text{latency} + \frac{8 \times M \times \text{bytesPerToken}}{\text{bandwidth} \times 10^6}$$
- **Non-Linear Batch Scaling (`TimeTable`):** Sub-linear execution time vs batch size $m$ represented as piecewise linear tables with setup overheads $S$.

---

## Dual Optimization Objectives & SLOs

The system evaluates solutions under an adaptive objective function parameterized by test-specific weights ($W_{tp}$ and $W_c = 1 - W_{tp}$):

$$\text{Final Score} = W_{tp} \cdot \text{NormTP} + W_c \cdot \text{NormC}$$

Where:
- **Throughput Score ($\text{NormTP}$):** Measures overall token completion rate relative to upper and baseline bounds.
- **Latency / Quality Score ($\text{NormC}$):** Measures penalty-adjusted SLA compliance based on:
  - **TDR (Time to Detect Response / Time to First Token):** Penalty incurred when $\text{TDR} > \text{SLO}_{TDR}$.
  - **TPOT (Time Per Output Token / Inter-token Latency):** Penalty incurred when $\text{TPOT} > \text{SLO}_{TPOT}$.

---

## Key Scheduling Challenges & Bottlenecks

1. **Edge vs Cloud Resource Contention:**
   - Single-threaded Edge processor shared among `P PRE`, `P POST`, `D PRE`, and `D POST`.
   - Balancing immediate token delivery (`D POST`) against pipeline feeding (`P PRE` / `D PRE`).
2. **Uplink / Downlink Wire Contention:**
   - Over-subscribing network transfers causes Head-of-Line (HoL) blocking in FIFO queues.
   - Remote worker starvation occurs if upload batches are delayed.
3. **Continuous Batching vs Latency Tradeoff:**
   - Larger batch sizes maximize Cloud compute efficiency ($m / \text{Time}(m)$).
   - Waiting to accumulate large batches delays individual requests, degrading TDR and TPOT.
4. **Decoupled Worker Asynchrony:**
   - Different cloud workers advance at varying speeds depending on batch allocations.

---

## Algorithmic Architecture & Evolution (v1 – v61)

The solutions evolved through 5 major architectural paradigms across 60+ iterations:

| Generation | Versions | Core Paradigm & Mechanism | Key Insights / Breakthroughs |
|---|---|---|---|
| **Phase 1: Baselines** | `scheduler_1.cpp` – `scheduler_7.cpp` | Greedy FIFO & Round-Robin balancing | Established baseline state tracking, synchronous dispatch, and event loops. |
| **Phase 2: Peak Preserving & Backlog Safety** | `v11` – `v24` | Peak preserving heuristics, conservative continuous batching, and pressure gating | Introduced dynamic thresholding for prefill vs decode prioritization to prevent cloud starvation. |
| **Phase 3: Multi-Target Pipelining** | `v25` – `v37` | Multi-target batch sizing, wave batching, and steady-state rate matching (`ssrate`) | Synchronized pipeline stages across edge and cloud to stabilize queue lengths. |
| **Phase 4: Global Throughput Engine** | `v38` – `v50` | Exact 2D precomputed batch lookup tables (`batchPick`, `dpreBatchPick`) | Eliminated runtime $O(M)$ batch searches; modeled wire link costs directly in batch optimality decisions. |
| **Phase 5: SOTA Adaptive Hybrid** | `v51` – `v61` | Dynamic age-escalation, asynchronous cloud regrouping, and starvation prevention | Smooth interpolation across $W_{tp} \in [0, 1]$, adaptive age thresholds, and stage dominance balancing. |

---

## Core Architectural Innovations in v61

The final flagship engine (`edge_cloud_scheduler_v61.cpp`) incorporates several advanced systems:

### 1. Precomputed Multi-Dimensional Rate Tables
To eliminate runtime batch evaluation overheads in the interactive loop:
```cpp
// Precomputes optimal batch sizes m for every column c and wire condition
constexpr int MAX_BATCH = 4096;
array<array<array<int, MAX_BATCH + 1>, 2>, 6> batchPick{};
array<array<int, MAX_BATCH + 1>, 65> dpreBatchPick{};
```
- `dpreBatchPick[h][m]` explicitly accounts for the $h$ distinct remote workers in the D-PRE group, computing exact uplink overhead:
  $$\text{Link Time}(h, m) = h \cdot \text{latency} + \frac{8 \cdot m \cdot \text{bytesPerToken}}{\text{bandwidth} \times 10^6}$$

### 2. Dynamic Age-Escalated Prefill Priority
Prevents long-tail prefill starvation in waiting-sensitive tests ($W_c > 0.5$):
$$\text{ageLimit} = \max\left(4S + 2\cdot\text{latency},\; (0.35 + 0.55\cdot W_{tp})\cdot \text{SLO}_{TDR}\right)$$
Cloud workers prioritize `P PROC` over `D PROC` when:
- Request age exceeds `ageLimit`.
- Local prefill pressure exceeds decode queue pressure.
- System detects an impending TDR slope escalation.

### 3. Asynchronous Remote Regrouping & Load Balancing
- Distributes incoming prefill requests across the least-loaded cloud workers based on pending active batch durations.
- Enables multi-job decode batching on remote workers while preserving strict FIFO ordering on the return downlink.

---

## Analytical Tools & Micro-Oracles

The repository includes a suite of analytical tools used to verify optimal substructure and diagnose performance:

- [`async_regroup_oracle.py`](file:///c:/Users/vishw/OneDrive/Desktop/icpc/async_regroup_oracle.py):
  Exact micro-oracle exploring all reachable states to determine mathematically optimal D-PROC regrouping policies.
- [`cloud_interleave_oracle.py`](file:///c:/Users/vishw/OneDrive/Desktop/icpc/cloud_interleave_oracle.py):
  Simulates multi-cloud execution interleaving to identify optimal synchronization points between workers.
- [`analyze_waiting_v2.py`](file:///c:/Users/vishw/OneDrive/Desktop/icpc/analyze_waiting_v2.py):
  Stage-by-stage latency breakdown analyzing TDR dominance across edge admission, uplink transfer, cloud prefill, downlink transfer, and edge post-processing.
- [`offline_bounds.py`](file:///c:/Users/vishw/OneDrive/Desktop/icpc/offline_bounds.py):
  Computes theoretical upper bounds for throughput and lower bounds for latency given task graphs.

---

## Repository Structure

```
.
├── edge_cloud_scheduler_v61.cpp    # Flagship interactive scheduler (Latest SOTA)
├── edge_cloud_scheduler_v38..v60   # Milestone implementations and ablations
├── edge_cloud_v1..v40.cpp          # Architectural evolution across optimization phases
├── scheduler_1..7.cpp              # Baseline prototypes and greedy dispatchers
│
├── analyze_waiting.py              # Initial bottleneck diagnostic script
├── analyze_waiting_v2.py           # Comprehensive TDR pipeline stage breakdown
├── async_regroup_oracle.py         # Asynchronous cloud regrouping micro-oracle
├── cloud_interleave_oracle.py      # Cloud worker interleaving simulation
├── offline_bounds.py               # Theoretical bounds calculator
├── test_rollout.py                 # Rollout & trajectory validation suite
│
├── problem.pdf / problem.docx      # Official challenge problem specifications
├── problem.txt                     # Text specification of protocol and rules
├── example1_input.txt              # Sample input test harness
└── .gitignore                      # Git configuration
```

---

## Build & Execution Guide

### Prerequisites
- C++ Compiler: GCC 11+, Clang 13+, or MSVC 2022+ with C++17 or C++20 support.
- Python 3.10+ (for analytical oracles and validation scripts).

### Compiling the Scheduler
```bash
# Compile flagship v61 scheduler with optimization flags
g++ -O3 -std=c++20 -Wall -Wextra edge_cloud_scheduler_v61.cpp -o scheduler_v61
```

### Running with the Interactive Test Harness
```bash
# Test against sample input
./scheduler_v61 < example1_input.txt
```

### Running Micro-Oracles
```bash
python async_regroup_oracle.py
python analyze_waiting_v2.py
```

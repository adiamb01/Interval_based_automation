# CMN / HNS / CCG / DMC Interval Collector

Baseline script:

```bash
Interval_Based_CPU_CMN_DMC_automation.py
```

This wrapper runs a benchmark multiple times and collects interval PMU data for CPU bandwidth, HNS local/remote traffic, SLC behavior, CCG opcode bandwidth, CBusy distribution, POCQ pressure, HNS congestion, and DMC Phoenix FE/BE telemetry.

---

## 1. Requirements

### Python packages

Recommended install:

```bash
python3 -m pip install --upgrade numpy pandas matplotlib numexpr bottleneck
```

If the system Python environment has conflicting packages, use a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install numpy pandas matplotlib numexpr bottleneck
```

### Useful system packages

Depending on the distro:

```bash
apt-get update
apt-get install -y linux-tools-common linux-tools-generic numactl python3-pip python3-venv
```

`bw_mem` usually comes from lmbench or your local benchmark build.

---


## 2. Full Sweep Command

For `bw_mem` read validation:

```bash
python3 Interval_Based_CPU_CMN_DMC_automation.py \
  --cmd "taskset -c 0-127 bw_mem -P 128 -N 1 -W 2 512m rd" \
  --dmc-phx-all \
  --ccg-cbusy \
  --ccg-pocq \
  --ccg-hns-txdat-stall \
  --ccg-hns-txrsp-stall \
  --ccg-hns-throttle-read \
  --ccg-hns-throttle-write
```

---

## 3. Metrics Collected

| Category | Metrics |
|---|---|
| CPU | Read BW, Write BW, Total BW |
| HNS | Local BW, Remote BW, Total BW, Local %, Remote % |
| CPU/HNS | HNS/CPU bandwidth ratio |
| SLC | Accesses, Misses, Miss/Access % |
| CCG Reads | READ_A BW, READ_B BW |
| CCG Writes | WRITEUNIQUEFULL, WRITEBACKFULL, WRITEEVICTORVICT, WRITENOSNOOPFULL, WRITEDATAFULL |
| CCG Total | Total classified CCG bandwidth |
| CBusy | cbusy00 %, cbusy01 %, cbusy10 %, cbusy11 % |
| POCQ | class0 occupancy %, class0 retry % |
| HNS Congestion | txdat stall %, txrsp stall %, throttle read %, throttle write % |
| DMC FE | retry %, timeout/credit % |
| DMC BE | controller busy %, port busy %, CMDQ almost full %, queue alloc/dealloc |
| Quality | mux rows, not counted |

---

## 4. Collection Phases

The wrapper runs the benchmark multiple times. Each pass keeps the number of events low enough to avoid perf multiplexing.

### Phase 1: CPU + Local HNS + DMC Phoenix

Collected in the first benchmark execution.

CPU:
- CPU read bandwidth
- CPU write bandwidth
- CPU total bandwidth

Local HNS:
- Local HNS memory requests
- Local HNS memory retries
- Local HNS bandwidth

DMC Phoenix is collected with independent `arm_cspmu_mc_*` PMUs, so these do not consume CMN event slots.

FE events:

| Metric | Event code |
|---|---|
| CHI cycles | `0x10` |
| CHI retry | `0x50` |
| CHI timeout/credit | `0x60` |

BE events:

| Metric | Event code |
|---|---|
| DMC cycles | `0x110` |
| Controller busy | `0x90` |
| Port busy | `0x160` |
| CMDQ almost full | `0x80` |
| Queue alloc/dealloc | `0xD0` |

### Phase 2: Remote HNS

Collected in a separate benchmark execution.

- Remote HNS requests
- Remote HNS retries
- Remote HNS bandwidth

### Phase 3: SLC

Collected in a separate benchmark execution.

- SLC accesses
- SLC misses
- Miss/access %

### Phase 4: CCG Read Opcode Sweep + CBusy

The wrapper tracks two read opcode groups.

| Opcode | Value | Purpose |
|---|---:|---|
| READ_A | `0x800013000` | Primary read traffic |
| READ_B | `0x800003800` | Secondary read traffic |

Node coverage is split into two combined node groups:

```text
00_80 + 280_300
100_180 + 380_400
```

This covers:

```text
0x0
0x80
0x100
0x180
0x280
0x300
0x380
0x400
```

CBusy events are attached to the read passes:

| CBusy event | Meaning |
|---|---|
| `hns_cbusy00_all` | cbusy00 level |
| `hns_cbusy01_all` | cbusy01 level |
| `hns_cbusy10_all` | cbusy10 level |
| `hns_cbusy11_all` | cbusy11 level |

CBusy is reported as a distribution. The four percentages should sum to approximately 100%.

### Phase 5: CCG Write Opcode Sweep + POCQ

Write opcode coverage:

| Opcode | Purpose |
|---|---|
| WRITEUNIQUEFULL_AR1 | Write allocation traffic |
| WRITEBACKFULL_AR1 | Writeback traffic |
| WRITEEVICTORVICT_AR1 | Eviction traffic |
| WRITENOSNOOPFULL_AR1 | Non-snoop write traffic |
| WRITEDATAFULL_AR1 | Write data traffic |

POCQ events:

| Event | Meaning |
|---|---|
| `hns_pocq_class_occup_class0` | POCQ class0 occupancy |
| `hns_pocq_class_retry_class0` | POCQ class0 retry pressure |

POCQ is normalized against:

```text
2 GHz CMN clock
2 CMNs
```

### Phase 6: HNS Congestion

Collected with dedicated CCG write passes.

| Event | Meaning |
|---|---|
| `hns_txdat_stall` | TXDAT stall cycles |
| `hns_txrsp_stall` | TXRSP stall cycles |
| `hns_sn_throttle_read` | SN throttle read pressure |
| `hns_sn_throttle_write` | SN throttle write pressure |

These are also normalized against:

```text
2 GHz CMN clock
2 CMNs
```

---

## 6. Output Directory

Each run creates a timestamped output directory:

```text
cpu_hns_ccg_interval_YYYYMMDD_HHMMSS/
```

Files:

| File | Description |
|---|---|
| `summary.csv` | Aggregated benchmark-window metrics |
| `timeseries.csv` | Per-interval time-series data |
| `bandwidth_timeseries.png` | Main plot |
| `mux_check.csv` | Rows where perf running time indicates multiplexing |
| `not_counted.csv` | Events reported as not counted or unsupported |
| `cpu_local.perf.csv` | Raw perf output for CPU/local HNS/DMC pass |
| `remote.perf.csv` | Raw perf output for remote HNS pass |
| `slc.perf.csv` | Raw perf output for SLC pass |
| `ccg_cbusy/` | Raw CCG read and CBusy logs |
| `ccg_pocq/` | Raw CCG write and POCQ logs |
| `ccg_hns_congestion/` | Raw CCG/HNS congestion logs |

---

## 7. How to Read the Summary

### CPU

`CPU total BW` is CPU-side memory demand.

### HNS

`HNS total BW` is memory-controller-visible traffic through HNS/SNF paths.

### CPU/HNS

```text
HNS/CPU BW = HNS total / CPU total * 100
```

Near 100% means most CPU traffic is reaching memory.

### SLC

`miss/access` shows how much SLC traffic missed. High miss/access means SLC is not absorbing much traffic.

### CCG dedicated

For `bw_mem rd`, READ_A should dominate and CCG total should be close to HNS total.

### CBusy

CBusy is the HNS CBusy distribution. The four values should sum to roughly 100%.

### DMC Phoenix

Example:

```text
DMC Phoenix
  FE
    retry       : 3.496% avg / 3.530% max
    timeout/cr  : 0.000% avg / 0.000% max
  BE
    ctrl busy   : 99.996% avg / 100.002% max
    port busy   : 31.049% avg / 31.052% max
    cmdq almost : 12.280% avg / 12.383% max
    q alloc/dea : 11052964307 / interval
```

Interpretation:

| Metric | Meaning |
|---|---|
| FE retry % | CHI request retry pressure |
| FE timeout/credit % | CHI credit starvation |
| BE controller busy % | DMC controller active cycles |
| BE port busy % | DRAM port/datapath utilization |
| BE CMDQ almost full % | Backend command queue pressure |
| BE queue alloc/dealloc | Queue activity level |

### HNS congestion

High values indicate stalls or throttling in HNS paths.

### POCQ

Higher occupancy or retry means higher HNS POCQ pressure.

---

## 8. How to Read `summary.csv`

Inspect selected metrics:

```bash
grep -E "cpu_total|hns_total|ccg_|dmc_phx|cbusy|pocq|throttle|stall" summary.csv
```

DMC only:

```bash
grep dmc_phx summary.csv
```

CCG only:

```bash
grep ccg_ summary.csv
```

Quality only:

```bash
grep -E "mux|not_counted" summary.csv
```

---

## 9. How to Read `timeseries.csv`

`timeseries.csv` contains one row per interval.

Useful columns include:

```text
time_s
cpu_read_GBps
cpu_write_GBps
cpu_total_GBps
hns_local_GBps
hns_remote_GBps
hns_total_GBps
ccg_bw_GBps
dmc_phx_fe_retry_pct
dmc_phx_be_controller_busy_pct
dmc_phx_be_port_busy_pct
dmc_phx_be_cmdq_almost_full_pct
pocq_class0_occup_pct
pocq_class0_retry_pct
```

Quick inspection:

```bash
head -1 timeseries.csv | tr ',' '\n' | grep -E "cpu|hns|ccg|dmc|pocq|cbusy|stall|throttle"
```

---

## 10. How to Read the Plot

Plot file:

```text
bandwidth_timeseries.png
```

Use it to check:

- benchmark start/end window
- CPU vs HNS bandwidth agreement
- CCG classified bandwidth
- CBusy distribution
- POCQ occupancy/retry
- HNS stall/throttle behavior
- DMC FE/BE pressure

Expected for `bw_mem rd`:

```text
CPU total approximately HNS total approximately CCG total
READ_A dominates
READ_B near zero
DMC controller busy high
DMC port busy lower than controller busy
```

---

## 11. Quality Checks

Expected:

```text
Quality
  mux rows    : 0
  not counted : 0
```

`mux rows > 0` means perf multiplexing occurred.

`not counted > 0` means some event was unsupported or failed to count.

Inspect:

```bash
cat mux_check.csv
cat not_counted.csv
```

---

## 12. Reparse Existing Runs

If only parser/summary/plot logic changed, reparse without rerunning the benchmark:

```bash
python3 Interval_Based_CPU_CMN_DMC_automation.py \
  --parse-only cpu_hns_ccg_interval_YYYYMMDD_HHMMSS \
  --dmc-phx-all \
  --ccg-cbusy \
  --ccg-pocq \
  --ccg-hns-txdat-stall \
  --ccg-hns-txrsp-stall \
  --ccg-hns-throttle-read \
  --ccg-hns-throttle-write
```

---

## 13. SPEC Usage

Example SPEC command:

```bash
python3 Interval_Based_CPU_CMN_DMC_automation.py \
  --spec-root /mnt/spec/bench_exchange \
  --outdir spec_500.perlbench_r_$(date +%Y%m%d_%H%M%S) \
  --cmd "cd /mnt/spec/bench_exchange && ulimit -s unlimited && . ./shrc && numactl --physcpubind=0-127 runcpu --verbose 6 --action run --nobuild --config userconfig.cfg --loose --define jemalloc-thp --iterations 1 --size ref --tune base --copies 128 500.perlbench_r" \
  --dmc-phx-all \
  --ccg-cbusy \
  --ccg-pocq \
  --ccg-hns-txdat-stall \
  --ccg-hns-txrsp-stall \
  --ccg-hns-throttle-read \
  --ccg-hns-throttle-write
```

SPEC summary includes benchmark ratio and spread.

---

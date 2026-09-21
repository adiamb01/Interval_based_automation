#!/usr/bin/env python3
"""
Interval_Based_CPU_CMN_DMC_automation.py

CPU + HNS + SLC hit/miss + optional CCG interval collector.

Key modes:
  Default:
    CPU/local HNS pass
    remote HNS pass
    SLC hit/miss is combined into the first CCG pass when CCG is enabled
    CCG READ_AR1_A only

  CCG grouped modes:
    --ccg-set read
      READ_AR1_A + READ_AR1_B, aggregated as one CCG BW

    --ccg-set write
      WRITEUNIQUEFULL_AR1 + WRITEEVICTORVICT_AR1 + WRITEBACKFULL_AR1
      + WRITENOSNOOPFULL_AR1 + WRITEDATAFULL_AR1, aggregated as one CCG BW

    --ccg-set readwrite
      all read + write opcodes, aggregated as one CCG BW

    --ccg-set all
      same as readwrite

    --ccg-set single --opcode READ_AR1_A
      only the selected opcode

  Parse-only:
    --parse-only <existing_run_dir>
      Reparse existing perf logs and regenerate timeseries.csv, summary.csv,
      mux_check.csv, not_counted.csv, and plot if dependencies are installed.

Notes:
  The script aggregates read/write opcode results together in the final CCG BW.
  CPU/HNS/CCG passes are aligned by interval index on the x-axis, not raw perf timestamp.
  It does not force multiple different CCG opcodes into one perf event group,
  because that can reintroduce DTM/watchpoint contention. It runs clean node/opcode
  passes and combines the parsed results offline.

CCG defaults:
  CCG node groups:
    0x0,0x80
    0x100,0x180
    0x280,0x300
    0x380,0x400

  CCG PMU:
    arm_cmn_0 only by default, to avoid double counting.
    Use --ccg-both-cmns only for debug.

Output:
  pass_scores.csv for SPEC ratio per pass
  timeseries.csv
  Plot/summary hide zero-only sections, e.g. CCG when --ccg-set none
  summary.csv
  bandwidth_timeseries.png, if matplotlib is available
  mux_check.csv
  not_counted.csv
  ccg/<opcode>/<group>.perf.csv
"""

import argparse
import csv
import re
import subprocess
from collections import defaultdict
from datetime import datetime
from pathlib import Path

BYTES_PER_FLIT = 64
BYTES_PER_GB = 1_000_000_000
MASK = "0xfffffff7fffc07ff"

CPU_EVENTS = [
    ("cpu_read_0x60", "armv8_pmuv3_0/event=0x60/"),
    ("cpu_write_0x61", "armv8_pmuv3_0/event=0x61/"),
    ("l2d_cache_refill", "armv8_pmuv3_0/l2d_cache_refill/"),
    ("l2d_cache", "armv8_pmuv3_0/l2d_cache/"),
    ("instructions", "armv8_pmuv3_0/instructions/"),
    ("cpu_cycles", "armv8_pmuv3_0/cpu_cycles/"),
]

LOCAL_HNS_EVENTS = [
    ("cmn0_local_req", "arm_cmn_0/hns_mc_reqs_local_sn/"),
    ("cmn0_local_retry", "arm_cmn_0/hns_mc_retries_local_sn/"),
    ("cmn1_local_req", "arm_cmn_1/hns_mc_reqs_local_sn/"),
    ("cmn1_local_retry", "arm_cmn_1/hns_mc_retries_local_sn/"),
]

REMOTE_HNS_EVENTS = [
    ("cmn0_remote_req", "arm_cmn_0/hns_mc_reqs_remote_sn/"),
    ("cmn0_remote_retry", "arm_cmn_0/hns_mc_retries_remote_sn/"),
    ("cmn1_remote_req", "arm_cmn_1/hns_mc_reqs_remote_sn/"),
    ("cmn1_remote_retry", "arm_cmn_1/hns_mc_retries_remote_sn/"),
]


SLC_EVENTS = [
    ("cmn0_slc_access", "arm_cmn_0/hns_slc_sf_cache_access_all/"),
    ("cmn0_slc_miss", "arm_cmn_0/hns_cache_miss_all/"),
    ("cmn1_slc_access", "arm_cmn_1/hns_slc_sf_cache_access_all/"),
    ("cmn1_slc_miss", "arm_cmn_1/hns_cache_miss_all/"),
]


CBUSY_EVENTS = [
    ("cmn0_cbusy00", "arm_cmn_0/hns_cbusy00_all/"),
    ("cmn0_cbusy01", "arm_cmn_0/hns_cbusy01_all/"),
    ("cmn1_cbusy00", "arm_cmn_1/hns_cbusy00_all/"),
    ("cmn1_cbusy01", "arm_cmn_1/hns_cbusy01_all/"),
    ("cmn0_cbusy10", "arm_cmn_0/hns_cbusy10_all/"),
    ("cmn0_cbusy11", "arm_cmn_0/hns_cbusy11_all/"),
    ("cmn1_cbusy10", "arm_cmn_1/hns_cbusy10_all/"),
    ("cmn1_cbusy11", "arm_cmn_1/hns_cbusy11_all/"),
]


POCQ_EVENTS = [
    ("cmn0_pocq_occup_class0", "arm_cmn_0/hns_pocq_class_occup_class0/"),
    ("cmn0_pocq_retry_class0", "arm_cmn_0/hns_pocq_class_retry_class0/"),
    ("cmn1_pocq_occup_class0", "arm_cmn_1/hns_pocq_class_occup_class0/"),
    ("cmn1_pocq_retry_class0", "arm_cmn_1/hns_pocq_class_retry_class0/"),
]

CMN_CLOCK_HZ = 2_000_000_000.0

READ_OPCODES = [
    ("READ_AR1_A", "0x800013000"),
    ("READ_AR1_B", "0x800003800"),
]

WRITE_OPCODES = [
    ("WRITEUNIQUEFULL_AR1", "0x80000a800"),
    ("WRITEEVICTORVICT_AR1", "0x800021000"),
    ("WRITEBACKFULL_AR1", "0x80000d800"),
    ("WRITENOSNOOPFULL_AR1", "0x80000b800"),
    ("WRITEDATAFULL_AR1", "0x80000c800"),
]

CCG_OPCODES = READ_OPCODES + WRITE_OPCODES

CCG_NODE_GROUPS = [
    ("ccg_00_80", ["0x0", "0x80"]),
    ("ccg_100_180", ["0x100", "0x180"]),
    ("ccg_280_300", ["0x280", "0x300"]),
    ("ccg_380_400", ["0x380", "0x400"]),
]

FIELDS = [
    "time_s",
    "cpu_read_0x60",
    "cpu_write_0x61",
    "cpu_read_bw_GBps",
    "cpu_write_bw_GBps",
    "cpu_total_bw_GBps",
    "l2d_cache_refill",
    "l2d_cache",
    "l2_miss_pct",
    "instructions",
    "cpu_cycles",
    "ipc",
    "cmn0_local_req",
    "cmn0_local_retry",
    "cmn1_local_req",
    "cmn1_local_retry",
    "cmn0_remote_req",
    "cmn0_remote_retry",
    "cmn1_remote_req",
    "cmn1_remote_retry",
    "local_req_total",
    "remote_req_total",
    "local_retry_total",
    "remote_retry_total",
    "slc_access_total",
    "slc_miss_total",
    "slc_miss_access_pct",
    "local_pct",
    "remote_pct",
    "local_snf_bw_GBps",
    "remote_snf_bw_GBps",
    "total_snf_bw_GBps",
    "ccg_up_count",
    "ccg_down_count",
    "ccg_up_down_count",
    "cbusy00_total",
    "cbusy01_total",
    "cbusy10_total",
    "cbusy11_total",
    "cbusy_total",
    "cbusy00_pct",
    "cbusy01_pct",
    "cbusy10_pct",
    "cbusy11_pct",
    "pocq_occup_class0_total",
    "pocq_retry_class0_total",
    "pocq_occup_class0_pct",
    "pocq_retry_class0_pct",
    "hns_txdat_stall_total",
    "hns_txrsp_stall_total",
    "hns_sn_throttle_read_total",
    "hns_sn_throttle_write_total",
    "hns_txdat_stall_pct",
    "hns_txrsp_stall_pct",
    "hns_sn_throttle_read_pct",
    "hns_sn_throttle_write_pct",
    "ccg_up_bw_GBps",
    "ccg_down_bw_GBps",
    "ccg_up_down_bw_GBps",
    "ccg_read_a_bw_GBps",
    "ccg_read_b_bw_GBps",
    "ccg_writeunique_bw_GBps",
    "ccg_writeback_bw_GBps",
    "ccg_writeevictorvict_bw_GBps",
    "ccg_writenosnoop_bw_GBps",
    "ccg_writedata_bw_GBps",
    "ccg_dedicated_total_bw_GBps",
    "bw_mem_GBps",
    "dmc_cpu_local_0",
    "dmc_cpu_local_1",
    "dmc_cpu_local_2",
    "dmc_cpu_local_3",
    "dmc_cpu_local_4",
    "dmc_cpu_local_5",
    "dmc_cpu_local_6",
    "dmc_cpu_local_7",
    "dmc_cpu_local_8",
    "dmc_cpu_local_9",
    "dmc_cpu_local_10",
    "dmc_cpu_local_11",
    "dmc_cpu_local_12",
    "dmc_cpu_local_13",
    "dmc_cpu_local_14",
    "dmc_cpu_local_15",
    "dmc_remote_0",
    "dmc_remote_1",
    "dmc_remote_2",
    "dmc_remote_3",
    "dmc_remote_4",
    "dmc_remote_5",
    "dmc_remote_6",
    "dmc_remote_7",
    "dmc_remote_8",
    "dmc_remote_9",
    "dmc_remote_10",
    "dmc_remote_11",
    "dmc_remote_12",
    "dmc_remote_13",
    "dmc_remote_14",
    "dmc_remote_15",
    "dmc_slc_0",
    "dmc_slc_1",
    "dmc_slc_2",
    "dmc_slc_3",
    "dmc_slc_4",
    "dmc_slc_5",
    "dmc_slc_6",
    "dmc_slc_7",
    "dmc_slc_8",
    "dmc_slc_9",
    "dmc_slc_10",
    "dmc_slc_11",
    "dmc_slc_12",
    "dmc_slc_13",
    "dmc_slc_14",
    "dmc_slc_15",
    "dmc_cpu_local_16",
    "dmc_cpu_local_17",
    "dmc_cpu_local_18",
    "dmc_cpu_local_19",
    "dmc_cpu_local_20",
    "dmc_cpu_local_21",
    "dmc_cpu_local_22",
    "dmc_cpu_local_23",
    "dmc_cpu_local_24",
    "dmc_cpu_local_25",
    "dmc_cpu_local_26",
    "dmc_cpu_local_27",
    "dmc_cpu_local_28",
    "dmc_cpu_local_29",
    "dmc_cpu_local_30",
    "dmc_cpu_local_31",
    "dmc_cpu_local_32",
    "dmc_cpu_local_33",
    "dmc_cpu_local_34",
    "dmc_cpu_local_35",
    "dmc_cpu_local_36",
    "dmc_cpu_local_37",
    "dmc_cpu_local_38",
    "dmc_cpu_local_39",
    "dmc_cpu_local_40",
    "dmc_cpu_local_41",
    "dmc_cpu_local_42",
    "dmc_cpu_local_43",
    "dmc_cpu_local_44",
    "dmc_cpu_local_45",
    "dmc_cpu_local_46",
    "dmc_cpu_local_47",
    "dmc_cpu_local_48",
    "dmc_cpu_local_49",
    "dmc_cpu_local_50",
    "dmc_cpu_local_51",
    "dmc_cpu_local_52",
    "dmc_cpu_local_53",
    "dmc_cpu_local_54",
    "dmc_cpu_local_55",
    "dmc_cpu_local_56",
    "dmc_cpu_local_57",
    "dmc_cpu_local_58",
    "dmc_cpu_local_59",
    "dmc_cpu_local_60",
    "dmc_cpu_local_61",
    "dmc_cpu_local_62",
    "dmc_cpu_local_63",
    "dmc_remote_16",
    "dmc_remote_17",
    "dmc_remote_18",
    "dmc_remote_19",
    "dmc_remote_20",
    "dmc_remote_21",
    "dmc_remote_22",
    "dmc_remote_23",
    "dmc_remote_24",
    "dmc_remote_25",
    "dmc_remote_26",
    "dmc_remote_27",
    "dmc_remote_28",
    "dmc_remote_29",
    "dmc_remote_30",
    "dmc_remote_31",
    "dmc_remote_32",
    "dmc_remote_33",
    "dmc_remote_34",
    "dmc_remote_35",
    "dmc_remote_36",
    "dmc_remote_37",
    "dmc_remote_38",
    "dmc_remote_39",
    "dmc_remote_40",
    "dmc_remote_41",
    "dmc_remote_42",
    "dmc_remote_43",
    "dmc_remote_44",
    "dmc_remote_45",
    "dmc_remote_46",
    "dmc_remote_47",
    "dmc_remote_48",
    "dmc_remote_49",
    "dmc_remote_50",
    "dmc_remote_51",
    "dmc_remote_52",
    "dmc_remote_53",
    "dmc_remote_54",
    "dmc_remote_55",
    "dmc_remote_56",
    "dmc_remote_57",
    "dmc_remote_58",
    "dmc_remote_59",
    "dmc_remote_60",
    "dmc_remote_61",
    "dmc_remote_62",
    "dmc_remote_63",
    "dmc_slc_16",
    "dmc_slc_17",
    "dmc_slc_18",
    "dmc_slc_19",
    "dmc_slc_20",
    "dmc_slc_21",
    "dmc_slc_22",
    "dmc_slc_23",
    "dmc_slc_24",
    "dmc_slc_25",
    "dmc_slc_26",
    "dmc_slc_27",
    "dmc_slc_28",
    "dmc_slc_29",
    "dmc_slc_30",
    "dmc_slc_31",
    "dmc_slc_32",
    "dmc_slc_33",
    "dmc_slc_34",
    "dmc_slc_35",
    "dmc_slc_36",
    "dmc_slc_37",
    "dmc_slc_38",
    "dmc_slc_39",
    "dmc_slc_40",
    "dmc_slc_41",
    "dmc_slc_42",
    "dmc_slc_43",
    "dmc_slc_44",
    "dmc_slc_45",
    "dmc_slc_46",
    "dmc_slc_47",
    "dmc_slc_48",
    "dmc_slc_49",
    "dmc_slc_50",
    "dmc_slc_51",
    "dmc_slc_52",
    "dmc_slc_53",
    "dmc_slc_54",
    "dmc_slc_55",
    "dmc_slc_56",
    "dmc_slc_57",
    "dmc_slc_58",
    "dmc_slc_59",
    "dmc_slc_60",
    "dmc_slc_61",
    "dmc_slc_62",
    "dmc_slc_63",
    "dmc_chi_reqif_transfer",
    "dmc_chi_req_xmit_rd_retries",
    "dmc_chi_req_xmit_wr_retries",
    "dmc_chi_reqif_op_writenosnpfull",
    "dmc_chi_reqif_op_writenosnpfull_ptl_pcmosep",
    "dmc_chi_reqif_op_writenosnpptl",
    "dmc_chi_reqif_op_writezero",
    "dmc_chi_reqif_op_readnosnpsep",
    "dmc_chi_reqif_op_readnosnp",
    "dmc_chi_reqif_rd_ops_total",
    "dmc_chi_reqif_wr_ops_total",
    "dmc_chi_reqif_rdwr_ops_total",
    "dmc_chi_rd_retry_pct",
    "dmc_chi_wr_retry_pct",
    "dmc_chi_retry_pct",
    "dmc_chi_rd_pct",
    "dmc_chi_wr_pct",
    "dmc_chi_rd_without_retry_pct",
    "dmc_chi_wr_without_retry_pct",
    "dmc_chi_rdwr_without_retry_pct",
    "dmc_phx_cpu_fe_cycles",
    "dmc_phx_cpu_fe_retry",
    "dmc_phx_remote_fe_cycles",
    "dmc_phx_remote_fe_timeout_credit",
    "dmc_phx_be_buffer_full",
    "dmc_phx_be_queue_alloc_dealloc",
    "dmc_phx_be_cycles",
    "dmc_phx_be_cmdq_almost_full",
    "dmc_phx_be_controller_busy",
    "dmc_phx_be_port_busy",
    "dmc_phx_be_controller_busy_pct",
    "dmc_phx_be_port_busy_pct",
    "dmc_phx_be_cmdq_almost_full_pct",
    "dmc_phx_fe_retry_pct",
    "dmc_phx_fe_timeout_credit_pct",
    "benchmark_score",
]

def select_opcodes(opcode, ccg_set):
    ccg_set = ccg_set.lower()
    if ccg_set == "none":
        return []
    if ccg_set == "read":
        return READ_OPCODES
    if ccg_set == "write":
        return WRITE_OPCODES
    if ccg_set in ("readwrite", "all"):
        return CCG_OPCODES
    if ccg_set == "single":
        for name, value in CCG_OPCODES:
            if opcode.lower() in (name.lower(), value.lower()):
                return [(name, value)]
        valid = ", ".join(name for name, _ in CCG_OPCODES)
        raise SystemExit(f"Unknown opcode '{opcode}'. Valid: {valid}")
    raise SystemExit(f"Unknown --ccg-set '{ccg_set}'")


def parse_extra_perf_events(spec, prefix):
    out = []
    if not spec:
        return out
    for i, ev in enumerate(str(spec).split(",")):
        ev = ev.strip()
        if ev:
            out.append((f"{prefix}_{i}", ev))
    return out

DMC_PHX_MC_PORTS = list(range(48))

# DMC-Phoenix event encodings from Arm DMC-Phoenix Infrastructure Telemetry spec.
DMC_PHX_FE_CYCLES = dict(event="0x10", filter="0x0", filter2="0x0", filter3="0x0")
DMC_PHX_FE_RETRY = dict(event="0x50", filter="0x0", filter2="0x0", filter3="0x0")
DMC_PHX_FE_TIMEOUT_CREDIT = dict(event="0x60", filter="0x0", filter2="0x0", filter3="0x0")
DMC_PHX_BE_BUFFER_FULL = dict(event="0x70", filter="0x0", filter2="0x0", filter3="0x0")
DMC_PHX_BE_QUEUE_ALLOC_DEALLOC = dict(event="0xD0", filter="0x0", filter2="0x0", filter3="0x0")
DMC_PHX_BE_CYCLES = dict(event="0x110", filter="0x0", filter2="0x0", filter3="0x0")
DMC_PHX_BE_CMDQ_ALMOST_FULL = dict(event="0x80", filter="0x0", filter2="0x0", filter3="0x0")
DMC_PHX_BE_CONTROLLER_BUSY = dict(event="0x90", filter="0x0", filter2="0x0", filter3="0x0")
DMC_PHX_BE_PORT_BUSY = dict(event="0x160", filter="0x0", filter2="0x0", filter3="0x0")


def dmc_phx_event_string(port, cfg):
    return f"arm_cspmu_mc_{port}/config={cfg['event']}/"


def dmc_phx_filtered_event_string(port, cfg):
    parts = [f"event={cfg['event']}"]
    if "filter" in cfg:
        parts.append(f"filter={cfg['filter']}")
    if "filter2" in cfg:
        parts.append(f"filter2={cfg['filter2']}")
    if "filter3" in cfg:
        parts.append(f"filter3={cfg['filter3']}")
    return f"arm_cspmu_mc_{port}/" + ",".join(parts) + "/"


# DMC Phoenix FE CHI request-interface events used for read/write/retry mix.
# Event codes follow the DMC Phoenix FE event list:
#   CHI_REQIF_OP_*        -> 0x20
#   CHI_REQIF_TRANSFER    -> 0x40
#   CHI_REQ_XMIT_*RETRIES -> 0x50
# Payload bit masks follow PMU_CMD_TYPE_PAYLOAD and PMU_RETRY_REASON_PAYLOAD.
DMC_PHX_CHI_REQ_EVENTS = [
    ("dmc_chi_reqif_transfer", dict(event="0x40", filter="0x0")),
    ("dmc_chi_req_xmit_rd_retries", dict(event="0x50", filter="0x0", filter2="0x4")),
    ("dmc_chi_req_xmit_wr_retries", dict(event="0x50", filter="0x0", filter2="0x2")),
    ("dmc_chi_reqif_op_writenosnpfull", dict(event="0x20", filter="0x0", filter2="0x8")),
    ("dmc_chi_reqif_op_writenosnpfull_ptl_pcmosep", dict(event="0x20", filter="0x0", filter2="0x100")),
    ("dmc_chi_reqif_op_writenosnpptl", dict(event="0x20", filter="0x0", filter2="0x4")),
    ("dmc_chi_reqif_op_writezero", dict(event="0x20", filter="0x0", filter2="0x80")),
    ("dmc_chi_reqif_op_readnosnpsep", dict(event="0x20", filter="0x0", filter2="0x2")),
    ("dmc_chi_reqif_op_readnosnp", dict(event="0x20", filter="0x0", filter2="0x1")),
]


def build_dmc_phx_chi_req_events(ports=None):
    ports = DMC_PHX_MC_PORTS if ports is None else ports
    events = []
    for metric_name, cfg in DMC_PHX_CHI_REQ_EVENTS:
        for p in ports:
            events.append((metric_name, dmc_phx_filtered_event_string(p, cfg)))
    return events


def build_dmc_phx_all_events(metric_name, cfg, ports=None):
    """
    Build one logical metric aggregated over all MC PMU ports/channels.
    Multiple perf events intentionally share the same friendly name, so parser
    sums all 48 arm_cspmu_mc_N instances into one counter per interval.
    """
    ports = DMC_PHX_MC_PORTS if ports is None else ports
    return [(metric_name, dmc_phx_event_string(p, cfg)) for p in ports]




def perf_cmd(events, bench_cmd):
    cmd = ["perf", "stat", "-I", "1000", "-x,", "-a"]
    for _, ev in events:
        cmd.extend(["-e", ev])
    cmd.extend(["--", "bash", "-lc", bench_cmd])
    return cmd

def run_perf(events, bench_cmd, out_file):
    with open(out_file, "w") as f:
        p = subprocess.run(perf_cmd(events, bench_cmd), stdout=f, stderr=subprocess.STDOUT, text=True)
    return p.returncode

def build_ccg_events(nodes, opcode_value, both_cmns=False):
    cmns = ["arm_cmn_0", "arm_cmn_1"] if both_cmns else ["arm_cmn_0"]
    events = []
    for cmn in cmns:
        for node in nodes:
            for direction in ("watchpoint_up", "watchpoint_down"):
                ev = (
                    f"{cmn}/{direction},nodeid={node},bynodeid=1,"
                    f"wp_chn_sel=0,wp_dev_sel=1,wp_grp=2,"
                    f"wp_val={opcode_value},wp_mask={MASK}/"
                )
                events.append((f"ccg_{cmn}_{direction}_{node}", ev))
    return events


def build_ccg_cbusy_read_a_events():
    op = "0x800013000"
    events = [
        ("cmn0_cbusy00", "arm_cmn_0/hns_cbusy00_all/"),
        ("cmn0_cbusy01", "arm_cmn_0/hns_cbusy01_all/"),
        ("cmn1_cbusy00", "arm_cmn_1/hns_cbusy00_all/"),
        ("cmn1_cbusy01", "arm_cmn_1/hns_cbusy01_all/"),
    ]
    for cmn in ["arm_cmn_0", "arm_cmn_1"]:
        for direction in ("watchpoint_up", "watchpoint_down"):
            ev = (
                f"{cmn}/{direction},nodeid=0x0,bynodeid=1,"
                f"wp_chn_sel=0,wp_dev_sel=1,wp_grp=2,"
                f"wp_val={op},wp_mask={MASK}/"
            )
            events.append((f"ccg_{cmn}_{direction}_0x0_READ_A", ev))
    return events


def build_ccg_cbusy_read_b_events():
    op = "0x800003800"
    events = [
        ("cmn0_cbusy10", "arm_cmn_0/hns_cbusy10_all/"),
        ("cmn0_cbusy11", "arm_cmn_0/hns_cbusy11_all/"),
        ("cmn1_cbusy10", "arm_cmn_1/hns_cbusy10_all/"),
        ("cmn1_cbusy11", "arm_cmn_1/hns_cbusy11_all/"),
    ]
    for cmn in ["arm_cmn_0", "arm_cmn_1"]:
        for direction in ("watchpoint_up", "watchpoint_down"):
            ev = (
                f"{cmn}/{direction},nodeid=0x80,bynodeid=1,"
                f"wp_chn_sel=0,wp_dev_sel=1,wp_grp=2,"
                f"wp_val={op},wp_mask={MASK}/"
            )
            events.append((f"ccg_{cmn}_{direction}_0x80_READ_B", ev))
    return events



def build_ccg_pocq_writeunique_events():
    op = "0x80000a800"
    events = list(POCQ_EVENTS)
    for cmn in ["arm_cmn_0", "arm_cmn_1"]:
        for direction in ("watchpoint_up", "watchpoint_down"):
            ev = (
                f"{cmn}/{direction},nodeid=0x100,bynodeid=1,"
                f"wp_chn_sel=0,wp_dev_sel=1,wp_grp=2,"
                f"wp_val={op},wp_mask={MASK}/"
            )
            events.append((f"ccg_{cmn}_{direction}_0x100_WRITEUNIQUEFULL_AR1", ev))
    return events


def build_ccg_pocq_writeback_events():
    op = "0x80000d800"
    events = list(POCQ_EVENTS)
    for cmn in ["arm_cmn_0", "arm_cmn_1"]:
        for direction in ("watchpoint_up", "watchpoint_down"):
            ev = (
                f"{cmn}/{direction},nodeid=0x180,bynodeid=1,"
                f"wp_chn_sel=0,wp_dev_sel=1,wp_grp=2,"
                f"wp_val={op},wp_mask={MASK}/"
            )
            events.append((f"ccg_{cmn}_{direction}_0x180_WRITEBACKFULL_AR1", ev))
    return events



HNS_CONGESTION_EVENTS = [
    ("cmn0_hns_txdat_stall", "arm_cmn_0/hns_txdat_stall/"),
    ("cmn1_hns_txdat_stall", "arm_cmn_1/hns_txdat_stall/"),
    ("cmn0_hns_txrsp_stall", "arm_cmn_0/hns_txrsp_stall/"),
    ("cmn1_hns_txrsp_stall", "arm_cmn_1/hns_txrsp_stall/"),
    ("cmn0_hns_sn_throttle_read", "arm_cmn_0/hns_sn_throttle_read/"),
    ("cmn1_hns_sn_throttle_read", "arm_cmn_1/hns_sn_throttle_read/"),
    ("cmn0_hns_sn_throttle_write", "arm_cmn_0/hns_sn_throttle_write/"),
    ("cmn1_hns_sn_throttle_write", "arm_cmn_1/hns_sn_throttle_write/"),
]



FULL_CCG_NODE_GROUPS = [
    ("00_80", ["0x0", "0x80"]),
    ("100_180", ["0x100", "0x180"]),
    ("280_300", ["0x280", "0x300"]),
    ("380_400", ["0x380", "0x400"]),
]


COMBINED_CCG_NODE_GROUPS = [
    ("00_80__280_300", ["0x0", "0x80", "0x280", "0x300"]),
    ("100_180__380_400", ["0x100", "0x180", "0x380", "0x400"]),
]


def build_full_ccg_nodepair_events(op, nodes, label, extra_events=None):
    events = []
    if extra_events:
        events.extend(extra_events)
    for cmn in ["arm_cmn_0", "arm_cmn_1"]:
        for node in nodes:
            for direction in ("watchpoint_up", "watchpoint_down"):
                ev = (
                    f"{cmn}/{direction},nodeid={node},bynodeid=1,"
                    f"wp_chn_sel=0,wp_dev_sel=1,wp_grp=2,"
                    f"wp_val={op},wp_mask={MASK}/"
                )
                events.append((f"ccg_{cmn}_{direction}_{node}_{label}", ev))
    return events


def build_ccg_hns_congestion_events(op, node, hns_event_name, label):
    """
    4 CCG watchpoints + 2 HNS event instances.
    hns_event_name is one base HNS event collected on both CMNs.
    """
    events = [
        (f"cmn0_{hns_event_name}", f"arm_cmn_0/{hns_event_name}/"),
        (f"cmn1_{hns_event_name}", f"arm_cmn_1/{hns_event_name}/"),
    ]
    for cmn in ["arm_cmn_0", "arm_cmn_1"]:
        for direction in ("watchpoint_up", "watchpoint_down"):
            ev = (
                f"{cmn}/{direction},nodeid={node},bynodeid=1,"
                f"wp_chn_sel=0,wp_dev_sel=1,wp_grp=2,"
                f"wp_val={op},wp_mask={MASK}/"
            )
            events.append((f"ccg_{cmn}_{direction}_{node}_{label}", ev))
    return events


def parse_running_pct(parts):
    for p in reversed(parts):
        q = p.strip().strip("()").rstrip("%")
        try:
            v = float(q)
            if 0.0 <= v <= 100.0:
                return v
        except ValueError:
            pass
    return None


def dedicated_ccg_bucket_from_label(label):
    s = str(label).lower()
    if "read_a" in s:
        return "ccg_read_a_count"
    if "read_b" in s:
        return "ccg_read_b_count"
    if "writeunique" in s:
        return "ccg_writeunique_count"
    if "writeback" in s:
        return "ccg_writeback_count"
    if "writeevictorvict" in s or "writeevict" in s:
        return "ccg_writeevictorvict_count"
    if "writenosnoop" in s or "writenosnp" in s:
        return "ccg_writenosnoop_count"
    if "writedata" in s:
        return "ccg_writedata_count"
    return None


def parse_perf_csv(path, event_map, label, parse_ccg=False):
    """
    Parse one perf CSV and align samples by interval index, not raw perf timestamp.

    perf stat -I timestamps restart for each independent benchmark run. Since CPU,
    HNS and CCG are collected in separate passes, raw timestamps must not be used as
    a global x-axis. We map each unique timestamp in this file to 1,2,3,... and
    merge different passes by that interval index.
    """
    rows = defaultdict(dict)
    mux_rows = []
    not_counted = []

    if not Path(path).exists():
        return rows, mux_rows, not_counted

    ts_to_idx = {}
    dedicated_ccg_bucket = dedicated_ccg_bucket_from_label(label) if parse_ccg else None

    def interval_idx(ts):
        if ts not in ts_to_idx:
            ts_to_idx[ts] = len(ts_to_idx) + 1
        return ts_to_idx[ts]

    with open(path, errors="replace") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue

            if "<not counted>" in raw or "<not supported>" in raw:
                not_counted.append([label, raw])
                continue

            parts = raw.split(",")
            if len(parts) < 4:
                continue

            try:
                ts = float(parts[0])
                count = int(parts[1])
            except ValueError:
                continue

            idx = interval_idx(ts)

            name = None
            if parse_ccg and "watchpoint_" in raw:
                if "watchpoint_up" in raw:
                    name = "ccg_up_count"
                elif "watchpoint_down" in raw:
                    name = "ccg_down_count"

            if name is None:
                for friendly, ev in sorted(event_map, key=lambda x: len(x[1] or ""), reverse=True):
                    if not ev:
                        continue
                    if ev in raw:
                        name = friendly
                        break

            if name is None:
                continue

            rows[idx][name] = rows[idx].get(name, 0) + count

            if name in ("ccg_up_count", "ccg_down_count") and dedicated_ccg_bucket:
                rows[idx][dedicated_ccg_bucket] = rows[idx].get(dedicated_ccg_bucket, 0) + count

            pct = parse_running_pct(parts)
            if pct is not None and pct < 99.0 and count != 0:
                mux_rows.append([label, idx, raw, pct])

    return rows, mux_rows, not_counted

def parse_score_from_text_file(path):
    if not Path(path).exists():
        return None
    ratio_rx = re.compile(r"\bratio\s*=\s*([0-9]+(?:\.[0-9]+)?)\b", re.IGNORECASE)
    try:
        with open(path, errors="replace") as f:
            for line in f:
                m = ratio_rx.search(line)
                if m:
                    return float(m.group(1))
    except Exception:
        return None
    return None


def parse_benchmark_metrics_from_file(path):
    """Return (bw_mem_GBps, spec_ratio, referenced_files)."""
    bw = None
    score = None
    refs = []
    if not Path(path).exists():
        return bw, score, refs
    bw_rx = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s+([0-9]+(?:\.[0-9]+)?)\s*$")
    ratio_rx = re.compile(r"\bratio\s*=\s*([0-9]+(?:\.[0-9]+)?)\b", re.IGNORECASE)
    # Capture likely SPEC result/log paths printed by runcpu.
    path_rx = re.compile(r"(?P<path>(?:/|\./|[A-Za-z0-9_.-]+/)[^\s:'\"]*(?:result|log)[^\s:'\"]*)")
    try:
        with open(path, errors="replace") as f:
            for line in f:
                m = bw_rx.match(line)
                if m:
                    bw = float(m.group(2)) / 1000.0
                m = ratio_rx.search(line)
                if m:
                    score = float(m.group(1))
                for pm in path_rx.finditer(line):
                    p = pm.group('path').strip()
                    if p and p not in refs:
                        refs.append(p)
    except Exception:
        return bw, score, refs
    if score is None:
        base = Path(path).parent
        for r in refs:
            pp = Path(r)
            if not pp.is_absolute():
                pp = base / pp
            s = parse_score_from_text_file(pp)
            if s is not None:
                score = s
                break
    return bw, score, refs


def parse_bw_mem_gbps_from_file(path):
    bw, _, _ = parse_benchmark_metrics_from_file(path)
    return bw

def infer_spec_root_from_cmd(cmd):
    if not cmd:
        return None
    m = re.search(r"\bcd\s+([^;&|]+)", cmd)
    if not m:
        return None
    return m.group(1).strip().strip("'\"")


def find_latest_ratio_in_spec_result(spec_root):
    if not spec_root:
        return None, None
    result_dir = Path(spec_root) / 'result'
    if not result_dir.exists():
        return None, None
    files = sorted([p for p in result_dir.rglob('*') if p.is_file()], key=lambda p: p.stat().st_mtime, reverse=True)
    for p in files[:200]:
        s = parse_score_from_text_file(p)
        if s is not None:
            return s, str(p)
    return None, None


def find_benchmark_metrics(outdir, spec_root=None):
    bw = None
    score = None
    source = ''
    for rel in ['cpu_local.perf.csv', 'remote.perf.csv', 'slc.perf.csv']:
        b, s, _ = parse_benchmark_metrics_from_file(Path(outdir) / rel)
        if b is not None:
            bw = b
        if s is not None:
            score = s
            source = str(Path(outdir) / rel)
        if bw is not None or score is not None:
            return bw, score, source
    for p in sorted(Path(outdir).rglob('*.perf.csv')):
        b, s, _ = parse_benchmark_metrics_from_file(p)
        if b is not None and bw is None:
            bw = b
        if s is not None and score is None:
            score = s
            source = str(p)
        if bw is not None or score is not None:
            return bw, score, source
    s, src = find_latest_ratio_in_spec_result(spec_root)
    if s is not None:
        score = s
        source = src or ''
    return bw, score, source


def find_bw_mem_gbps(outdir):
    bw, _, _ = find_benchmark_metrics(outdir)
    return bw

def merge(dst, src):
    for sec, vals in src.items():
        for k, v in vals.items():
            dst[sec][k] = dst[sec].get(k, 0) + v

def add_derived(rows, bw_mem, benchmark_score=None):
    for _, r in rows.items():
        instr = r.get("instructions", 0)
        cyc = r.get("cpu_cycles", 0)
        l2_refill = r.get("l2d_cache_refill", 0)
        l2_cache = r.get("l2d_cache", 0)

        r["ipc"] = instr / cyc if cyc else 0.0
        r["l2_miss_pct"] = 100.0 * l2_refill / l2_cache if l2_cache else 0.0

        # CPU event scaling: CPU read/write events are 32B each.
        r["cpu_read_bw_GBps"] = r.get("cpu_read_0x60", 0) * 32 / BYTES_PER_GB
        r["cpu_write_bw_GBps"] = r.get("cpu_write_0x61", 0) * 32 / BYTES_PER_GB
        r["cpu_total_bw_GBps"] = r["cpu_read_bw_GBps"] + r["cpu_write_bw_GBps"]

        local_req = r.get("cmn0_local_req", 0) + r.get("cmn1_local_req", 0)
        remote_req = r.get("cmn0_remote_req", 0) + r.get("cmn1_remote_req", 0)
        local_retry = r.get("cmn0_local_retry", 0) + r.get("cmn1_local_retry", 0)
        remote_retry = r.get("cmn0_remote_retry", 0) + r.get("cmn1_remote_retry", 0)

        slc_access = r.get("cmn0_slc_access", 0) + r.get("cmn1_slc_access", 0)
        slc_miss = r.get("cmn0_slc_miss", 0) + r.get("cmn1_slc_miss", 0)

        total_req = local_req + remote_req

        r["local_req_total"] = local_req
        r["remote_req_total"] = remote_req
        r["local_retry_total"] = local_retry
        r["remote_retry_total"] = remote_retry
        r["slc_access_total"] = slc_access
        r["slc_miss_total"] = slc_miss
        r["slc_miss_access_pct"] = 100.0 * slc_miss / slc_access if slc_access else 0.0
        r["local_pct"] = 100.0 * local_req / total_req if total_req else 0.0
        r["remote_pct"] = 100.0 * remote_req / total_req if total_req else 0.0

        r["local_snf_bw_GBps"] = local_req * BYTES_PER_FLIT / BYTES_PER_GB
        r["remote_snf_bw_GBps"] = remote_req * BYTES_PER_FLIT / BYTES_PER_GB
        r["total_snf_bw_GBps"] = total_req * BYTES_PER_FLIT / BYTES_PER_GB

        cbusy00 = r.get("cmn0_cbusy00", 0) + r.get("cmn1_cbusy00", 0)
        cbusy01 = r.get("cmn0_cbusy01", 0) + r.get("cmn1_cbusy01", 0)
        cbusy10 = r.get("cmn0_cbusy10", 0) + r.get("cmn1_cbusy10", 0)
        cbusy11 = r.get("cmn0_cbusy11", 0) + r.get("cmn1_cbusy11", 0)
        cbusy_total = cbusy00 + cbusy01 + cbusy10 + cbusy11
        r["cbusy00_total"] = cbusy00
        r["cbusy01_total"] = cbusy01
        r["cbusy10_total"] = cbusy10
        r["cbusy11_total"] = cbusy11
        r["cbusy_total"] = cbusy_total
        r["cbusy00_pct"] = 100.0 * cbusy00 / cbusy_total if cbusy_total else 0.0
        r["cbusy01_pct"] = 100.0 * cbusy01 / cbusy_total if cbusy_total else 0.0
        r["cbusy10_pct"] = 100.0 * cbusy10 / cbusy_total if cbusy_total else 0.0
        r["cbusy11_pct"] = 100.0 * cbusy11 / cbusy_total if cbusy_total else 0.0

        pocq_occup = r.get("cmn0_pocq_occup_class0", 0) + r.get("cmn1_pocq_occup_class0", 0)
        pocq_retry = r.get("cmn0_pocq_retry_class0", 0) + r.get("cmn1_pocq_retry_class0", 0)
        r["pocq_occup_class0_total"] = pocq_occup
        r["pocq_retry_class0_total"] = pocq_retry
        # perf interval is 1 second, so counter / (2 GHz * 2 CMNs) gives percent.
        # This is the same convention for POCQ occupancy/retry and HNS congestion.
        denom = CMN_CLOCK_HZ * 2.0
        r["pocq_occup_class0_pct"] = 100.0 * pocq_occup / denom if denom else 0.0
        r["pocq_retry_class0_pct"] = 100.0 * pocq_retry / denom if denom else 0.0

        hns_txdat_stall = r.get("cmn0_hns_txdat_stall", 0) + r.get("cmn1_hns_txdat_stall", 0)
        hns_txrsp_stall = r.get("cmn0_hns_txrsp_stall", 0) + r.get("cmn1_hns_txrsp_stall", 0)
        hns_throttle_read = r.get("cmn0_hns_sn_throttle_read", 0) + r.get("cmn1_hns_sn_throttle_read", 0)
        hns_throttle_write = r.get("cmn0_hns_sn_throttle_write", 0) + r.get("cmn1_hns_sn_throttle_write", 0)

        r["hns_txdat_stall_total"] = hns_txdat_stall
        r["hns_txrsp_stall_total"] = hns_txrsp_stall
        r["hns_sn_throttle_read_total"] = hns_throttle_read
        r["hns_sn_throttle_write_total"] = hns_throttle_write
        r["hns_txdat_stall_pct"] = 100.0 * hns_txdat_stall / denom if denom else 0.0
        r["hns_txrsp_stall_pct"] = 100.0 * hns_txrsp_stall / denom if denom else 0.0
        r["hns_sn_throttle_read_pct"] = 100.0 * hns_throttle_read / denom if denom else 0.0
        r["hns_sn_throttle_write_pct"] = 100.0 * hns_throttle_write / denom if denom else 0.0

        ccg_up = r.get("ccg_up_count", 0)
        ccg_down = r.get("ccg_down_count", 0)
        r["ccg_up_down_count"] = ccg_up + ccg_down
        r["ccg_up_bw_GBps"] = ccg_up * BYTES_PER_FLIT / BYTES_PER_GB
        r["ccg_down_bw_GBps"] = ccg_down * BYTES_PER_FLIT / BYTES_PER_GB
        r["ccg_up_down_bw_GBps"] = (ccg_up + ccg_down) * BYTES_PER_FLIT / BYTES_PER_GB

        r["ccg_read_a_bw_GBps"] = r.get("ccg_read_a_count", 0) * BYTES_PER_FLIT / BYTES_PER_GB
        r["ccg_read_b_bw_GBps"] = r.get("ccg_read_b_count", 0) * BYTES_PER_FLIT / BYTES_PER_GB
        r["ccg_writeunique_bw_GBps"] = r.get("ccg_writeunique_count", 0) * BYTES_PER_FLIT / BYTES_PER_GB
        r["ccg_writeback_bw_GBps"] = r.get("ccg_writeback_count", 0) * BYTES_PER_FLIT / BYTES_PER_GB
        r["ccg_writeevictorvict_bw_GBps"] = r.get("ccg_writeevictorvict_count", 0) * BYTES_PER_FLIT / BYTES_PER_GB
        r["ccg_writenosnoop_bw_GBps"] = r.get("ccg_writenosnoop_count", 0) * BYTES_PER_FLIT / BYTES_PER_GB
        r["ccg_writedata_bw_GBps"] = r.get("ccg_writedata_count", 0) * BYTES_PER_FLIT / BYTES_PER_GB
        r["ccg_dedicated_total_bw_GBps"] = (
            r["ccg_read_a_bw_GBps"]
            + r["ccg_read_b_bw_GBps"]
            + r["ccg_writeunique_bw_GBps"]
            + r["ccg_writeback_bw_GBps"]
            + r["ccg_writeevictorvict_bw_GBps"]
            + r["ccg_writenosnoop_bw_GBps"]
            + r["ccg_writedata_bw_GBps"]
        )

        dmc_chi_rd_ops = r.get("dmc_chi_reqif_op_readnosnpsep", 0) + r.get("dmc_chi_reqif_op_readnosnp", 0)
        dmc_chi_wr_ops = (
            r.get("dmc_chi_reqif_op_writenosnpfull", 0)
            + r.get("dmc_chi_reqif_op_writenosnpfull_ptl_pcmosep", 0)
            + r.get("dmc_chi_reqif_op_writenosnpptl", 0)
            + r.get("dmc_chi_reqif_op_writezero", 0)
        )
        dmc_chi_rdwr_ops = dmc_chi_rd_ops + dmc_chi_wr_ops
        dmc_chi_rd_retries = r.get("dmc_chi_req_xmit_rd_retries", 0)
        dmc_chi_wr_retries = r.get("dmc_chi_req_xmit_wr_retries", 0)
        dmc_chi_transfer = r.get("dmc_chi_reqif_transfer", 0)

        r["dmc_chi_reqif_rd_ops_total"] = dmc_chi_rd_ops
        r["dmc_chi_reqif_wr_ops_total"] = dmc_chi_wr_ops
        r["dmc_chi_reqif_rdwr_ops_total"] = dmc_chi_rdwr_ops
        r["dmc_chi_rd_retry_pct"] = 100.0 * dmc_chi_rd_retries / dmc_chi_rd_ops if dmc_chi_rd_ops else 0.0
        r["dmc_chi_wr_retry_pct"] = 100.0 * dmc_chi_wr_retries / dmc_chi_wr_ops if dmc_chi_wr_ops else 0.0
        r["dmc_chi_retry_pct"] = 100.0 * (dmc_chi_rd_retries + dmc_chi_wr_retries) / dmc_chi_transfer if dmc_chi_transfer else 0.0
        r["dmc_chi_rd_pct"] = 100.0 * dmc_chi_rd_ops / dmc_chi_transfer if dmc_chi_transfer else 0.0
        r["dmc_chi_wr_pct"] = 100.0 * dmc_chi_wr_ops / dmc_chi_transfer if dmc_chi_transfer else 0.0
        r["dmc_chi_rd_without_retry_pct"] = 100.0 * (dmc_chi_rd_ops - dmc_chi_rd_retries) / dmc_chi_rd_ops if dmc_chi_rd_ops else 0.0
        r["dmc_chi_wr_without_retry_pct"] = 100.0 * (dmc_chi_wr_ops - dmc_chi_wr_retries) / dmc_chi_wr_ops if dmc_chi_wr_ops else 0.0
        r["dmc_chi_rdwr_without_retry_pct"] = 100.0 * (dmc_chi_rdwr_ops - dmc_chi_rd_retries - dmc_chi_wr_retries) / dmc_chi_rdwr_ops if dmc_chi_rdwr_ops else 0.0

        cpu_fe_cycles = r.get("dmc_phx_cpu_fe_cycles", 0)
        cpu_fe_retry = r.get("dmc_phx_cpu_fe_retry", 0)
        remote_fe_cycles = r.get("dmc_phx_remote_fe_cycles", 0)
        remote_fe_timeout = r.get("dmc_phx_remote_fe_timeout_credit", 0)

        r["dmc_phx_fe_retry_pct"] = (100.0 * cpu_fe_retry / cpu_fe_cycles) if cpu_fe_cycles else 0.0
        r["dmc_phx_fe_timeout_credit_pct"] = (100.0 * remote_fe_timeout / remote_fe_cycles) if remote_fe_cycles else 0.0

        be_cycles = r.get("dmc_phx_be_cycles", 0)
        be_controller_busy = r.get("dmc_phx_be_controller_busy", 0)
        be_port_busy = r.get("dmc_phx_be_port_busy", 0)
        be_cmdq_almost_full = r.get("dmc_phx_be_cmdq_almost_full", 0)

        r["dmc_phx_be_controller_busy_pct"] = (100.0 * be_controller_busy / be_cycles) if be_cycles else 0.0
        r["dmc_phx_be_port_busy_pct"] = (100.0 * be_port_busy / be_cycles) if be_cycles else 0.0
        r["dmc_phx_be_cmdq_almost_full_pct"] = (100.0 * be_cmdq_almost_full / be_cycles) if be_cycles else 0.0

        r["bw_mem_GBps"] = bw_mem or 0.0
        r["benchmark_score"] = benchmark_score or 0.0

def trim_to_common_active_intervals(rows):
    """
    CPU/local, remote, and CCG are separate benchmark executions.
    After aligning each perf file by interval index, keep only intervals where
    the main active signals overlap. This removes trailing zero/partial intervals
    from one pass that otherwise make lines drop at the end of the plot.
    """
    if not rows:
        return rows

    def active_secs(keys):
        out = set()
        for sec, r in rows.items():
            if any(r.get(k, 0) > 0 for k in keys):
                out.add(sec)
        return out

    cpu_active = active_secs(["cpu_read_0x60", "cpu_write_0x61"])
    local_active = active_secs(["cmn0_local_req", "cmn1_local_req"])
    remote_active = active_secs(["cmn0_remote_req", "cmn1_remote_req"])

    active_sets = [s for s in [cpu_active, local_active, remote_active] if s]
    if not active_sets:
        return rows

    common = set.intersection(*active_sets)

    ccg_active = active_secs(["ccg_up_count", "ccg_down_count"])
    if ccg_active:
        common = common & ccg_active

    if not common:
        return rows

    # Drop first warm-up interval and last partial interval when possible.
    ordered = sorted(common)
    if len(ordered) > 2:
        ordered = ordered[1:-1]

    new_rows = defaultdict(dict)
    for new_idx, old_sec in enumerate(ordered, start=1):
        new_rows[new_idx] = rows[old_sec]

    return new_rows


def write_timeseries(path, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for sec in sorted(rows):
            out = {"time_s": sec}
            out.update(rows[sec])
            w.writerow({k: out.get(k, 0) for k in FIELDS})

def sum_metric(rows, key):
    return sum(r.get(key, 0.0) for r in rows.values())


def avg_nonzero(rows, key):
    vals = [r.get(key, 0.0) for r in rows.values() if r.get(key, 0.0) != 0]
    return sum(vals) / len(vals) if vals else 0.0


def find_sustained_active_window(samples, threshold=50.0, min_run=5):
    """
    samples: list of (time, value)
    Returns the first sustained active start and last sustained active end.

    This avoids two bad cases:
      1) Middle SPEC phase gaps should not terminate the benchmark window.
      2) Isolated tail spikes after the benchmark finishes should not extend it.

    Active groups shorter than min_run are ignored. If no sustained group exists,
    fall back to all intervals above threshold.
    """
    if not samples:
        return None, None

    samples = sorted(samples, key=lambda x: x[0])
    active_times = [t for t, v in samples if v >= threshold]
    if not active_times:
        return None, None

    groups = []
    cur = []
    prev_t = None

    # Times are integer interval indices in the parsed timeseries.
    for t, v in samples:
        if v >= threshold:
            if prev_t is None or not cur or t == prev_t + 1:
                cur.append(t)
            else:
                groups.append(cur)
                cur = [t]
        else:
            if cur:
                groups.append(cur)
                cur = []
        prev_t = t

    if cur:
        groups.append(cur)

    sustained = [g for g in groups if len(g) >= min_run]
    if not sustained:
        sustained = groups

    if not sustained:
        return min(active_times), max(active_times)

    return sustained[0][0], sustained[-1][-1]


def detect_benchmark_active_window(rows):
    """
    Detect benchmark-running window for summary statistics.

    Preferred rule:
      Use Total SNF BW as the activity signal.
      Start = first sustained active region where Total SNF BW >= 50 GB/s.
      End   = end of last sustained active region where Total SNF BW >= 50 GB/s.

    This matches the plot annotation: start when memory traffic ramps up, and end
    when Total SNF BW finally drops below ~50 GB/s and stays down. Middle SPEC
    phase gaps are included inside the window; isolated tail spikes are ignored.

    Fallback:
      If Total SNF BW never reaches 50 GB/s, use a 5% of peak activity rule.
    """
    if not rows:
        return None, None

    snf_samples = [
        (sec, r.get("total_snf_bw_GBps", 0.0))
        for sec, r in rows.items()
    ]
    start, end = find_sustained_active_window(snf_samples, threshold=50.0, min_run=5)
    if start is not None and end is not None:
        return start, end

    activity = {}
    for sec, r in rows.items():
        cpu = r.get("cpu_total_bw_GBps", 0.0)
        snf = r.get("total_snf_bw_GBps", 0.0)
        ccg = r.get("ccg_up_down_bw_GBps", 0.0)
        activity[sec] = max(cpu, snf, ccg)

    peak = max(activity.values()) if activity else 0.0
    if peak <= 0.0:
        return None, None

    threshold = peak * 0.05
    samples = sorted(activity.items())
    return find_sustained_active_window(samples, threshold=threshold, min_run=5)

def filter_rows_for_summary(rows):
    start, end = detect_benchmark_active_window(rows)
    if start is None or end is None:
        return rows, start, end

    active_rows = {
        sec: vals for sec, vals in rows.items()
        if start <= sec <= end
    }
    return active_rows, start, end


def max_nonzero(rows, key):
    vals = [r.get(key, 0.0) for r in rows.values() if r.get(key, 0.0) != 0]
    return max(vals) if vals else 0.0


def write_summary(path, rows, bw_mem, benchmark_score, score_source, pass_scores, mux_count, nc_count, ccg_set):
    summary_rows, active_start, active_end = filter_rows_for_summary(rows)

    cpu_read_avg = avg_nonzero(summary_rows, "cpu_read_bw_GBps")
    cpu_write_avg = avg_nonzero(summary_rows, "cpu_write_bw_GBps")
    cpu_total_avg = cpu_read_avg + cpu_write_avg

    cpu_read_max = max_nonzero(summary_rows, "cpu_read_bw_GBps")
    cpu_write_max = max_nonzero(summary_rows, "cpu_write_bw_GBps")
    cpu_total_max = max_nonzero(summary_rows, "cpu_total_bw_GBps")

    local_avg = avg_nonzero(summary_rows, "local_snf_bw_GBps")
    remote_avg = avg_nonzero(summary_rows, "remote_snf_bw_GBps")
    total_snf = local_avg + remote_avg

    local_max = max_nonzero(summary_rows, "local_snf_bw_GBps")
    remote_max = max_nonzero(summary_rows, "remote_snf_bw_GBps")
    total_snf_max = max_nonzero(summary_rows, "total_snf_bw_GBps")

    local_pct = 100.0 * local_avg / total_snf if total_snf else 0.0
    remote_pct = 100.0 * remote_avg / total_snf if total_snf else 0.0

    hns_cpu_bw_pct = 100.0 * total_snf / cpu_total_avg if cpu_total_avg else 0.0

    slc_access_avg = avg_nonzero(summary_rows, "slc_access_total")
    slc_miss_avg = avg_nonzero(summary_rows, "slc_miss_total")
    slc_miss_access_pct = 100.0 * slc_miss_avg / slc_access_avg if slc_access_avg else 0.0

    slc_access_max = max_nonzero(summary_rows, "slc_access_total")
    slc_miss_max = max_nonzero(summary_rows, "slc_miss_total")

    ccg_bw_avg = avg_nonzero(summary_rows, "ccg_up_down_bw_GBps")
    ccg_bw_max = max_nonzero(summary_rows, "ccg_up_down_bw_GBps")

    ccg_read_a_avg = avg_nonzero(summary_rows, "ccg_read_a_bw_GBps")
    ccg_read_b_avg = avg_nonzero(summary_rows, "ccg_read_b_bw_GBps")
    ccg_writeunique_avg = avg_nonzero(summary_rows, "ccg_writeunique_bw_GBps")
    ccg_writeback_avg = avg_nonzero(summary_rows, "ccg_writeback_bw_GBps")
    ccg_writeevictorvict_avg = avg_nonzero(summary_rows, "ccg_writeevictorvict_bw_GBps")
    ccg_writenosnoop_avg = avg_nonzero(summary_rows, "ccg_writenosnoop_bw_GBps")
    ccg_writedata_avg = avg_nonzero(summary_rows, "ccg_writedata_bw_GBps")
    ccg_dedicated_total_avg = (
        ccg_read_a_avg
        + ccg_read_b_avg
        + ccg_writeunique_avg
        + ccg_writeback_avg
        + ccg_writeevictorvict_avg
        + ccg_writenosnoop_avg
        + ccg_writedata_avg
    )

    cbusy00_avg = avg_nonzero(summary_rows, "cbusy00_total")
    cbusy01_avg = avg_nonzero(summary_rows, "cbusy01_total")
    cbusy10_avg = avg_nonzero(summary_rows, "cbusy10_total")
    cbusy11_avg = avg_nonzero(summary_rows, "cbusy11_total")
    cbusy_total_avg = avg_nonzero(summary_rows, "cbusy_total")
    cbusy_total_max = max_nonzero(summary_rows, "cbusy_total")

    # CBusy percentages must be based on sums over the active window, not
    # avg_nonzero(channel) / avg_nonzero(total), because READ_A and READ_B
    # CBusy passes are collected separately and can have different non-zero
    # interval populations.
    cbusy00_sum = sum_metric(summary_rows, "cbusy00_total")
    cbusy01_sum = sum_metric(summary_rows, "cbusy01_total")
    cbusy10_sum = sum_metric(summary_rows, "cbusy10_total")
    cbusy11_sum = sum_metric(summary_rows, "cbusy11_total")
    cbusy_grand_sum = cbusy00_sum + cbusy01_sum + cbusy10_sum + cbusy11_sum

    cbusy00_pct = 100.0 * cbusy00_sum / cbusy_grand_sum if cbusy_grand_sum else 0.0
    cbusy01_pct = 100.0 * cbusy01_sum / cbusy_grand_sum if cbusy_grand_sum else 0.0
    cbusy10_pct = 100.0 * cbusy10_sum / cbusy_grand_sum if cbusy_grand_sum else 0.0
    cbusy11_pct = 100.0 * cbusy11_sum / cbusy_grand_sum if cbusy_grand_sum else 0.0

    pocq_occup_avg = avg_nonzero(summary_rows, "pocq_occup_class0_total")
    pocq_retry_avg = avg_nonzero(summary_rows, "pocq_retry_class0_total")
    pocq_occup_pct_avg = avg_nonzero(summary_rows, "pocq_occup_class0_pct")
    pocq_occup_pct_max = max_nonzero(summary_rows, "pocq_occup_class0_pct")
    pocq_retry_pct_avg = avg_nonzero(summary_rows, "pocq_retry_class0_pct")
    pocq_retry_pct_max = max_nonzero(summary_rows, "pocq_retry_class0_pct")

    hns_txdat_stall_pct_avg = avg_nonzero(summary_rows, "hns_txdat_stall_pct")
    hns_txdat_stall_pct_max = max_nonzero(summary_rows, "hns_txdat_stall_pct")
    hns_txrsp_stall_pct_avg = avg_nonzero(summary_rows, "hns_txrsp_stall_pct")
    hns_txrsp_stall_pct_max = max_nonzero(summary_rows, "hns_txrsp_stall_pct")
    hns_throttle_read_pct_avg = avg_nonzero(summary_rows, "hns_sn_throttle_read_pct")
    hns_throttle_read_pct_max = max_nonzero(summary_rows, "hns_sn_throttle_read_pct")
    hns_throttle_write_pct_avg = avg_nonzero(summary_rows, "hns_sn_throttle_write_pct")
    hns_throttle_write_pct_max = max_nonzero(summary_rows, "hns_sn_throttle_write_pct")

    dmc_summary_values = {}
    for base in ["cpu_local", "remote", "slc"]:
        for i in range(64):
            key = f"dmc_{base}_{i}"
            dmc_summary_values[f"{key}_avg"] = avg_nonzero(summary_rows, key)
            dmc_summary_values[f"{key}_max"] = max_nonzero(summary_rows, key)

    dmc_chi_summary_values = {}
    for key in [
        "dmc_chi_reqif_transfer",
        "dmc_chi_req_xmit_rd_retries",
        "dmc_chi_req_xmit_wr_retries",
        "dmc_chi_reqif_op_writenosnpfull",
        "dmc_chi_reqif_op_writenosnpfull_ptl_pcmosep",
        "dmc_chi_reqif_op_writenosnpptl",
        "dmc_chi_reqif_op_writezero",
        "dmc_chi_reqif_op_readnosnpsep",
        "dmc_chi_reqif_op_readnosnp",
        "dmc_chi_reqif_rd_ops_total",
        "dmc_chi_reqif_wr_ops_total",
        "dmc_chi_reqif_rdwr_ops_total",
        "dmc_chi_rd_retry_pct",
        "dmc_chi_wr_retry_pct",
        "dmc_chi_retry_pct",
        "dmc_chi_rd_pct",
        "dmc_chi_wr_pct",
        "dmc_chi_rd_without_retry_pct",
        "dmc_chi_wr_without_retry_pct",
        "dmc_chi_rdwr_without_retry_pct",
    ]:
        dmc_chi_summary_values[f"{key}_avg"] = avg_nonzero(summary_rows, key)
        dmc_chi_summary_values[f"{key}_max"] = max_nonzero(summary_rows, key)

    dmc_phx_cpu_fe_cycles_avg = avg_nonzero(summary_rows, "dmc_phx_cpu_fe_cycles")
    dmc_phx_cpu_fe_retry_avg = avg_nonzero(summary_rows, "dmc_phx_cpu_fe_retry")
    dmc_phx_remote_fe_cycles_avg = avg_nonzero(summary_rows, "dmc_phx_remote_fe_cycles")
    dmc_phx_remote_fe_timeout_avg = avg_nonzero(summary_rows, "dmc_phx_remote_fe_timeout_credit")
    dmc_phx_be_buffer_full_avg = avg_nonzero(summary_rows, "dmc_phx_be_buffer_full")
    dmc_phx_be_queue_alloc_dealloc_avg = avg_nonzero(summary_rows, "dmc_phx_be_queue_alloc_dealloc")
    dmc_phx_be_cycles_avg = avg_nonzero(summary_rows, "dmc_phx_be_cycles")
    dmc_phx_be_cmdq_almost_full_avg = avg_nonzero(summary_rows, "dmc_phx_be_cmdq_almost_full")
    dmc_phx_be_controller_busy_avg = avg_nonzero(summary_rows, "dmc_phx_be_controller_busy")
    dmc_phx_be_port_busy_avg = avg_nonzero(summary_rows, "dmc_phx_be_port_busy")
    dmc_phx_be_controller_busy_pct_avg = avg_nonzero(summary_rows, "dmc_phx_be_controller_busy_pct")
    dmc_phx_be_controller_busy_pct_max = max_nonzero(summary_rows, "dmc_phx_be_controller_busy_pct")
    dmc_phx_be_port_busy_pct_avg = avg_nonzero(summary_rows, "dmc_phx_be_port_busy_pct")
    dmc_phx_be_port_busy_pct_max = max_nonzero(summary_rows, "dmc_phx_be_port_busy_pct")
    dmc_phx_be_cmdq_almost_full_pct_avg = avg_nonzero(summary_rows, "dmc_phx_be_cmdq_almost_full_pct")
    dmc_phx_be_cmdq_almost_full_pct_max = max_nonzero(summary_rows, "dmc_phx_be_cmdq_almost_full_pct")

    dmc_phx_fe_retry_pct_avg = avg_nonzero(summary_rows, "dmc_phx_fe_retry_pct")
    dmc_phx_fe_retry_pct_max = max_nonzero(summary_rows, "dmc_phx_fe_retry_pct")
    dmc_phx_fe_timeout_credit_pct_avg = avg_nonzero(summary_rows, "dmc_phx_fe_timeout_credit_pct")
    dmc_phx_fe_timeout_credit_pct_max = max_nonzero(summary_rows, "dmc_phx_fe_timeout_credit_pct")

    if bw_mem and bw_mem > 0:
        delta_pct = (total_snf - bw_mem) / bw_mem * 100.0
        result_2pct = "PASS" if abs(delta_pct) <= 2.0 else "FAIL"
    else:
        delta_pct = 0.0
        result_2pct = "NO_BWMEM"

    score_values = [float(x["score"]) for x in pass_scores if x.get("score") not in (None, "")]
    if score_values:
        score_min = min(score_values)
        score_max = max(score_values)
        score_avg = sum(score_values) / len(score_values)
        score_delta_pct = 100.0 * (score_max - score_min) / score_avg if score_avg else 0.0
    else:
        score_min = score_max = score_avg = score_delta_pct = 0.0

    benchmark_type = "SPEC" if (benchmark_score or 0.0) != 0.0 else ("bw_mem" if (bw_mem or 0.0) != 0.0 else "unknown")

    data = [
        ["benchmark_type", benchmark_type],
        ["active_start_s", active_start if active_start is not None else ""],
        ["active_end_s", active_end if active_end is not None else ""],
        ["active_intervals", len(summary_rows)],

        ["ccg_set", ccg_set],
        ["ccg_enabled", "yes" if ccg_set not in ("none", "") else "no"],
        ["bw_mem_GBps", f"{bw_mem or 0.0:.9f}"],
        ["benchmark_score", f"{benchmark_score or 0.0:.9f}"],
        ["benchmark_score_source", score_source or ""],
        ["benchmark_score_min", f"{score_min:.9f}"],
        ["benchmark_score_max", f"{score_max:.9f}"],
        ["benchmark_score_avg", f"{score_avg:.9f}"],
        ["benchmark_score_delta_pct", f"{score_delta_pct:.6f}"],

        ["cpu_read_bw_avg_GBps", f"{cpu_read_avg:.9f}"],
        ["cpu_write_bw_avg_GBps", f"{cpu_write_avg:.9f}"],
        ["cpu_total_bw_avg_GBps", f"{cpu_total_avg:.9f}"],
        ["cpu_read_bw_max_GBps", f"{cpu_read_max:.9f}"],
        ["cpu_write_bw_max_GBps", f"{cpu_write_max:.9f}"],
        ["cpu_total_bw_max_GBps", f"{cpu_total_max:.9f}"],

        ["local_snf_bw_avg_GBps", f"{local_avg:.9f}"],
        ["remote_snf_bw_avg_GBps", f"{remote_avg:.9f}"],
        ["total_snf_bw_avg_GBps", f"{total_snf:.9f}"],
        ["local_snf_bw_max_GBps", f"{local_max:.9f}"],
        ["remote_snf_bw_max_GBps", f"{remote_max:.9f}"],
        ["total_snf_bw_max_GBps", f"{total_snf_max:.9f}"],

        ["local_pct_avg", f"{local_pct:.6f}"],
        ["remote_pct_avg", f"{remote_pct:.6f}"],
        ["hns_cpu_bw_pct", f"{hns_cpu_bw_pct:.6f}"],

        ["slc_access_avg", f"{slc_access_avg:.9f}"],
        ["slc_miss_avg", f"{slc_miss_avg:.9f}"],
        ["slc_access_max", f"{slc_access_max:.9f}"],
        ["slc_miss_max", f"{slc_miss_max:.9f}"],
        ["slc_miss_access_pct", f"{slc_miss_access_pct:.6f}"],

        ["ccg_bw_avg_GBps", f"{ccg_bw_avg:.9f}"],
        ["ccg_bw_max_GBps", f"{ccg_bw_max:.9f}"],
        ["ccg_read_a_bw_avg_GBps", f"{ccg_read_a_avg:.9f}"],
        ["ccg_read_b_bw_avg_GBps", f"{ccg_read_b_avg:.9f}"],
        ["ccg_writeunique_bw_avg_GBps", f"{ccg_writeunique_avg:.9f}"],
        ["ccg_writeback_bw_avg_GBps", f"{ccg_writeback_avg:.9f}"],
        ["ccg_writeevictorvict_bw_avg_GBps", f"{ccg_writeevictorvict_avg:.9f}"],
        ["ccg_writenosnoop_bw_avg_GBps", f"{ccg_writenosnoop_avg:.9f}"],
        ["ccg_writedata_bw_avg_GBps", f"{ccg_writedata_avg:.9f}"],
        ["ccg_dedicated_total_bw_avg_GBps", f"{ccg_dedicated_total_avg:.9f}"],

        ["cbusy00_avg", f"{cbusy00_avg:.9f}"],
        ["cbusy01_avg", f"{cbusy01_avg:.9f}"],
        ["cbusy10_avg", f"{cbusy10_avg:.9f}"],
        ["cbusy11_avg", f"{cbusy11_avg:.9f}"],
        ["cbusy_total_avg", f"{cbusy_total_avg:.9f}"],
        ["cbusy_total_max", f"{cbusy_total_max:.9f}"],
        ["cbusy00_sum", f"{cbusy00_sum:.9f}"],
        ["cbusy01_sum", f"{cbusy01_sum:.9f}"],
        ["cbusy10_sum", f"{cbusy10_sum:.9f}"],
        ["cbusy11_sum", f"{cbusy11_sum:.9f}"],
        ["cbusy_grand_sum", f"{cbusy_grand_sum:.9f}"],
        ["cbusy00_pct", f"{cbusy00_pct:.6f}"],
        ["cbusy01_pct", f"{cbusy01_pct:.6f}"],
        ["cbusy10_pct", f"{cbusy10_pct:.6f}"],
        ["cbusy11_pct", f"{cbusy11_pct:.6f}"],

        ["pocq_occup_class0_avg", f"{pocq_occup_avg:.9f}"],
        ["pocq_retry_class0_avg", f"{pocq_retry_avg:.9f}"],
        ["pocq_occup_class0_pct_avg", f"{pocq_occup_pct_avg:.6f}"],
        ["pocq_occup_class0_pct_max", f"{pocq_occup_pct_max:.6f}"],
        ["pocq_retry_class0_pct_avg", f"{pocq_retry_pct_avg:.6f}"],
        ["pocq_retry_class0_pct_max", f"{pocq_retry_pct_max:.6f}"],

        ["hns_txdat_stall_pct_avg", f"{hns_txdat_stall_pct_avg:.6f}"],
        ["hns_txdat_stall_pct_max", f"{hns_txdat_stall_pct_max:.6f}"],
        ["hns_txrsp_stall_pct_avg", f"{hns_txrsp_stall_pct_avg:.6f}"],
        ["hns_txrsp_stall_pct_max", f"{hns_txrsp_stall_pct_max:.6f}"],
        ["hns_sn_throttle_read_pct_avg", f"{hns_throttle_read_pct_avg:.6f}"],
        ["hns_sn_throttle_read_pct_max", f"{hns_throttle_read_pct_max:.6f}"],
        ["hns_sn_throttle_write_pct_avg", f"{hns_throttle_write_pct_avg:.6f}"],
        ["hns_sn_throttle_write_pct_max", f"{hns_throttle_write_pct_max:.6f}"],

        *[[k, f"{v:.9f}"] for k, v in sorted(dmc_summary_values.items()) if v != 0.0],
        *[[k, f"{v:.9f}"] for k, v in sorted(dmc_chi_summary_values.items()) if v != 0.0],

        ["dmc_phx_cpu_fe_cycles_avg", f"{dmc_phx_cpu_fe_cycles_avg:.9f}"],
        ["dmc_phx_cpu_fe_retry_avg", f"{dmc_phx_cpu_fe_retry_avg:.9f}"],
        ["dmc_phx_remote_fe_cycles_avg", f"{dmc_phx_remote_fe_cycles_avg:.9f}"],
        ["dmc_phx_remote_fe_timeout_credit_avg", f"{dmc_phx_remote_fe_timeout_avg:.9f}"],
        ["dmc_phx_be_buffer_full_avg", f"{dmc_phx_be_buffer_full_avg:.9f}"],
        ["dmc_phx_be_queue_alloc_dealloc_avg", f"{dmc_phx_be_queue_alloc_dealloc_avg:.9f}"],
        ["dmc_phx_be_cycles_avg", f"{dmc_phx_be_cycles_avg:.9f}"],
        ["dmc_phx_be_cmdq_almost_full_avg", f"{dmc_phx_be_cmdq_almost_full_avg:.9f}"],
        ["dmc_phx_be_controller_busy_avg", f"{dmc_phx_be_controller_busy_avg:.9f}"],
        ["dmc_phx_be_port_busy_avg", f"{dmc_phx_be_port_busy_avg:.9f}"],
        ["dmc_phx_be_controller_busy_pct_avg", f"{dmc_phx_be_controller_busy_pct_avg:.6f}"],
        ["dmc_phx_be_controller_busy_pct_max", f"{dmc_phx_be_controller_busy_pct_max:.6f}"],
        ["dmc_phx_be_port_busy_pct_avg", f"{dmc_phx_be_port_busy_pct_avg:.6f}"],
        ["dmc_phx_be_port_busy_pct_max", f"{dmc_phx_be_port_busy_pct_max:.6f}"],
        ["dmc_phx_be_cmdq_almost_full_pct_avg", f"{dmc_phx_be_cmdq_almost_full_pct_avg:.6f}"],
        ["dmc_phx_be_cmdq_almost_full_pct_max", f"{dmc_phx_be_cmdq_almost_full_pct_max:.6f}"],
        ["dmc_phx_fe_retry_pct_avg", f"{dmc_phx_fe_retry_pct_avg:.6f}"],
        ["dmc_phx_fe_retry_pct_max", f"{dmc_phx_fe_retry_pct_max:.6f}"],
        ["dmc_phx_fe_timeout_credit_pct_avg", f"{dmc_phx_fe_timeout_credit_pct_avg:.6f}"],
        ["dmc_phx_fe_timeout_credit_pct_max", f"{dmc_phx_fe_timeout_credit_pct_max:.6f}"],

        ["snf_vs_bw_mem_delta_pct", f"{delta_pct:.6f}"],
        ["result_2pct", result_2pct],
        ["mux_rows", mux_count],
        ["not_counted_rows", nc_count],
    ]

    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value"])
        w.writerows(data)

    return {k: str(v) for k, v in data}

def make_plot(csv_path, png_path):
    try:
        import pandas as pd
        import matplotlib.pyplot as plt
    except Exception as e:
        print()
        print("WARNING: Plot not created.")
        print(f"Reason: {e}")
        print()
        print("Install plotting dependencies with:")
        print("  apt-get update")
        print("  apt install -y python3-pandas python3-matplotlib")
        print()
        print("If apt reports missing packages, run:")
        print("  apt-get update")
        print("  apt install -y --fix-missing python3-pandas python3-matplotlib")
        print()
        return False

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print()
        print("WARNING: Plot not created.")
        print(f"Could not read CSV {csv_path}: {e}")
        print()
        return False

    if df.empty:
        print()
        print("WARNING: Plot not created. timeseries.csv is empty.")
        print()
        return False

    try:
        fig, ax1 = plt.subplots(figsize=(12, 6))
        x = df["time_s"]

        # Shade benchmark window using the same summary rule:
        # first sustained Total SNF BW >= 50 GB/s through the last sustained
        # Total SNF BW >= 50 GB/s. Isolated tail spikes are ignored.
        if "total_snf_bw_GBps" in df.columns:
            samples = list(zip(df["time_s"].tolist(), df["total_snf_bw_GBps"].fillna(0).tolist()))
            active_start, active_end = find_sustained_active_window(samples, threshold=50.0, min_run=5)
            if active_start is not None and active_end is not None:
                ax1.axvspan(active_start, active_end, alpha=0.08, label="benchmark window")
            else:
                activity_cols = [c for c in ["cpu_total_bw_GBps", "total_snf_bw_GBps", "ccg_up_down_bw_GBps"] if c in df.columns]
                if activity_cols:
                    activity = df[activity_cols].max(axis=1)
                    peak = activity.max()
                    if peak > 0:
                        samples = list(zip(df["time_s"].tolist(), activity.tolist()))
                        active_start, active_end = find_sustained_active_window(samples, threshold=peak * 0.05, min_run=5)
                        if active_start is not None and active_end is not None:
                            ax1.axvspan(active_start, active_end, alpha=0.08, label="benchmark window")

        def has_nonzero(col):
            return col in df.columns and (df[col].fillna(0) != 0).any()

        for col, label in [
            ("cpu_total_bw_GBps", "CPU total BW"),
            ("local_snf_bw_GBps", "Local SNF BW"),
            ("remote_snf_bw_GBps", "Remote SNF BW"),
            ("total_snf_bw_GBps", "Total SNF BW"),
            ("ccg_dedicated_total_bw_GBps", "CCG dedicated total BW"),
            ("bw_mem_GBps", "bw_mem"),
        ]:
            if has_nonzero(col):
                ax1.plot(x, df[col], label=label)

        ax1.set_xlabel("Time (s)")
        ax1.set_ylabel("Bandwidth (GB/s)")

        ax2 = ax1.twinx()
        plotted_pct = False
        for col, label in [
            ("slc_miss_access_pct", "SLC miss/access %"),
            ("cbusy00_pct", "CBusy00 %"),
            ("cbusy01_pct", "CBusy01 %"),
            ("cbusy10_pct", "CBusy10 %"),
            ("cbusy11_pct", "CBusy11 %"),
            ("pocq_occup_class0_pct", "POCQ class0 occup %"),
            ("pocq_retry_class0_pct", "POCQ class0 retry %"),
            ("hns_txdat_stall_pct", "HNS txdat stall %"),
            ("hns_txrsp_stall_pct", "HNS txrsp stall %"),
            ("hns_sn_throttle_read_pct", "HNS throttle read %"),
            ("hns_sn_throttle_write_pct", "HNS throttle write %"),
        ]:
            if has_nonzero(col):
                ax2.plot(x, df[col], linestyle="--", label=label)
                plotted_pct = True

        if plotted_pct:
            ax2.set_ylabel("Percent (%)")
            ax2.set_ylim(0, 100)
        else:
            ax2.set_yticks([])
            ax2.set_ylabel("")

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="best")

        plt.title("CPU total + HNS + CCG Interval Bandwidth")
        plt.tight_layout()
        plt.savefig(png_path, dpi=150)
        plt.close(fig)

        print(f"Plot created: {png_path}")
        return True
    except Exception as e:
        print()
        print("WARNING: Plot not created.")
        print(f"Reason while creating plot: {e}")
        print()
        return False



def collect_pass_scores(outdir, spec_root=None):
    """
    Collect per-pass SPEC ratio scores from every perf log.
    This lets us check whether benchmark score varies across CPU/HNS/SLC/CCG passes.
    """
    rows = []

    for p in sorted(Path(outdir).rglob("*.perf.csv")):
        try:
            bw, score, refs = parse_benchmark_metrics_from_file(p)
        except ValueError:
            # Older parser variant may return only bw,score.
            bw, score = parse_benchmark_metrics_from_file(p)
        if score is not None:
            rows.append({
                "pass": str(p.relative_to(outdir)),
                "score": score,
                "source": str(p),
            })

    if not rows:
        try:
            s, src = find_latest_ratio_in_spec_result(spec_root)
        except Exception:
            s, src = None, None
        if s is not None:
            rows.append({
                "pass": "spec_result_latest",
                "score": s,
                "source": src,
            })

    return rows


def write_pass_scores(path, pass_scores):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["pass", "score", "source"])
        w.writeheader()
        for row in pass_scores:
            w.writerow(row)


def parse_run_dir(outdir, ccg_set, spec_root=None, dmc_cpu_local_events=None, dmc_remote_events=None, dmc_slc_events=None):
    outdir = Path(outdir)
    dmc_cpu_local_events = dmc_cpu_local_events or []
    dmc_remote_events = dmc_remote_events or []
    dmc_slc_events = dmc_slc_events or []
    rows = defaultdict(dict)
    mux_all = []
    nc_all = []

    parsed, mux, nc = parse_perf_csv(outdir / "cpu_local.perf.csv", CPU_EVENTS + LOCAL_HNS_EVENTS + dmc_cpu_local_events + dmc_remote_events + dmc_slc_events, "cpu_local")
    merge(rows, parsed)
    mux_all.extend(mux)
    nc_all.extend(nc)

    parsed, mux, nc = parse_perf_csv(outdir / "remote.perf.csv", REMOTE_HNS_EVENTS + dmc_remote_events, "remote")
    merge(rows, parsed)
    mux_all.extend(mux)
    nc_all.extend(nc)

    parsed, mux, nc = parse_perf_csv(outdir / "slc.perf.csv", SLC_EVENTS + dmc_slc_events, "slc")
    merge(rows, parsed)
    mux_all.extend(mux)
    nc_all.extend(nc)

    ccg_dir = outdir / "ccg"
    if ccg_dir.exists():
        for p in sorted(ccg_dir.rglob("*.perf.csv")):
            parsed, mux, nc = parse_perf_csv(
                p,
                SLC_EVENTS + CBUSY_EVENTS + POCQ_EVENTS + HNS_CONGESTION_EVENTS,
                str(p.relative_to(outdir)),
                parse_ccg=True,
            )
            merge(rows, parsed)
            mux_all.extend(mux)
            nc_all.extend(nc)

    ccg_cbusy_dir = outdir / "ccg_cbusy"
    if ccg_cbusy_dir.exists():
        for p in sorted(ccg_cbusy_dir.rglob("*.perf.csv")):
            parsed, mux, nc = parse_perf_csv(
                p,
                CBUSY_EVENTS,
                str(p.relative_to(outdir)),
                parse_ccg=True,
            )
            merge(rows, parsed)
            mux_all.extend(mux)
            nc_all.extend(nc)

    ccg_pocq_dir = outdir / "ccg_pocq"
    if ccg_pocq_dir.exists():
        for p in sorted(ccg_pocq_dir.rglob("*.perf.csv")):
            parsed, mux, nc = parse_perf_csv(
                p,
                POCQ_EVENTS,
                str(p.relative_to(outdir)),
                parse_ccg=True,
            )
            merge(rows, parsed)
            mux_all.extend(mux)
            nc_all.extend(nc)

    ccg_hns_dir = outdir / "ccg_hns_congestion"
    if ccg_hns_dir.exists():
        for p in sorted(ccg_hns_dir.rglob("*.perf.csv")):
            parsed, mux, nc = parse_perf_csv(
                p,
                HNS_CONGESTION_EVENTS,
                str(p.relative_to(outdir)),
                parse_ccg=True,
            )
            merge(rows, parsed)
            mux_all.extend(mux)
            nc_all.extend(nc)

    bw_mem, benchmark_score, score_source = find_benchmark_metrics(outdir, spec_root=spec_root)
    pass_scores = collect_pass_scores(outdir, spec_root=spec_root)
    write_pass_scores(outdir / "pass_scores.csv", pass_scores)

    # Align separate benchmark passes by interval index, then keep only common
    # active steady-state intervals so the plot overlays CPU/HNS/CCG correctly.
    rows = trim_to_common_active_intervals(rows)
    add_derived(rows, bw_mem, benchmark_score)

    ts = outdir / "timeseries.csv"
    write_timeseries(ts, rows)

    with open(outdir / "mux_check.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["label", "time_s", "event_line", "running_pct"])
        w.writerows(mux_all)

    with open(outdir / "not_counted.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["label", "line"])
        w.writerows(nc_all)

    summary = write_summary(outdir / "summary.csv", rows, bw_mem, benchmark_score, score_source, pass_scores, len(mux_all), len(nc_all), ccg_set)
    plot_path = outdir / "bandwidth_timeseries.png"
    plot_ok = make_plot(ts, plot_path)

    return outdir, ts, plot_path, plot_ok, summary

def print_summary(outdir, ts, plot_path, plot_ok, summary):
    print()
    print(f"Results: {outdir}")
    print(f"Timeseries: {ts}")
    print(f"Summary: {Path(outdir) / 'summary.csv'}")
    print(f"Plot: {plot_path}" if plot_ok else "Plot: not created")
    print()
    print("=============== SUMMARY ===============")

    bw_mem_val = float(summary.get("bw_mem_GBps", 0.0))
    spec_ratio = float(summary.get("benchmark_score", 0.0))

    if spec_ratio != 0.0:
        print("Benchmark")
        print(f"  type        : SPEC")
        print(f"  ratio       : {spec_ratio:.3f}")
        print(f"  spread      : {float(summary.get('benchmark_score_delta_pct', 0.0)):.2f}%")
        src = summary.get("benchmark_score_source", "")
        if src:
            print(f"  source      : {src}")
        print()
    elif bw_mem_val != 0.0:
        print("Benchmark")
        print("  type        : bw_mem")
        print(f"  bandwidth   : {bw_mem_val:.3f} GB/s")
        print()

    print("CPU")
    print(f"  read BW     : {float(summary['cpu_read_bw_avg_GBps']):.3f} GB/s")
    print(f"  write BW    : {float(summary['cpu_write_bw_avg_GBps']):.3f} GB/s")
    print(f"  total BW    : {float(summary['cpu_total_bw_avg_GBps']):.3f} GB/s avg / {float(summary.get('cpu_total_bw_max_GBps', 0.0)):.3f} max")
    print()
    print("HNS")
    print(f"  local BW    : {float(summary['local_snf_bw_avg_GBps']):.3f} GB/s")
    print(f"  remote BW   : {float(summary['remote_snf_bw_avg_GBps']):.3f} GB/s")
    print(f"  total BW    : {float(summary['total_snf_bw_avg_GBps']):.3f} GB/s avg / {float(summary.get('total_snf_bw_max_GBps', 0.0)):.3f} max")
    print(f"  local %     : {float(summary['local_pct_avg']):.2f}%")
    print(f"  remote %    : {float(summary['remote_pct_avg']):.2f}%")
    if float(summary.get("hns_cpu_bw_pct", 0.0)) != 0.0:
        print()
        print("CPU/HNS")
        print(f"  CPU total   : {float(summary['cpu_total_bw_avg_GBps']):.3f} GB/s")
        print(f"  HNS total   : {float(summary['total_snf_bw_avg_GBps']):.3f} GB/s")
        print(f"  HNS/CPU BW  : {float(summary['hns_cpu_bw_pct']):.2f}%")
    slc_present = float(summary.get("slc_access_avg", 0.0)) != 0.0
    if slc_present:
        print()
        print("SLC")
        print(f"  access      : {float(summary['slc_access_avg']):.0f} / interval")
        print(f"  miss        : {float(summary['slc_miss_avg']):.0f} / interval")
        print(f"  miss/access : {float(summary['slc_miss_access_pct']):.2f}%")

    ccg_present = float(summary.get("ccg_dedicated_total_bw_avg_GBps", 0.0)) != 0.0
    if ccg_present:
        print()
        print("CCG dedicated")
        print(f"  READ_A      : {float(summary.get('ccg_read_a_bw_avg_GBps', 0.0)):.3f} GB/s")
        print(f"  READ_B      : {float(summary.get('ccg_read_b_bw_avg_GBps', 0.0)):.3f} GB/s")
        print(f"  WRITEUNIQ   : {float(summary.get('ccg_writeunique_bw_avg_GBps', 0.0)):.3f} GB/s")
        print(f"  WRITEBACK   : {float(summary.get('ccg_writeback_bw_avg_GBps', 0.0)):.3f} GB/s")
        print(f"  WRITEEVICT  : {float(summary.get('ccg_writeevictorvict_bw_avg_GBps', 0.0)):.3f} GB/s")
        print(f"  WRITENOSNP  : {float(summary.get('ccg_writenosnoop_bw_avg_GBps', 0.0)):.3f} GB/s")
        print(f"  WRITEDATA   : {float(summary.get('ccg_writedata_bw_avg_GBps', 0.0)):.3f} GB/s")
        print(f"  total       : {float(summary.get('ccg_dedicated_total_bw_avg_GBps', 0.0)):.3f} GB/s")

    cbusy_present = float(summary.get("cbusy_total_avg", 0.0)) != 0.0
    if cbusy_present:
        print()
        print("CBusy")
        print(f"  cbusy00     : {float(summary.get('cbusy00_pct', 0.0)):.2f}%")
        print(f"  cbusy01     : {float(summary.get('cbusy01_pct', 0.0)):.2f}%")
        print(f"  cbusy10     : {float(summary.get('cbusy10_pct', 0.0)):.2f}%")
        print(f"  cbusy11     : {float(summary.get('cbusy11_pct', 0.0)):.2f}%")
    print()
    dmc_phx_present = any(float(summary.get(k, 0.0)) != 0.0 for k in [
        "dmc_phx_cpu_fe_cycles_avg",
        "dmc_phx_cpu_fe_retry_avg",
        "dmc_phx_remote_fe_timeout_credit_avg",
        "dmc_phx_be_buffer_full_avg",
        "dmc_phx_be_queue_alloc_dealloc_avg",
    ])
    dmc_chi_present = any(
        float(summary.get(f"{k}_avg", 0.0)) != 0.0
        for k in [
            "dmc_chi_reqif_transfer",
            "dmc_chi_reqif_rd_ops_total",
            "dmc_chi_reqif_wr_ops_total",
            "dmc_chi_req_xmit_rd_retries",
            "dmc_chi_req_xmit_wr_retries",
        ]
    )
    if dmc_chi_present:
        print()
        print("DMC CHI request mix")
        print(f"  transfer        : {float(summary.get('dmc_chi_reqif_transfer_avg', 0.0)):.0f} avg / {float(summary.get('dmc_chi_reqif_transfer_max', 0.0)):.0f} max")
        print(f"  read ops        : {float(summary.get('dmc_chi_reqif_rd_ops_total_avg', 0.0)):.0f} avg / {float(summary.get('dmc_chi_reqif_rd_ops_total_max', 0.0)):.0f} max")
        print(f"  write ops       : {float(summary.get('dmc_chi_reqif_wr_ops_total_avg', 0.0)):.0f} avg / {float(summary.get('dmc_chi_reqif_wr_ops_total_max', 0.0)):.0f} max")
        print(f"  rd retry        : {float(summary.get('dmc_chi_rd_retry_pct_avg', 0.0)):.3f}% avg / {float(summary.get('dmc_chi_rd_retry_pct_max', 0.0)):.3f}% max")
        print(f"  wr retry        : {float(summary.get('dmc_chi_wr_retry_pct_avg', 0.0)):.3f}% avg / {float(summary.get('dmc_chi_wr_retry_pct_max', 0.0)):.3f}% max")
        print(f"  total retry     : {float(summary.get('dmc_chi_retry_pct_avg', 0.0)):.3f}% avg / {float(summary.get('dmc_chi_retry_pct_max', 0.0)):.3f}% max")
        print(f"  rd/wr mix       : {float(summary.get('dmc_chi_rd_pct_avg', 0.0)):.2f}% read / {float(summary.get('dmc_chi_wr_pct_avg', 0.0)):.2f}% write")

    if dmc_phx_present:
        print()
        print("DMC Phoenix")
        print("  FE")
        print(f"    retry       : {float(summary.get('dmc_phx_fe_retry_pct_avg', 0.0)):.3f}% avg / {float(summary.get('dmc_phx_fe_retry_pct_max', 0.0)):.3f}% max")
        print(f"    timeout/cr  : {float(summary.get('dmc_phx_fe_timeout_credit_pct_avg', 0.0)):.3f}% avg / {float(summary.get('dmc_phx_fe_timeout_credit_pct_max', 0.0)):.3f}% max")
        print("  BE")
        print(f"    ctrl busy   : {float(summary.get('dmc_phx_be_controller_busy_pct_avg', 0.0)):.3f}% avg / {float(summary.get('dmc_phx_be_controller_busy_pct_max', 0.0)):.3f}% max")
        print(f"    port busy   : {float(summary.get('dmc_phx_be_port_busy_pct_avg', 0.0)):.3f}% avg / {float(summary.get('dmc_phx_be_port_busy_pct_max', 0.0)):.3f}% max")
        print(f"    cmdq almost : {float(summary.get('dmc_phx_be_cmdq_almost_full_pct_avg', 0.0)):.3f}% avg / {float(summary.get('dmc_phx_be_cmdq_almost_full_pct_max', 0.0)):.3f}% max")
        print(f"    q alloc/dea : {float(summary.get('dmc_phx_be_queue_alloc_dealloc_avg', 0.0)):.0f} / interval")

    dmc_present = any(
        float(summary.get(f"dmc_{base}_{i}_avg", 0.0)) != 0.0
        for base in ["cpu_local", "remote", "slc"]
        for i in range(64)
    )
    if dmc_present:
        print()
        print("DMC extra events")
        for base, title in [("cpu_local", "CPU/local pass"), ("remote", "Remote pass"), ("slc", "SLC pass")]:
            shown = False
            for i in range(64):
                avg_v = float(summary.get(f"dmc_{base}_{i}_avg", 0.0))
                max_v = float(summary.get(f"dmc_{base}_{i}_max", 0.0))
                if avg_v != 0.0 or max_v != 0.0:
                    if not shown:
                        print(f"  {title}:")
                        shown = True
                    print(f"    event{i:<2d}: {avg_v:.0f} avg / {max_v:.0f} max")

    hns_cong_present = any(
        float(summary.get(k, 0.0)) != 0.0
        for k in [
            "hns_txdat_stall_pct_avg",
            "hns_txrsp_stall_pct_avg",
            "hns_sn_throttle_read_pct_avg",
            "hns_sn_throttle_write_pct_avg",
        ]
    )
    if hns_cong_present:
        print()
        print("HNS congestion")
        print(f"  txdat stall     : {float(summary.get('hns_txdat_stall_pct_avg', 0.0)):.2f}% avg / {float(summary.get('hns_txdat_stall_pct_max', 0.0)):.2f}% max")
        print(f"  txrsp stall     : {float(summary.get('hns_txrsp_stall_pct_avg', 0.0)):.2f}% avg / {float(summary.get('hns_txrsp_stall_pct_max', 0.0)):.2f}% max")
        print(f"  throttle read   : {float(summary.get('hns_sn_throttle_read_pct_avg', 0.0)):.2f}% avg / {float(summary.get('hns_sn_throttle_read_pct_max', 0.0)):.2f}% max")
        print(f"  throttle write  : {float(summary.get('hns_sn_throttle_write_pct_avg', 0.0)):.2f}% avg / {float(summary.get('hns_sn_throttle_write_pct_max', 0.0)):.2f}% max")

    pocq_present = float(summary.get("pocq_occup_class0_avg", 0.0)) != 0.0
    if pocq_present:
        print()
        print("POCQ")
        print(f"  class0 occup: {float(summary.get('pocq_occup_class0_pct_avg', 0.0)):.2f}% avg / {float(summary.get('pocq_occup_class0_pct_max', 0.0)):.2f}% max")
        print(f"  class0 retry: {float(summary.get('pocq_retry_class0_pct_avg', 0.0)):.2f}% avg / {float(summary.get('pocq_retry_class0_pct_max', 0.0)):.2f}% max")
    print()
    print("Summary window")
    print(f"  active start: {summary.get('active_start_s', '')} s")
    print(f"  active end  : {summary.get('active_end_s', '')} s")
    print(f"  intervals   : {summary.get('active_intervals', '')}")
    print()
    print("Quality")
    print(f"  mux rows    : {summary['mux_rows']}")
    print(f"  not counted : {summary['not_counted_rows']}")

def main():
    ap = argparse.ArgumentParser(description="Simple CPU + HNS + grouped CCG interval collector")
    ap.add_argument("--cmd", help='benchmark command in quotes; required unless --parse-only is used')
    ap.add_argument("-o", "--outdir", default=None, help="output directory")
    ap.add_argument("--no-cleanup", action="store_true", help="do not kill stale perf stat first")

    ap.add_argument("--ccg-set", default="single",
                    choices=["none", "single", "read", "write", "readwrite", "all"],
                    help="CCG opcode set. Default: single")
    ap.add_argument("--opcode", default="READ_AR1_A", help="CCG opcode for --ccg-set single")
    ap.add_argument("--ccg-both-cmns", action="store_true", help="collect CCG on arm_cmn_0 and arm_cmn_1")
    ap.add_argument("--ccg-cbusy", action="store_true", help="run validated CCG+CBusy read passes")
    ap.add_argument("--ccg-pocq", action="store_true", help="run CCG+POCQ class0 occupancy/retry write passes")
    ap.add_argument("--ccg-hns-txdat-stall", action="store_true", help="run full WRITEEVICTORVICT CCG combined-node sweep with hns_txdat_stall on first pass")
    ap.add_argument("--ccg-hns-txrsp-stall", action="store_true", help="run full WRITENOSNOOPFULL CCG combined-node sweep with hns_txrsp_stall on first pass")
    ap.add_argument("--ccg-hns-throttle-read", action="store_true", help="run full WRITEDATAFULL CCG combined-node sweep with hns_sn_throttle_read on first pass")
    ap.add_argument("--ccg-hns-throttle-write", action="store_true", help="run full WRITEEVICTORVICT CCG combined-node sweep with hns_sn_throttle_write on first pass")
    ap.add_argument("--dmc-cpu-local-events", default="", help="comma-separated DMC perf events to add to CPU+LOCAL HNS pass")
    ap.add_argument("--dmc-remote-events", default="", help="comma-separated DMC perf events to add to REMOTE HNS pass")
    ap.add_argument("--dmc-slc-events", default="", help="comma-separated DMC perf events to add to SLC pass")
    ap.add_argument("--dmc-phx-all", action="store_true", help="add built-in DMC-Phoenix FE/BE events across all arm_cspmu_mc_0..47 ports/channels into CPU+LOCAL base pass")
    ap.add_argument("--dmc-phx-chi-reqmix", action="store_true", help="add DMC-Phoenix CHI request mix/retry events to CPU+LOCAL base pass")

    ap.add_argument("--parse-only", default=None, help="existing run directory to reparse without running perf")
    ap.add_argument("--spec-root", default=None, help="SPEC root used to find result/ files for ratio= parsing")
    args = ap.parse_args()

    dmc_cpu_local_events = parse_extra_perf_events(args.dmc_cpu_local_events, "dmc_cpu_local")
    dmc_remote_events = parse_extra_perf_events(args.dmc_remote_events, "dmc_remote")
    dmc_slc_events = parse_extra_perf_events(args.dmc_slc_events, "dmc_slc")

    if args.dmc_phx_all:
        # DMC/MC PMUs are independent system PMUs, so keep the full DMC Phoenix
        # collection in the first CPU+LOCAL pass instead of spreading it across
        # remote/SLC passes. This keeps DMC aligned to one benchmark execution.
        dmc_cpu_local_events += build_dmc_phx_all_events("dmc_phx_cpu_fe_cycles", DMC_PHX_FE_CYCLES)
        dmc_cpu_local_events += build_dmc_phx_all_events("dmc_phx_cpu_fe_retry", DMC_PHX_FE_RETRY)
        dmc_cpu_local_events += build_dmc_phx_all_events("dmc_phx_remote_fe_cycles", DMC_PHX_FE_CYCLES)
        dmc_cpu_local_events += build_dmc_phx_all_events("dmc_phx_remote_fe_timeout_credit", DMC_PHX_FE_TIMEOUT_CREDIT)
        dmc_cpu_local_events += build_dmc_phx_all_events("dmc_phx_be_buffer_full", DMC_PHX_BE_BUFFER_FULL)
        dmc_cpu_local_events += build_dmc_phx_all_events("dmc_phx_be_queue_alloc_dealloc", DMC_PHX_BE_QUEUE_ALLOC_DEALLOC)
        dmc_cpu_local_events += build_dmc_phx_all_events("dmc_phx_be_cycles", DMC_PHX_BE_CYCLES)
        dmc_cpu_local_events += build_dmc_phx_all_events("dmc_phx_be_cmdq_almost_full", DMC_PHX_BE_CMDQ_ALMOST_FULL)
        dmc_cpu_local_events += build_dmc_phx_all_events("dmc_phx_be_controller_busy", DMC_PHX_BE_CONTROLLER_BUSY)
        dmc_cpu_local_events += build_dmc_phx_all_events("dmc_phx_be_port_busy", DMC_PHX_BE_PORT_BUSY)
        dmc_cpu_local_events += build_dmc_phx_chi_req_events()

    if args.dmc_phx_chi_reqmix and not args.dmc_phx_all:
        dmc_cpu_local_events += build_dmc_phx_chi_req_events()

    if args.parse_only:
        spec_root = args.spec_root or infer_spec_root_from_cmd(args.cmd)
        outdir, ts, plot_path, plot_ok, summary = parse_run_dir(args.parse_only, args.ccg_set, spec_root=spec_root, dmc_cpu_local_events=dmc_cpu_local_events, dmc_remote_events=dmc_remote_events, dmc_slc_events=dmc_slc_events)
        print_summary(outdir, ts, plot_path, plot_ok, summary)
        return

    if not args.cmd:
        raise SystemExit("--cmd is required unless --parse-only is used")

    if not args.no_cleanup:
        subprocess.run(["pkill", "-9", "-f", r"^perf stat"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    outdir = Path(args.outdir or f"cpu_hns_ccg_interval_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    outdir.mkdir(parents=True, exist_ok=True)

    if dmc_cpu_local_events:
        print(f"[DMC] CPU+LOCAL HNS pass: {len(dmc_cpu_local_events)} extra event(s)", flush=True)
    if dmc_remote_events:
        print(f"[DMC] REMOTE HNS pass: {len(dmc_remote_events)} extra event(s)", flush=True)
    if dmc_slc_events:
        print(f"[DMC] SLC pass: {len(dmc_slc_events)} extra event(s)", flush=True)

    print("[CPU+LOCAL HNS]", flush=True)
    cpu_local_log = outdir / "cpu_local.perf.csv"
    rc1 = run_perf(CPU_EVENTS + LOCAL_HNS_EVENTS + dmc_cpu_local_events, args.cmd, cpu_local_log)
    if rc1 != 0:
        print(f"WARNING: CPU/local pass returned {rc1}", flush=True)

    print("[REMOTE HNS]", flush=True)
    remote_log = outdir / "remote.perf.csv"
    rc2 = run_perf(REMOTE_HNS_EVENTS + dmc_remote_events, args.cmd, remote_log)
    if rc2 != 0:
        print(f"WARNING: remote pass returned {rc2}", flush=True)

    # Dedicated-pass CCG model:
    #   --ccg-cbusy covers READ_A/READ_B
    #   --ccg-pocq covers WRITEUNIQUEFULL/WRITEBACKFULL
    #   --ccg-hns-* covers remaining write opcodes with congestion counters
    #
    # The old --ccg-set all/single/read/write flow is intentionally not run.
    print("[SLC HIT/MISS]", flush=True)
    slc_log = outdir / "slc.perf.csv"
    rc3 = run_perf(SLC_EVENTS + dmc_slc_events, args.cmd, slc_log)
    if rc3 != 0:
        print(f"WARNING: SLC pass returned {rc3}", flush=True)

    if args.ccg_cbusy:
        cbusy_dir = outdir / "ccg_cbusy"
        cbusy_dir.mkdir(exist_ok=True)

        for idx, (group_name, nodes) in enumerate(COMBINED_CCG_NODE_GROUPS):
            extras = []
            if idx == 0:
                extras = [
                    ("cmn0_cbusy00", "arm_cmn_0/hns_cbusy00_all/"),
                    ("cmn0_cbusy01", "arm_cmn_0/hns_cbusy01_all/"),
                    ("cmn1_cbusy00", "arm_cmn_1/hns_cbusy00_all/"),
                    ("cmn1_cbusy01", "arm_cmn_1/hns_cbusy01_all/"),
                ]
                print(f"[CCG+CBUSY] READ_A cbusy00/01 {group_name}", flush=True)
            else:
                print(f"[CCG] READ_A {group_name}", flush=True)
            log = cbusy_dir / f"read_a_{group_name}.perf.csv"
            events = build_full_ccg_nodepair_events("0x800013000", nodes, "READ_A", extras)
            rc = run_perf(events, args.cmd, log)
            if rc != 0:
                print(f"WARNING: CCG READ_A {group_name} returned {rc}", flush=True)

        for idx, (group_name, nodes) in enumerate(COMBINED_CCG_NODE_GROUPS):
            extras = []
            if idx == 0:
                extras = [
                    ("cmn0_cbusy10", "arm_cmn_0/hns_cbusy10_all/"),
                    ("cmn0_cbusy11", "arm_cmn_0/hns_cbusy11_all/"),
                    ("cmn1_cbusy10", "arm_cmn_1/hns_cbusy10_all/"),
                    ("cmn1_cbusy11", "arm_cmn_1/hns_cbusy11_all/"),
                ]
                print(f"[CCG+CBUSY] READ_B cbusy10/11 {group_name}", flush=True)
            else:
                print(f"[CCG] READ_B {group_name}", flush=True)
            log = cbusy_dir / f"read_b_{group_name}.perf.csv"
            events = build_full_ccg_nodepair_events("0x800003800", nodes, "READ_B", extras)
            rc = run_perf(events, args.cmd, log)
            if rc != 0:
                print(f"WARNING: CCG READ_B {group_name} returned {rc}", flush=True)

    if args.ccg_pocq:
        pocq_dir = outdir / "ccg_pocq"
        pocq_dir.mkdir(exist_ok=True)

        for idx, (group_name, nodes) in enumerate(COMBINED_CCG_NODE_GROUPS):
            extras = list(POCQ_EVENTS) if idx == 0 else []
            if idx == 0:
                print(f"[CCG+POCQ] WRITEUNIQUEFULL_AR1 class0 {group_name}", flush=True)
            else:
                print(f"[CCG] WRITEUNIQUEFULL_AR1 {group_name}", flush=True)
            log = pocq_dir / f"writeunique_{group_name}.perf.csv"
            events = build_full_ccg_nodepair_events("0x80000a800", nodes, "WRITEUNIQUEFULL_AR1", extras)
            rc = run_perf(events, args.cmd, log)
            if rc != 0:
                print(f"WARNING: CCG WRITEUNIQUEFULL_AR1 {group_name} returned {rc}", flush=True)

        for idx, (group_name, nodes) in enumerate(COMBINED_CCG_NODE_GROUPS):
            extras = list(POCQ_EVENTS) if idx == 0 else []
            if idx == 0:
                print(f"[CCG+POCQ] WRITEBACKFULL_AR1 class0 {group_name}", flush=True)
            else:
                print(f"[CCG] WRITEBACKFULL_AR1 {group_name}", flush=True)
            log = pocq_dir / f"writeback_{group_name}.perf.csv"
            events = build_full_ccg_nodepair_events("0x80000d800", nodes, "WRITEBACKFULL_AR1", extras)
            rc = run_perf(events, args.cmd, log)
            if rc != 0:
                print(f"WARNING: CCG WRITEBACKFULL_AR1 {group_name} returned {rc}", flush=True)

    congestion_passes = []
    if args.ccg_hns_txdat_stall:
        congestion_passes.append(("writeevictorvict", "0x80000c800", "hns_txdat_stall", "WRITEEVICTORVICT_AR1"))
    if args.ccg_hns_txrsp_stall:
        congestion_passes.append(("writenosnoopfull", "0x80000b800", "hns_txrsp_stall", "WRITENOSNOOPFULL_AR1"))
    if args.ccg_hns_throttle_read:
        congestion_passes.append(("writedatafull", "0x800011800", "hns_sn_throttle_read", "WRITEDATAFULL_AR1"))
    if args.ccg_hns_throttle_write:
        congestion_passes.append(("writeevictorvict_throttle_write", "0x80000c800", "hns_sn_throttle_write", "WRITEEVICTORVICT_AR1"))

    if congestion_passes:
        cong_dir = outdir / "ccg_hns_congestion"
        cong_dir.mkdir(exist_ok=True)

        for fname, op, hns_event, label in congestion_passes:
            for idx, (group_name, nodes) in enumerate(COMBINED_CCG_NODE_GROUPS):
                extras = []
                if idx == 0:
                    extras = [
                        (f"cmn0_{hns_event}", f"arm_cmn_0/{hns_event}/"),
                        (f"cmn1_{hns_event}", f"arm_cmn_1/{hns_event}/"),
                    ]
                    print(f"[CCG+HNS] {label} {hns_event} {group_name}", flush=True)
                else:
                    print(f"[CCG] {label} {group_name}", flush=True)

                log = cong_dir / f"{fname}_{group_name}.perf.csv"
                events = build_full_ccg_nodepair_events(op, nodes, label, extras)
                rc = run_perf(events, args.cmd, log)
                if rc != 0:
                    print(f"WARNING: CCG+HNS {label} {group_name} returned {rc}", flush=True)

    spec_root = args.spec_root or infer_spec_root_from_cmd(args.cmd)
    outdir, ts, plot_path, plot_ok, summary = parse_run_dir(outdir, args.ccg_set, spec_root=spec_root, dmc_cpu_local_events=dmc_cpu_local_events, dmc_remote_events=dmc_remote_events, dmc_slc_events=dmc_slc_events)
    print_summary(outdir, ts, plot_path, plot_ok, summary)

if __name__ == "__main__":
    main()

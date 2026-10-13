"""Parse a PowerCenter repository export (XML) into a simple model.

Handles SOURCE/TARGET, MAPPING/TRANSFORMATION/CONNECTOR (including Router,
Joiner, Union, Sorter, Rank, Normalizer, SQL), WORKFLOW/TASK/WORKFLOWLINK/
WORKFLOWVARIABLE, SESSION/CONFIG, MAPPLET/WORKLET and the parameter file.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Port:
    name: str
    porttype: str = ""
    expression: str = ""
    datatype: str = "string"
    group: str = ""


@dataclass
class Transform:
    name: str
    type: str
    fields: list = field(default_factory=list)     # list[Port]
    attrs: dict = field(default_factory=dict)
    groups: list = field(default_factory=list)      # Router groups: [(name, condition)]


@dataclass
class Mapping:
    name: str
    transforms: list = field(default_factory=list)
    connectors: list = field(default_factory=list)  # [(from_inst, to_inst, field)]


@dataclass
class FieldDef:
    name: str
    datatype: str = "string"
    precision: str = "0"
    scale: str = "0"


@dataclass
class Dataset:
    name: str
    kind: str            # SOURCE | TARGET
    dbtype: str = ""
    fields: list = field(default_factory=list)


@dataclass
class Task:
    name: str
    type: str
    attrs: dict = field(default_factory=dict)


@dataclass
class Workflow:
    name: str
    schedule: str = ""
    tasks: list = field(default_factory=list)
    links: list = field(default_factory=list)       # [(from, to)]
    variables: list = field(default_factory=list)   # [(name, value, type)]


@dataclass
class Repository:
    datasets: list = field(default_factory=list)
    mappings: list = field(default_factory=list)
    workflows: list = field(default_factory=list)
    sessions: list = field(default_factory=list)
    mapplets: list = field(default_factory=list)
    worklets: list = field(default_factory=list)
    parameters: dict = field(default_factory=dict)


# ------------------------------------------------------------------ parsing
def _attrs(el) -> dict:
    return {a.get("NAME"): a.get("VALUE") for a in el.iter("TABLEATTRIBUTE")}


def parse_mapping(path: Path) -> Mapping:
    root = ET.parse(path).getroot()
    m = next(root.iter("MAPPING"))
    mapping = Mapping(name=m.get("NAME"))
    for tr in m.iter("TRANSFORMATION"):
        groups = []
        for g in tr.iter("GROUP"):
            ga = {a.get("NAME"): a.get("VALUE") for a in g.iter("GROUPATTRIBUTE")}
            groups.append((g.get("NAME"), ga.get("Filter Condition", "")))
        fields = [Port(name=f.get("NAME"), porttype=f.get("PORTTYPE", ""),
                       expression=f.get("EXPRESSION", ""), datatype=f.get("DATATYPE", "string"),
                       group=f.get("GROUP", ""))
                  for f in tr.iter("TRANSFORMFIELD")]
        mapping.transforms.append(Transform(name=tr.get("NAME"), type=tr.get("TYPE"),
                                            fields=fields, attrs=_attrs(tr), groups=groups))
    mapping.connectors = [(c.get("FROMINSTANCE"), c.get("TOINSTANCE"), c.get("FROMFIELD"))
                          for c in m.iter("CONNECTOR")]
    return mapping


def parse_datasets(path: Path) -> list[Dataset]:
    root = ET.parse(path).getroot()
    out = []
    for tag in ("SOURCE", "TARGET"):
        for el in root.iter(tag):
            fields = []
            for f in el.iter("SOURCEFIELD" if tag == "SOURCE" else "TARGETFIELD"):
                fields.append(FieldDef(f.get("NAME"), f.get("DATATYPE", "string"),
                                       f.get("PRECISION", "0"), f.get("SCALE", "0")))
            out.append(Dataset(el.get("NAME"), tag, el.get("DATABASETYPE", ""), fields))
    return out


def parse_workflow(path: Path) -> Workflow:
    root = ET.parse(path).getroot()
    wf = next(root.iter("WORKFLOW"))
    w = Workflow(name=wf.get("NAME"), schedule=wf.get("SCHEDULE", ""))
    for t in wf.iter("TASK"):
        w.tasks.append(Task(t.get("NAME"), t.get("TYPE"), _attrs(t)))
    w.links = [(l.get("FROMTASK"), l.get("TOTASK")) for l in wf.iter("WORKFLOWLINK")]
    w.variables = [(v.get("NAME"), v.get("VALUE"), v.get("DATATYPE"))
                   for v in wf.iter("WORKFLOWVARIABLE")]
    return w


def parse_reusable(path: Path):
    root = ET.parse(path).getroot()
    mapplets = [m.get("NAME") for m in root.iter("MAPPLET")]
    worklets = [w.get("NAME") for w in root.iter("WORKLET")]
    return mapplets, worklets


def parse_sessions(path: Path):
    root = ET.parse(path).getroot()
    sessions = [(s.get("NAME"), s.get("MAPPINGNAME")) for s in root.iter("SESSION")]
    configs = [(c.get("NAME"), {a.get("NAME"): a.get("VALUE") for a in c.iter("ATTRIBUTE")})
               for c in root.iter("CONFIG")]
    return sessions, configs


def parse_parameters(path: Path) -> dict:
    params = {"global": {}, "sections": {}}
    section = "Global"
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1]
            params["sections"].setdefault(section, {})
            continue
        if "=" in line:
            k, _, v = line.partition("=")
            if section == "Global":
                params["global"][k.strip()] = v.strip()
            else:
                params["sections"].setdefault(section, {})[k.strip()] = v.strip()
    return params


def load_repository(root: Path) -> Repository:
    legacy = root / "legacy"
    repo = Repository()
    for p in (legacy / "sources").glob("*.xml"):
        repo.datasets += parse_datasets(p)
    for p in sorted((legacy / "mappings").glob("*.xml")):
        repo.mappings.append(parse_mapping(p))
    for p in (legacy / "workflows").glob("*.xml"):
        repo.workflows.append(parse_workflow(p))
    for p in (legacy / "mapplets").glob("*.xml"):
        m, w = parse_reusable(p)
        repo.mapplets += m
        repo.worklets += w
    for p in (legacy / "sessions").glob("*.xml"):
        s, c = parse_sessions(p)
        repo.sessions += s
    prm = legacy / "parameters" / "bank_params.prm"
    if prm.exists():
        repo.parameters = parse_parameters(prm)
    return repo


# ------------------------------------------------------------------ helpers
def topo_order(mapping: Mapping) -> list[Transform]:
    names = [t.name for t in mapping.transforms]
    indeg = {n: 0 for n in names}
    adj = {n: [] for n in names}
    for frm, to, _ in mapping.connectors:
        if frm in indeg and to in adj:
            adj[frm].append(to)
            indeg[to] += 1
    order, queue = [], [n for n in names if indeg[n] == 0]
    while queue:
        n = queue.pop(0)
        order.append(n)
        for m in adj[n]:
            indeg[m] -= 1
            if indeg[m] == 0:
                queue.append(m)
    by_name = {t.name: t for t in mapping.transforms}
    return [by_name[n] for n in order]


def source_table_name(cobol_like: str) -> str:
    return re.sub(r"[^a-z0-9_]", "_", cobol_like.lower())

"""Generate a workflow overview PlantUML file from flow-control YAML."""

from __future__ import annotations

import argparse
import re
import textwrap
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

SOURCE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SOURCE_DIR.parent
DEFAULT_FLOW_CONTROL_PATH = SOURCE_DIR / "workflow_flow_control.yml"
DEFAULT_OUTPUT_PATH = SOURCE_DIR / "_generated" / "workflow_overview_diagram.uml"
HEADING_UNDERLINE_CHARS = set("=-`:'\"~^_*+#<>.")
SPECIAL_TITLE_TOKENS = {
    "fmv": "FMV",
    "mls": "MLS",
}


class DiagramConfigError(ValueError):
    """Raised when workflow_flow_control.yml has invalid or unresolved references."""


@dataclass
class WorkflowSpec:
    id: str
    dir: str | None = None
    index_rst: str | None = None
    title: str | None = None
    index_path: Path | None = None
    steps: list[str] = field(default_factory=list)


@dataclass
class ActivityGroupSpec:
    id: str
    workflow: str | None = None
    dir: str | None = None
    index_rst: str | None = None
    title: str | None = None
    index_path: Path | None = None
    steps: list[str] = field(default_factory=list)


def maybe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def humanize(token: str) -> str:
    parts = [part for part in re.split(r"[/_]+", token) if part]
    if not parts:
        return token

    rendered: list[str] = []
    for part in parts:
        lower = part.lower()
        if lower in SPECIAL_TITLE_TOKENS:
            rendered.append(SPECIAL_TITLE_TOKENS[lower])
            continue
        if part.isdigit():
            rendered.append(part)
            continue
        rendered.append(part[0].upper() + part[1:])
    return " ".join(rendered)


def wrap_label(label: str, width: int = 22) -> str:
    compact = " ".join(label.split())
    wrapped = textwrap.wrap(compact, width=width) or [compact]
    return "\\n".join(line.replace('"', '\\"') for line in wrapped)


def safe_alias(prefix: str, raw: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", raw)
    return f"n_{prefix}_{cleaned}".strip("_")


def parse_title_from_rst(path: Path) -> str | None:
    if not path.exists():
        return None

    lines = path.read_text(encoding="utf-8").splitlines()
    for index, line in enumerate(lines[:-1]):
        title = line.strip()
        if not title:
            continue
        underline = lines[index + 1].strip()
        if not underline:
            continue
        if len(underline) < len(title):
            continue
        if len(set(underline)) != 1:
            continue
        if underline[0] not in HEADING_UNDERLINE_CHARS:
            continue
        return title
    return None


def extract_toctree_target(line: str) -> str | None:
    entry = line.strip()
    if not entry:
        return None

    if "<" in entry and entry.endswith(">"):
        left = entry.rfind("<")
        entry = entry[left + 1 : -1].strip()

    if not entry or "://" in entry:
        return None

    if entry.startswith("/"):
        entry = entry[1:]
    if entry.endswith(".rst"):
        entry = entry[:-4]
    return entry or None


def parse_toctree_targets(index_path: Path) -> list[str]:
    lines = index_path.read_text(encoding="utf-8").splitlines()
    targets: list[str] = []
    cursor = 0

    while cursor < len(lines):
        row = lines[cursor]
        stripped = row.lstrip()
        if not stripped.startswith(".. toctree::"):
            cursor += 1
            continue

        base_indent = len(row) - len(stripped)
        cursor += 1
        while cursor < len(lines):
            candidate = lines[cursor]
            candidate_stripped = candidate.strip()
            candidate_indent = len(candidate) - len(candidate.lstrip(" "))

            if not candidate_stripped:
                cursor += 1
                continue
            if candidate_indent <= base_indent:
                break
            if candidate_stripped.startswith(":"):
                cursor += 1
                continue

            target = extract_toctree_target(candidate_stripped)
            if target:
                targets.append(target)
            cursor += 1

    return targets


def infer_steps_from_index(index_path: Path, expected_prefix: str | None) -> list[str]:
    steps: list[str] = []
    seen: set[str] = set()

    for target in parse_toctree_targets(index_path):
        rel_target = target[:-6] if target.endswith("/index") else target
        candidate_dir = (index_path.parent / rel_target).resolve()
        candidate_index = candidate_dir / "index.rst"

        if not candidate_dir.is_dir() or not candidate_index.exists():
            continue

        try:
            step_ref = candidate_dir.relative_to(SOURCE_DIR).as_posix()
        except ValueError:
            continue

        if expected_prefix and not step_ref.startswith(f"{expected_prefix}/"):
            continue

        if step_ref in seen:
            continue
        seen.add(step_ref)
        steps.append(step_ref)

    return steps


def resolve_index_path(dir_path: str | None, index_rst: str | None, entity_name: str) -> Path | None:
    if index_rst:
        resolved = (SOURCE_DIR / index_rst).resolve()
    elif dir_path:
        resolved = (SOURCE_DIR / dir_path / "index.rst").resolve()
    else:
        return None

    if not resolved.exists():
        raise DiagramConfigError(f"{entity_name} references missing index file: {resolved}")
    return resolved


def normalize_workflows(raw: Any) -> OrderedDict[str, WorkflowSpec]:
    if raw is None:
        raw = []
    if not isinstance(raw, list):
        raise DiagramConfigError("'workflows' must be a list.")

    workflows: OrderedDict[str, WorkflowSpec] = OrderedDict()
    for entry in raw:
        if isinstance(entry, str):
            workflow_id = maybe_text(entry)
            if not workflow_id:
                raise DiagramConfigError("Workflow id cannot be empty.")
            spec = WorkflowSpec(id=workflow_id, dir=workflow_id)
        elif isinstance(entry, dict):
            workflow_id = maybe_text(entry.get("id")) or maybe_text(entry.get("dir"))
            if not workflow_id:
                raise DiagramConfigError("Workflow entries must define 'id' or 'dir'.")

            has_dir_key = "dir" in entry
            workflow_dir = maybe_text(entry.get("dir")) if has_dir_key else None
            index_rst = maybe_text(entry.get("index_rst"))
            title = maybe_text(entry.get("title"))

            # Backward-compatible default: when nothing is specified, infer directory from id.
            if not has_dir_key and index_rst is None:
                workflow_dir = workflow_id

            spec = WorkflowSpec(
                id=workflow_id,
                dir=workflow_dir,
                index_rst=index_rst,
                title=title,
            )
        else:
            raise DiagramConfigError("Each workflow entry must be a string or mapping.")

        if spec.id in workflows:
            raise DiagramConfigError(f"Duplicate workflow id: {spec.id}")
        workflows[spec.id] = spec

    return workflows


def normalize_activity_groups(raw: Any) -> OrderedDict[str, ActivityGroupSpec]:
    if raw is None:
        raw = []
    if not isinstance(raw, list):
        raise DiagramConfigError("'activity_groups' must be a list.")

    groups: OrderedDict[str, ActivityGroupSpec] = OrderedDict()
    for entry in raw:
        if isinstance(entry, str):
            group_id = maybe_text(entry)
            if not group_id:
                raise DiagramConfigError("Activity group id cannot be empty.")
            spec = ActivityGroupSpec(id=group_id)
        elif isinstance(entry, dict):
            group_id = maybe_text(entry.get("id")) or maybe_text(entry.get("dir"))
            if not group_id:
                raise DiagramConfigError("Activity-group entries must define 'id' or 'dir'.")

            has_dir_key = "dir" in entry
            group_dir = maybe_text(entry.get("dir")) if has_dir_key else None
            index_rst = maybe_text(entry.get("index_rst"))
            workflow = maybe_text(entry.get("workflow"))
            title = maybe_text(entry.get("title"))

            spec = ActivityGroupSpec(
                id=group_id,
                workflow=workflow,
                dir=group_dir,
                index_rst=index_rst,
                title=title,
            )
        else:
            raise DiagramConfigError("Each activity-group entry must be a string or mapping.")

        if spec.id in groups:
            raise DiagramConfigError(f"Duplicate activity-group id: {spec.id}")
        groups[spec.id] = spec

    return groups


def infer_group_workflow(group: ActivityGroupSpec, workflows: OrderedDict[str, WorkflowSpec]) -> str | None:
    if group.workflow:
        return group.workflow

    path_hint = group.dir or group.index_rst
    if not path_hint:
        return None

    matches: list[str] = []
    for workflow in workflows.values():
        if not workflow.dir:
            continue
        if path_hint == workflow.dir or path_hint.startswith(f"{workflow.dir}/"):
            matches.append(workflow.id)

    if len(matches) == 1:
        return matches[0]
    return None


def ordered_unique(items: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        output.append(item)
    return output


class RefResolver:
    def __init__(
        self,
        workflows: OrderedDict[str, WorkflowSpec],
        groups: OrderedDict[str, ActivityGroupSpec],
        step_refs: list[str],
    ) -> None:
        self.workflow_keys = {workflow_id: f"workflow:{workflow_id}" for workflow_id in workflows}
        self.workflow_dir_keys: dict[str, str] = {}
        self.workflow_dir_by_id: dict[str, str] = {}
        for workflow in workflows.values():
            if workflow.dir:
                self.workflow_dir_keys[workflow.dir] = f"workflow:{workflow.id}"
                self.workflow_dir_by_id[workflow.id] = workflow.dir

        self.group_keys = {group_id: f"group:{group_id}" for group_id in groups}
        self.step_full_keys = {step_ref: f"step:{step_ref}" for step_ref in step_refs}
        self.step_short_keys: dict[str, str | None] = {}
        for step_ref in step_refs:
            short = step_ref.split("/")[-1]
            previous = self.step_short_keys.get(short)
            if previous is None and short in self.step_short_keys:
                continue
            if previous is not None and previous != f"step:{step_ref}":
                self.step_short_keys[short] = None
                continue
            self.step_short_keys[short] = f"step:{step_ref}"

    def resolve(self, ref: str) -> str | None:
        token = ref.strip()
        if not token:
            return None

        if token.startswith("workflow:") or token.startswith("group:") or token.startswith("step:"):
            if token in self.workflow_keys.values() or token in self.group_keys.values() or token in self.step_full_keys.values():
                return token
            return None

        if token in self.workflow_keys:
            return self.workflow_keys[token]
        if token in self.workflow_dir_keys:
            return self.workflow_dir_keys[token]
        if token in self.group_keys:
            return self.group_keys[token]
        if token in self.step_full_keys:
            return self.step_full_keys[token]

        short_match = self.step_short_keys.get(token)
        if short_match:
            return short_match

        if "/" in token:
            first, remainder = token.split("/", 1)
            workflow_dir = self.workflow_dir_by_id.get(first)
            if workflow_dir:
                candidate = f"{workflow_dir}/{remainder}"
                if candidate in self.step_full_keys:
                    return self.step_full_keys[candidate]

        return None


def build_model(flow_control: dict[str, Any]) -> tuple[
    OrderedDict[str, WorkflowSpec],
    OrderedDict[str, ActivityGroupSpec],
    dict[str, list[str]],
    list[str],
    RefResolver,
]:
    workflows = normalize_workflows(flow_control.get("workflows"))
    groups = normalize_activity_groups(flow_control.get("activity_groups"))

    for workflow in workflows.values():
        workflow.index_path = resolve_index_path(
            dir_path=workflow.dir,
            index_rst=workflow.index_rst,
            entity_name=f"workflow '{workflow.id}'",
        )
        expected_prefix = workflow.dir
        if workflow.index_path:
            workflow.steps = infer_steps_from_index(workflow.index_path, expected_prefix=expected_prefix)

    for group in groups.values():
        inferred_workflow = infer_group_workflow(group, workflows)
        if group.workflow is None:
            group.workflow = inferred_workflow
        if group.workflow and group.workflow not in workflows:
            raise DiagramConfigError(
                f"Activity group '{group.id}' references unknown workflow '{group.workflow}'."
            )

        if group.index_rst or group.dir:
            group.index_path = resolve_index_path(
                dir_path=group.dir,
                index_rst=group.index_rst,
                entity_name=f"activity group '{group.id}'",
            )
        elif group.workflow:
            group.index_path = workflows[group.workflow].index_path
        else:
            group.index_path = None

        expected_prefix = None
        if group.workflow and workflows[group.workflow].dir:
            expected_prefix = workflows[group.workflow].dir
        if group.index_path:
            group.steps = infer_steps_from_index(group.index_path, expected_prefix=expected_prefix)

    step_owner_group: dict[str, str] = {}
    for group in groups.values():
        for step in group.steps:
            previous_owner = step_owner_group.get(step)
            if previous_owner and previous_owner != group.id:
                raise DiagramConfigError(
                    f"Step '{step}' appears in multiple activity groups: '{previous_owner}' and '{group.id}'."
                )
            step_owner_group[step] = group.id

    workflow_ungrouped_steps: dict[str, list[str]] = {}
    for workflow in workflows.values():
        grouped_in_workflow: set[str] = set()
        for group in groups.values():
            if group.workflow == workflow.id:
                grouped_in_workflow.update(group.steps)
        workflow_ungrouped_steps[workflow.id] = [
            step for step in workflow.steps if step not in grouped_in_workflow
        ]

    all_steps = ordered_unique(
        [step for workflow in workflows.values() for step in workflow.steps]
        + [step for group in groups.values() for step in group.steps]
    )

    resolver = RefResolver(workflows=workflows, groups=groups, step_refs=all_steps)
    return workflows, groups, workflow_ungrouped_steps, all_steps, resolver


def build_titles(
    flow_control: dict[str, Any],
    workflows: OrderedDict[str, WorkflowSpec],
    groups: OrderedDict[str, ActivityGroupSpec],
    all_steps: list[str],
    resolver: RefResolver,
) -> dict[str, str]:
    titles: dict[str, str] = {}

    for workflow in workflows.values():
        key = f"workflow:{workflow.id}"
        inferred = parse_title_from_rst(workflow.index_path) if workflow.index_path else None
        titles[key] = workflow.title or inferred or humanize(workflow.id)

    for group in groups.values():
        key = f"group:{group.id}"
        inferred = None
        if group.index_path and (group.index_rst is not None or group.dir is not None):
            inferred = parse_title_from_rst(group.index_path)
        titles[key] = group.title or inferred or humanize(group.id)

    for step in all_steps:
        key = f"step:{step}"
        step_index = SOURCE_DIR / step / "index.rst"
        inferred = parse_title_from_rst(step_index)
        titles[key] = inferred or humanize(step.split("/")[-1])

    raw_overrides = flow_control.get("title_overrides", {})
    if raw_overrides is None:
        raw_overrides = {}
    if not isinstance(raw_overrides, dict):
        raise DiagramConfigError("'title_overrides' must be a mapping of node ref to title text.")

    for raw_ref, raw_title in raw_overrides.items():
        ref = maybe_text(raw_ref)
        title = maybe_text(raw_title)
        if not ref or not title:
            raise DiagramConfigError("Each title override must include a non-empty key and value.")
        canonical = resolver.resolve(ref)
        if not canonical:
            raise DiagramConfigError(f"title_overrides references unknown node '{ref}'.")
        titles[canonical] = title

    return titles


def build_edges(
    flow_control: dict[str, Any],
    workflows: OrderedDict[str, WorkflowSpec],
    groups: OrderedDict[str, ActivityGroupSpec],
    workflow_ungrouped_steps: dict[str, list[str]],
    resolver: RefResolver,
) -> list[tuple[str, str]]:
    explicit_edges: list[tuple[str, str]] = []
    raw_edges = flow_control.get("finish_to_start", [])
    if raw_edges is None:
        raw_edges = []
    if not isinstance(raw_edges, list):
        raise DiagramConfigError("'finish_to_start' must be a list.")

    for edge in raw_edges:
        if not isinstance(edge, dict):
            raise DiagramConfigError("Each finish_to_start entry must be a mapping with 'from' and 'to'.")
        from_ref = maybe_text(edge.get("from"))
        to_ref = maybe_text(edge.get("to"))
        if not from_ref or not to_ref:
            raise DiagramConfigError("Each finish_to_start entry requires both 'from' and 'to'.")

        from_key = resolver.resolve(from_ref)
        to_key = resolver.resolve(to_ref)
        if not from_key:
            raise DiagramConfigError(f"finish_to_start 'from' references unknown node '{from_ref}'.")
        if not to_key:
            raise DiagramConfigError(f"finish_to_start 'to' references unknown node '{to_ref}'.")
        explicit_edges.append((from_key, to_key))

    explicit_step_edges: list[tuple[str, str]] = [
        (from_key, to_key)
        for from_key, to_key in explicit_edges
        if from_key.startswith("step:") and to_key.startswith("step:")
    ]
    explicit_scope_edges: list[tuple[str, str]] = [
        (from_key, to_key)
        for from_key, to_key in explicit_edges
        if not (from_key.startswith("step:") and to_key.startswith("step:"))
    ]

    inferred_step_edges: list[tuple[str, str]] = []
    for group in groups.values():
        group_step_keys = {f"step:{step}" for step in group.steps}
        has_explicit_group_edges = any(
            edge_from in group_step_keys and edge_to in group_step_keys
            for edge_from, edge_to in explicit_step_edges
        )
        if not has_explicit_group_edges:
            for index in range(1, len(group.steps)):
                inferred_step_edges.append((f"step:{group.steps[index - 1]}", f"step:{group.steps[index]}"))

    for workflow in workflows.values():
        ungrouped = workflow_ungrouped_steps.get(workflow.id, [])
        ungrouped_keys = {f"step:{step}" for step in ungrouped}
        has_explicit_ungrouped_edges = any(
            edge_from in ungrouped_keys and edge_to in ungrouped_keys
            for edge_from, edge_to in explicit_step_edges
        )
        if not has_explicit_ungrouped_edges:
            for index in range(1, len(ungrouped)):
                inferred_step_edges.append((f"step:{ungrouped[index - 1]}", f"step:{ungrouped[index]}"))

    step_edges: list[tuple[str, str]] = []
    step_seen: set[tuple[str, str]] = set()
    for edge in explicit_step_edges + inferred_step_edges:
        if edge in step_seen:
            continue
        step_seen.add(edge)
        step_edges.append(edge)

    all_edges: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for edge in (
        explicit_scope_edges
        + step_edges
    ):
        if edge in seen:
            continue
        seen.add(edge)
        all_edges.append(edge)
    return all_edges


def build_hidden_layout_edges(
    flow_control: dict[str, Any],
    resolver: RefResolver,
) -> list[tuple[str, str]]:
    layout = flow_control.get("layout", {})
    if layout is None:
        layout = {}
    if not isinstance(layout, dict):
        raise DiagramConfigError("'layout' must be a mapping.")

    hidden_edges: list[tuple[str, str]] = []
    raw_hidden_edges = layout.get("hidden_edges", [])
    if raw_hidden_edges is None:
        raw_hidden_edges = []
    if not isinstance(raw_hidden_edges, list):
        raise DiagramConfigError("'layout.hidden_edges' must be a list.")

    for edge in raw_hidden_edges:
        if not isinstance(edge, dict):
            raise DiagramConfigError("Each 'layout.hidden_edges' entry must be a mapping with 'from' and 'to'.")
        from_ref = maybe_text(edge.get("from"))
        to_ref = maybe_text(edge.get("to"))
        if not from_ref or not to_ref:
            raise DiagramConfigError("Each 'layout.hidden_edges' entry requires both 'from' and 'to'.")
        from_key = resolver.resolve(from_ref)
        to_key = resolver.resolve(to_ref)
        if not from_key:
            raise DiagramConfigError(f"layout.hidden_edges 'from' references unknown node '{from_ref}'.")
        if not to_key:
            raise DiagramConfigError(f"layout.hidden_edges 'to' references unknown node '{to_ref}'.")
        hidden_edges.append((from_key, to_key))

    raw_chains = layout.get("chains", [])
    if raw_chains is None:
        raw_chains = []
    if not isinstance(raw_chains, list):
        raise DiagramConfigError("'layout.chains' must be a list.")

    for chain in raw_chains:
        if not isinstance(chain, list):
            raise DiagramConfigError("Each 'layout.chains' entry must be a list of node refs.")
        if len(chain) < 2:
            continue

        resolved_chain: list[str] = []
        for item in chain:
            ref = maybe_text(item)
            if not ref:
                raise DiagramConfigError("layout.chains contains an empty node ref.")
            canonical = resolver.resolve(ref)
            if not canonical:
                raise DiagramConfigError(f"layout.chains references unknown node '{ref}'.")
            resolved_chain.append(canonical)

        for index in range(1, len(resolved_chain)):
            hidden_edges.append((resolved_chain[index - 1], resolved_chain[index]))

    deduped: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for edge in hidden_edges:
        if edge in seen:
            continue
        seen.add(edge)
        deduped.append(edge)
    return deduped


def render_plantuml_source(
    flow_control_label: str,
    workflows: OrderedDict[str, WorkflowSpec],
    groups: OrderedDict[str, ActivityGroupSpec],
    workflow_ungrouped_steps: dict[str, list[str]],
    titles: dict[str, str],
    edges: list[tuple[str, str]],
    hidden_edges: list[tuple[str, str]],
) -> str:
    aliases: dict[str, str] = {}
    for workflow in workflows.values():
        aliases[f"workflow:{workflow.id}"] = safe_alias("workflow", workflow.id)
    for group in groups.values():
        aliases[f"group:{group.id}"] = safe_alias("group", group.id)

    all_step_refs = ordered_unique(
        [step for workflow in workflows.values() for step in workflow.steps]
        + [step for group in groups.values() for step in group.steps]
    )
    for step in all_step_refs:
        aliases[f"step:{step}"] = safe_alias("step", step)

    lines: list[str] = [
        "@startuml",
        f"' Auto-generated from {flow_control_label}.",
        "top to bottom direction",
        "hide circle",
        "skinparam shadowing false",
        "skinparam linetype polyline",
        "skinparam nodesep 20",
        "skinparam ranksep 30",
        "",
    ]

    def render_group(group: ActivityGroupSpec, indent: str) -> None:
        group_key = f"group:{group.id}"
        group_alias = aliases[group_key]
        if group.steps:
            lines.append(f'{indent}package "{wrap_label(titles[group_key])}" as {group_alias} {{')
            for step in group.steps:
                step_key = f"step:{step}"
                lines.append(
                    f'{indent}  rectangle "{wrap_label(titles[step_key])}" as {aliases[step_key]}'
                )
            lines.append(f"{indent}}}")
        else:
            lines.append(f'{indent}rectangle "{wrap_label(titles[group_key])}" as {group_alias}')

    for workflow in workflows.values():
        workflow_key = f"workflow:{workflow.id}"
        workflow_alias = aliases[workflow_key]
        ungrouped_steps = workflow_ungrouped_steps.get(workflow.id, [])

        if ungrouped_steps:
            lines.append(f'package "{wrap_label(titles[workflow_key])}" as {workflow_alias} {{')
            for step in ungrouped_steps:
                step_key = f"step:{step}"
                lines.append(
                    f'  rectangle "{wrap_label(titles[step_key])}" as {aliases[step_key]}'
                )
            lines.append("}")
        else:
            lines.append(
                f'rectangle "{wrap_label(titles[workflow_key])}" as {workflow_alias}'
            )

    if workflows:
        lines.append("")

    for group in groups.values():
        render_group(group, indent="")

    if groups:
        lines.append("")

    for from_key, to_key in edges:
        from_alias = aliases.get(from_key)
        to_alias = aliases.get(to_key)
        if not from_alias or not to_alias:
            raise DiagramConfigError(f"Cannot render edge '{from_key} -> {to_key}' due to missing node.")
        lines.append(f"{from_alias} --> {to_alias}")

    if hidden_edges:
        lines.append("")
    for from_key, to_key in hidden_edges:
        from_alias = aliases.get(from_key)
        to_alias = aliases.get(to_key)
        if not from_alias or not to_alias:
            raise DiagramConfigError(
                f"Cannot render hidden layout edge '{from_key} -> {to_key}' due to missing node."
            )
        lines.append(f"{from_alias} -[hidden]down-> {to_alias}")

    lines.append("")
    lines.append("@enduml")
    return "\n".join(lines)


def run(flow_control_path: Path, output_path: Path) -> int:
    if not flow_control_path.exists():
        raise DiagramConfigError(f"Missing flow-control YAML: {flow_control_path}")

    data = yaml.safe_load(flow_control_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise DiagramConfigError("Flow-control YAML root must be a mapping.")

    workflows, groups, workflow_ungrouped_steps, all_steps, resolver = build_model(data)
    titles = build_titles(data, workflows, groups, all_steps, resolver)
    edges = build_edges(data, workflows, groups, workflow_ungrouped_steps, resolver)
    hidden_edges = build_hidden_layout_edges(data, resolver)
    try:
        flow_control_label = flow_control_path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        flow_control_label = str(flow_control_path)

    rendered = render_plantuml_source(
        flow_control_label=flow_control_label,
        workflows=workflows,
        groups=groups,
        workflow_ungrouped_steps=workflow_ungrouped_steps,
        titles=titles,
        edges=edges,
        hidden_edges=hidden_edges,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered, encoding="utf-8")
    print(f"Wrote workflow diagram UML: {output_path}")
    return 0


def run_default() -> int:
    return run(DEFAULT_FLOW_CONTROL_PATH, DEFAULT_OUTPUT_PATH)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--flow-control",
        default=str(DEFAULT_FLOW_CONTROL_PATH),
        help="Path to flow-control YAML.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help="Path to generated .uml file.",
    )
    args = parser.parse_args()

    try:
        return run(Path(args.flow_control), Path(args.output))
    except DiagramConfigError as error:
        print(f"ERROR: {error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

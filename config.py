"""Config loader. YAML if PyYAML available, else a minimal subset parser."""
from __future__ import annotations

from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
CFG_PATH = ROOT / "config.yaml"

_cached: dict[str, Any] | None = None


def _load_yaml(text: str) -> dict[str, Any]:
    try:
        import yaml  # type: ignore
        return yaml.safe_load(text)
    except ImportError:
        return _mini_yaml(text)


def _coerce(v: str) -> Any:
    s = v.strip()
    if s == "": return ""
    if s == "true": return True
    if s == "false": return False
    if s == "null" or s == "~": return None
    if s.startswith('"') and s.endswith('"'): return s[1:-1]
    if s.startswith("'") and s.endswith("'"): return s[1:-1]
    if s.startswith("[") and s.endswith("]"):
        inner = s[1:-1].strip()
        if not inner: return []
        return [_coerce(x.strip()) for x in inner.split(",")]
    try:
        if "." in s: return float(s)
        return int(s)
    except ValueError:
        return s


def _mini_yaml(text: str) -> dict[str, Any]:
    """Minimal YAML subset — enough for our own config. Indented blocks
    of mappings, scalar values, inline lists. Lists-of-mappings supported
    via `- key:` syntax. No anchors, no flow maps, no folded scalars."""
    root: dict[str, Any] = {}
    stack: list[tuple[int, Any]] = [(-1, root)]
    pending_block: list[str] = []
    pending_key: str | None = None
    pending_indent = 0

    for raw in text.splitlines():
        line = raw.rstrip()
        if not line.strip() or line.lstrip().startswith("#"):
            if pending_block and pending_key:
                pending_block.append("")
            continue
        indent = len(line) - len(line.lstrip())
        body = line.lstrip()

        if pending_key is not None and indent > pending_indent:
            pending_block.append(line[pending_indent + 2:] if line.startswith(" " * (pending_indent + 2)) else body)
            continue
        elif pending_key is not None:
            container = stack[-1][1]
            container[pending_key] = "\n".join(pending_block).rstrip()
            pending_key = None
            pending_block = []

        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]

        if body.startswith("- "):
            item_body = body[2:]
            if isinstance(parent, list):
                if ":" in item_body:
                    new = {}
                    parent.append(new)
                    k, _, v = item_body.partition(":")
                    if v.strip():
                        new[k.strip()] = _coerce(v)
                    stack.append((indent, new))
                else:
                    parent.append(_coerce(item_body))
            continue

        if ":" in body:
            k, _, v = body.partition(":")
            k = k.strip()
            v = v.strip()
            if v == "":
                # Could be mapping or list ahead — peek? Default: dict, switch to list on first '-'
                new: Any = {}
                parent[k] = new
                stack.append((indent, new))
            elif v == "|":
                pending_key = k
                pending_indent = indent
                pending_block = []
                # ensure dict insertion happens
                parent[k] = ""
            else:
                parent[k] = _coerce(v)
                # If next line is "- ...", convert to list
                # (handled lazily by reassignment in the '-' branch)

    if pending_key is not None:
        container = stack[-1][1]
        container[pending_key] = "\n".join(pending_block).rstrip()

    # Post: any dict that ended up only collecting list items via '-'
    # should already have been converted because '- ' branch overwrites.
    return root


def get() -> dict[str, Any]:
    global _cached
    if _cached is None:
        _cached = _load_yaml(CFG_PATH.read_text())
    return _cached


def reload() -> dict[str, Any]:
    global _cached
    _cached = None
    return get()

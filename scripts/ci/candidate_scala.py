#!/usr/bin/env python3
"""Select the exact Scala version from a release plan or candidate manifest."""

import argparse
import json
from pathlib import Path
import re


def select(plan, line, binary):
    rows = [row for row in plan["jvm"]
            if row.get("line") == line and row.get("scalaBinary") == binary]
    if len(rows) != 1:
        raise ValueError("Expected exactly one candidate Spark/Scala selection")
    version = rows[0].get("scala", "")
    if not re.fullmatch(re.escape(binary) + r"\.\d+", version):
        raise ValueError("Candidate Scala version differs from its binary version")
    return version


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--plan", type=Path)
    source.add_argument("--candidate", type=Path)
    parser.add_argument("--line", required=True)
    parser.add_argument("--scala-binary", required=True)
    args = parser.parse_args()
    if args.plan:
        plan = json.loads(args.plan.read_text())
    else:
        plan = json.loads((args.candidate / "release-manifest.json").read_text())["plan"]
    print(select(plan, args.line, args.scala_binary))


if __name__ == "__main__":
    main()

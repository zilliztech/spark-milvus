#!/usr/bin/env python3
"""Bind unpublished CI coordinates to one GitHub run and producing attempt."""

import argparse
import os
import re


def check(version, environment):
    if environment.get("GITHUB_REF_TYPE") == "tag":
        if environment.get("GITHUB_EVENT_NAME") != "push" or environment.get("GITHUB_REF_NAME") != "v" + version:
            raise ValueError("Release coordinates must match the pushed tag")
        return
    run, attempt = environment.get("GITHUB_RUN_ID", ""), environment.get("GITHUB_RUN_ATTEMPT", "")
    if not re.fullmatch(r"[1-9][0-9]*", run) or not re.fullmatch(r"[1-9][0-9]*", attempt):
        raise ValueError("Candidate identity requires a GitHub run ID and attempt")
    expected = f"2.1.0-rc.{run}.{attempt}"
    if version != expected:
        raise ValueError("CI candidates require their unique run coordinates: " + expected)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    check(parser.parse_args().version, os.environ)

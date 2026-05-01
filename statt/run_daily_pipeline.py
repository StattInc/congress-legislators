#!/usr/bin/env python3
"""Run the daily Congress data pipeline from the latest upstream YAML snapshots."""

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Sequence

import requests

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
UPSTREAM_RAW_BASE_URL = os.getenv(
    "UPSTREAM_CONGRESS_LEGISLATORS_RAW_BASE_URL",
    "https://raw.githubusercontent.com/unitedstates/congress-legislators/main",
)
UPSTREAM_YAML_FILES = [
    "legislators-current.yaml",
    "legislators-historical.yaml",
    "committees-current.yaml",
    "committee-membership-current.yaml",
]


def run_step(name: str, command: Sequence[str], cwd: Path, env: dict | None = None) -> None:
    print(f"\n=== {name} ===")
    print(f"cwd={cwd}")
    print("cmd=" + " ".join(command))
    subprocess.run(command, cwd=str(cwd), env=env, check=True)


def prepare_runtime_workspace(base_env: dict[str, str]) -> tuple[tempfile.TemporaryDirectory[str], Path]:
    workspace = tempfile.TemporaryDirectory(prefix="congress-legislators-")
    workspace_root = Path(workspace.name)
    workspace_scripts_dir = workspace_root / "scripts"

    print(f"Preparing runtime workspace at {workspace_root}")
    shutil.copytree(
        SCRIPTS_DIR,
        workspace_scripts_dir,
        dirs_exist_ok=True,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )

    session = requests.Session()
    session.headers.update({"User-Agent": "congress-legislators-daily-pipeline/1.0"})

    for filename in UPSTREAM_YAML_FILES:
        url = f"{UPSTREAM_RAW_BASE_URL}/{filename}"
        destination = workspace_root / filename
        print(f"Downloading {url} -> {destination}")
        response = session.get(url, timeout=60)
        response.raise_for_status()
        destination.write_bytes(response.content)

    return workspace, workspace_root


def run_pipeline() -> None:
    base_env = os.environ.copy()
    base_env.setdefault("PYTHONUNBUFFERED", "1")

    workspace, workspace_root = prepare_runtime_workspace(base_env)
    try:
        workspace_scripts_dir = workspace_root / "scripts"

        run_step("Update House Contacts", [sys.executable, "house_contacts.py"], cwd=workspace_scripts_dir, env=base_env)
        run_step("Update Senate Contacts", [sys.executable, "senate_contacts.py"], cwd=workspace_scripts_dir, env=base_env)
        run_step(
            "Update Committee Membership",
            [sys.executable, "committee_membership.py"],
            cwd=workspace_scripts_dir,
            env=base_env,
        )

        loader_env = base_env.copy()
        loader_env["LEGISLATORS_CURRENT_YAML_PATH"] = str(workspace_root / "legislators-current.yaml")
        loader_env["LEGISLATORS_HISTORICAL_YAML_PATH"] = str(workspace_root / "legislators-historical.yaml")
        loader_env["COMMITTEES_CURRENT_YAML_PATH"] = str(workspace_root / "committees-current.yaml")
        loader_env["COMMITTEE_MEMBERSHIP_YAML_PATH"] = str(workspace_root / "committee-membership-current.yaml")

        run_step(
            "Load Legislator History into Postgres",
            [sys.executable, "statt/load_legislator_history.py"],
            cwd=ROOT,
            env=loader_env,
        )
        run_step("Load Committees into Postgres", [sys.executable, "statt/load_committees.py"], cwd=ROOT, env=loader_env)
        run_step(
            "Load Committee Membership into Postgres",
            [sys.executable, "statt/load_committee_members.py"],
            cwd=ROOT,
            env=loader_env,
        )

        print("\nDaily pipeline complete.")
    finally:
        workspace.cleanup()


if __name__ == "__main__":
    try:
        run_pipeline()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as exc:
        print(f"FATAL ERROR: {exc}")
        sys.exit(1)

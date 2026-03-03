#!/usr/bin/env python3
"""Run daily committee data pipeline for Azure Container Apps Jobs."""

import os
import subprocess
import sys
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"


def run_step(name: str, command: Sequence[str], cwd: Path, env: dict | None = None) -> None:
    print(f"\n=== {name} ===")
    print(f"cwd={cwd}")
    print("cmd=" + " ".join(command))
    subprocess.run(command, cwd=str(cwd), env=env, check=True)


def run_pipeline() -> None:
    base_env = os.environ.copy()
    base_env.setdefault("PYTHONUNBUFFERED", "1")

    run_step("Update House Contacts", [sys.executable, "house_contacts.py"], cwd=SCRIPTS_DIR, env=base_env)
    run_step("Update Senate Contacts", [sys.executable, "senate_contacts.py"], cwd=SCRIPTS_DIR, env=base_env)
    run_step("Update Committee Membership", [sys.executable, "committee_membership.py"], cwd=SCRIPTS_DIR, env=base_env)

    loader_env = base_env.copy()
    loader_env["COMMITTEES_CURRENT_YAML_PATH"] = str(ROOT / "committees-current.yaml")
    loader_env["COMMITTEE_MEMBERSHIP_YAML_PATH"] = str(ROOT / "committee-membership-current.yaml")

    run_step("Load Committees into Postgres", [sys.executable, "statt/load_committees.py"], cwd=ROOT, env=loader_env)
    run_step(
        "Load Committee Membership into Postgres",
        [sys.executable, "statt/load_committee_members.py"],
        cwd=ROOT,
        env=loader_env,
    )

    print("\nDaily committee pipeline complete.")


if __name__ == "__main__":
    try:
        run_pipeline()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(1)
    except Exception as exc:
        print(f"FATAL ERROR: {exc}")
        sys.exit(1)

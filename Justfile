set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

python := env_var_or_default("PYTHON", ".venv/bin/python")
image := env_var_or_default("IMAGE", "congress-legislators:local")
dockerfile := env_var_or_default("DOCKERFILE", "infra/azure/Dockerfile")
env_file := env_var_or_default("ENV_FILE", ".env")
docker_network := env_var_or_default("DOCKER_NETWORK", "")

default:
  @just --list

# Core scraper scripts (run from scripts/ so relative paths behave correctly).
house-contacts:
  cd scripts && {{python}} house_contacts.py

senate-contacts:
  cd scripts && {{python}} senate_contacts.py

committee-membership:
  cd scripts && {{python}} committee_membership.py

# DB and pipeline scripts.
load-committees:
  {{python}} statt/load_committees.py

load-committee-members:
  {{python}} statt/load_committee_members.py

update-us-federal-legislators:
  {{python}} statt/update_us_federal_legislators.py

run-daily-pipeline:
  {{python}} statt/run_daily_committee_pipeline.py

# Generic runners for ad-hoc scripts.
run-script script:
  cd scripts && {{python}} {{script}}

run-statt script:
  {{python}} statt/{{script}}

# Container lifecycle.
docker-build:
  docker build -f {{dockerfile}} -t {{image}} .

docker-run:
  docker run --rm --network "statt-network" --env-file {{env_file}} {{image}}

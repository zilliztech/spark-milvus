#!/usr/bin/env bash
# Drive the development container defined in .devcontainer/ from the command
# line. Design: docs/design/engineering/devcontainer.html.
#
#   scripts/devcontainer.sh up [--services]   build the dev image if needed and start it;
#                                              --services also starts Milvus, etcd and MinIO
#   scripts/devcontainer.sh init               initialize submodules and Conan inside the container
#   scripts/devcontainer.sh shell              interactive login shell in the container
#   scripts/devcontainer.sh run [--env-file F] <cmd...>
#                                              run one command in the container (login shell, so
#                                              NATIVE_JOBS defaults to the container's CPUs);
#                                              --env-file passes KEY=VALUE lines as environment
#   scripts/devcontainer.sh down               stop the container and the services
#   scripts/devcontainer.sh clean              down, then delete the cache and service volumes
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
compose=(docker compose --project-directory "${root}/.devcontainer" -f "${root}/.devcontainer/docker-compose.yml")

# Files written into the bind-mounted checkout must belong to the host user.
export DEV_UID="${DEV_UID:-$(id -u)}"
export DEV_GID="${DEV_GID:-$(id -g)}"

usage() {
  sed -n '2,/^set -euo/p' "${BASH_SOURCE[0]}" | grep '^#' | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

init_inside() {
  # Runs inside the container. Idempotent.
  cd /workspace
  git config --global --add safe.directory /workspace
  for submodule in milvus-proto milvus-storage knowhere; do
    git config --global --add safe.directory "/workspace/${submodule}"
  done
  git submodule update --init milvus-proto milvus-storage knowhere
  if [ ! -f "${CONAN_HOME}/profiles/default" ]; then
    conan profile detect --force
  fi
  if ! conan remote list 2>/dev/null | grep -q '^default-conan-local2:'; then
    conan remote add --force default-conan-local2 \
      https://milvus01.jfrog.io/artifactory/api/conan/default-conan-local2
  fi
  echo "dev container ready: $(conan --version), cmake $(cmake --version | head -1 | awk '{print $3}'), $(java -version 2>&1 | head -1), NATIVE_JOBS=${NATIVE_JOBS:-$(nproc)}"
}

command_name="${1:-}"
[ -n "${command_name}" ] || usage 1
shift

case "${command_name}" in
  up)
    if [ "${1:-}" = "--services" ]; then
      "${compose[@]}" --profile services up -d --build
    else
      "${compose[@]}" up -d --build dev
    fi
    ;;
  init)
    if [ "${1:-}" = "--inside" ]; then
      init_inside
    else
      "${compose[@]}" exec dev /workspace/scripts/devcontainer.sh init --inside
    fi
    ;;
  shell)
    "${compose[@]}" exec dev bash -l
    ;;
  run)
    env_args=()
    if [ "${1:-}" = "--env-file" ]; then
      while IFS= read -r line || [ -n "${line}" ]; do
        case "${line}" in
          ''|'#'*) ;;
          *=*) env_args+=(-e "${line}") ;;
        esac
      done < "$2"
      shift 2
    fi
    [ "$#" -gt 0 ] || usage 1
    "${compose[@]}" exec "${env_args[@]}" dev bash -lc "$(printf '%q ' "$@")"
    ;;
  down)
    "${compose[@]}" --profile services down
    ;;
  clean)
    "${compose[@]}" --profile services down --volumes --remove-orphans
    ;;
  -h|--help|help)
    usage 0
    ;;
  *)
    echo "unknown command: ${command_name}" >&2
    usage 1
    ;;
esac

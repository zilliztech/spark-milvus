#!/usr/bin/env bash

set -Eeuo pipefail

test_dir=$(mktemp -d "${TMPDIR:-/tmp}/spark-milvus-helm-run-test.XXXXXX")
trap 'rm -rf "$test_dir"' EXIT

chart_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
runner="${chart_dir}/run.sh"
fake_bin="${test_dir}/bin"
values_file="${test_dir}/values.yaml"
mkdir -p "$fake_bin"
printf 'test: true\n' >"$values_file"

cat >"${fake_bin}/helm" <<'EOF'
#!/usr/bin/env bash
set -eu

printf '%s\n' "$*" >>"$MOCK_HELM_CALLS"

case "${1:-}" in
  lint | install | status | uninstall | list)
    exit 0
    ;;
  get)
    printf '{"ownershipToken":"unused"}\n'
    ;;
  *)
    echo "Unexpected helm invocation: $*" >&2
    exit 2
    ;;
esac
EOF

cat >"${fake_bin}/kubectl" <<'EOF'
#!/usr/bin/env bash
set -eu

if [[ "${1:-}" == --request-timeout=* ]]; then
  shift
fi

printf '%s\n' "$*" >>"$MOCK_KUBECTL_CALLS"
verb=${1:-}
resource=${2:-}
arguments="$*"

case "${verb}:${resource}" in
  get:namespace)
    exit 0
    ;;
  get:job)
    printf '%s\n' "${MOCK_JOB_CONDITIONS:-|}"
    ;;
  get:jobs)
    if [[ "$arguments" == *'.metadata.name'* ]]; then
      printf '%s\n' "${MOCK_JOB_NAME:-integration-job}"
    elif [[ "$arguments" == *'-o name'* ]]; then
      printf 'job/%s\n' "${MOCK_JOB_NAME:-integration-job}"
    fi
    ;;
  get:pods)
    if [[ "$arguments" == *'containerStatuses'* ]]; then
      printf '%s\n' "${MOCK_WAITING_REASON:-}"
      if [[ "$arguments" == *'initContainerStatuses'* ]]; then
        printf '%s\n' "${MOCK_INIT_WAITING_REASON:-}"
      fi
      if [[ "$arguments" != *'component=integration-test'* ]]; then
        printf '%s\n' "${MOCK_MILVUS_WAITING_REASON:-}"
      fi
    elif [[ "$arguments" == *'PodScheduled'* ]]; then
      printf '%s\n' "${MOCK_SCHEDULED_CONDITIONS:-}"
    elif [[ "$arguments" == *'-o name'* ]]; then
      printf 'pod/milvus-pod\n'
      printf 'pod/%s-pod\n' "${MOCK_JOB_NAME:-integration-job}"
    fi
    ;;
  get:jobs,deployments,services,configmaps,pods | get:events | logs:* | describe:*)
    exit 0
    ;;
  *)
    echo "Unexpected kubectl invocation: $*" >&2
    exit 2
    ;;
esac
EOF

chmod +x "${fake_bin}/helm" "${fake_bin}/kubectl"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

run_case() {
  local name=$1
  local expected_status=$2
  local expected_message=$3
  local job_conditions=$4
  local waiting_reason=$5
  local scheduled_conditions=$6
  local case_dir="${test_dir}/${name}"
  local release="it-${name}"
  local status

  mkdir -p "$case_dir"
  : >"${case_dir}/helm-calls"
  : >"${case_dir}/kubectl-calls"

  set +e
  PATH="${fake_bin}:${PATH}" \
    ARTIFACT_DIR="${case_dir}/artifacts" \
    JOB_WAIT_TIMEOUT_SECONDS=1 \
    POLL_INTERVAL_SECONDS=1 \
    MOCK_HELM_CALLS="${case_dir}/helm-calls" \
    MOCK_KUBECTL_CALLS="${case_dir}/kubectl-calls" \
    MOCK_JOB_CONDITIONS="$job_conditions" \
    MOCK_WAITING_REASON="$waiting_reason" \
    MOCK_INIT_WAITING_REASON="${7:-}" \
    MOCK_MILVUS_WAITING_REASON="${8:-}" \
    MOCK_SCHEDULED_CONDITIONS="$scheduled_conditions" \
    "$runner" "$release" integration-tests "$values_file" \
    >"${case_dir}/output" 2>&1
  status=$?
  set -e

  if [[ "$status" -ne "$expected_status" ]]; then
    cat "${case_dir}/output" >&2
    fail "${name}: expected status ${expected_status}, got ${status}"
  fi
  if [[ -n "$expected_message" ]] &&
    ! grep -Fq -- "$expected_message" "${case_dir}/output"; then
    cat "${case_dir}/output" >&2
    fail "${name}: missing output: ${expected_message}"
  fi
  if ! grep -q '^uninstall ' "${case_dir}/helm-calls"; then
    fail "${name}: release was not uninstalled"
  fi
  if [[ ! -f "${case_dir}/artifacts/milvus-pod.log" ||
        ! -f "${case_dir}/artifacts/milvus-pod.previous.log" ]]; then
    fail "${name}: Milvus pod logs were not collected"
  fi
  if [[ ! -f "${case_dir}/artifacts/helm-status.txt" ]]; then
    fail "${name}: diagnostics were not collected"
  fi
}

for reason in \
  ErrImagePull \
  ErrImageNeverPull \
  ImagePullBackOff \
  InvalidImageName \
  CreateContainerConfigError \
  RunContainerError \
  CrashLoopBackOff; do
  case_name=$(tr '[:upper:]' '[:lower:]' <<<"$reason")
  run_case \
    "$case_name" \
    1 \
    "cannot start: release Pod waiting reason ${reason}" \
    '|' \
    "$reason" \
    ''
done

run_case \
  unschedulable \
  1 \
  'cannot start: release Pod is unschedulable' \
  '|' \
  '' \
  'False|Unschedulable'

run_case init-imagepull 1 'release Pod waiting reason ImagePullBackOff' '|' '' '' ImagePullBackOff
run_case milvus-crashloop 1 'release Pod waiting reason CrashLoopBackOff' '|' '' '' '' CrashLoopBackOff

run_case complete 0 '' 'True|' '' ''
run_case failed 1 'Integration Job integration-job failed' '|True' '' ''

echo "All run.sh tests passed"

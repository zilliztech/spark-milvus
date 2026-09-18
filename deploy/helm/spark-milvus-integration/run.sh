#!/usr/bin/env bash

set -Eeuo pipefail

usage() {
  echo "Usage: $0 <release> <namespace> <values-file>" >&2
}

if [[ $# -ne 3 ]]; then
  usage
  exit 2
fi

release=$1
namespace=$2
values_file=$3
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
helm_operation_timeout=${HELM_OPERATION_TIMEOUT:-5m}
job_wait_timeout_seconds=${JOB_WAIT_TIMEOUT_SECONDS:-3900}
poll_interval_seconds=${POLL_INTERVAL_SECONDS:-5}
kubectl_request_timeout=${KUBECTL_REQUEST_TIMEOUT:-20s}
keep_resources=${KEEP_RESOURCES:-false}
artifact_dir=${ARTIFACT_DIR:-"${PWD}/target/integration-helm/${release}"}
run_id=${CI_RUN_ID:-$release}
ownership_token="$(date +%s)-$$-${RANDOM}-${RANDOM}"
installed=false
install_started=false
selector="app.kubernetes.io/instance=${release},app.kubernetes.io/component=integration-test"

case "$keep_resources" in
  true | false) ;;
  *)
    echo "KEEP_RESOURCES must be 'true' or 'false'" >&2
    exit 2
    ;;
esac

if [[ ! "$job_wait_timeout_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "JOB_WAIT_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 2
fi

if [[ ! "$poll_interval_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "POLL_INTERVAL_SECONDS must be a positive integer" >&2
  exit 2
fi

if [[ ! "$release" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ || ${#release} -gt 53 ]]; then
  echo "Release must be a valid Helm release name of at most 53 characters" >&2
  exit 2
fi

if [[ ! "$namespace" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ || ${#namespace} -gt 63 ]]; then
  echo "Namespace must be a valid Kubernetes namespace name" >&2
  exit 2
fi

for tool in helm kubectl; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "$tool is required" >&2
    exit 2
  fi
done

if [[ ! -f "$values_file" ]]; then
  echo "Values file does not exist: $values_file" >&2
  exit 2
fi

mkdir -p "$artifact_dir"

kube() {
  kubectl --request-timeout="$kubectl_request_timeout" "$@"
}

collect_diagnostics() {
  helm status "$release" --namespace "$namespace" \
    >"${artifact_dir}/helm-status.txt" 2>&1 || true
  kube get jobs,pods --namespace "$namespace" \
    --selector "$selector" \
    -o wide >"${artifact_dir}/resources.txt" 2>&1 || true

  local resource resource_name
  for resource in $(kube get jobs --namespace "$namespace" \
    --selector "$selector" \
    -o name 2>/dev/null); do
    resource_name=${resource#*/}
    kube logs --namespace "$namespace" "$resource" --all-containers=true \
      >"${artifact_dir}/${resource_name}.log" 2>&1 || true
    kube describe --namespace "$namespace" "$resource" \
      >"${artifact_dir}/${resource_name}.describe.txt" 2>&1 || true
    kube get events --namespace "$namespace" \
      --field-selector "involvedObject.name=${resource_name}" \
      --sort-by=.metadata.creationTimestamp \
      >"${artifact_dir}/${resource_name}.events.txt" 2>&1 || true
  done

  for resource in $(kube get pods --namespace "$namespace" \
    --selector "$selector" \
    -o name 2>/dev/null); do
    resource_name=${resource#*/}
    kube describe --namespace "$namespace" "$resource" \
      >"${artifact_dir}/${resource_name}.describe.txt" 2>&1 || true
    kube get events --namespace "$namespace" \
      --field-selector "involvedObject.name=${resource_name}" \
      --sort-by=.metadata.creationTimestamp \
      >"${artifact_dir}/${resource_name}.events.txt" 2>&1 || true
  done
}

wait_for_job() {
  local job_name=$1
  local deadline=$((SECONDS + job_wait_timeout_seconds))
  local conditions complete failed waiting_reasons

  while ((SECONDS < deadline)); do
    conditions=$(kube get job "$job_name" --namespace "$namespace" \
      -o 'jsonpath={.status.conditions[?(@.type=="Complete")].status}{"|"}{.status.conditions[?(@.type=="Failed")].status}')
    IFS='|' read -r complete failed <<<"$conditions"

    if [[ "$complete" == True ]]; then
      return 0
    fi
    if [[ "$failed" == True ]]; then
      echo "Integration Job ${job_name} failed" >&2
      return 1
    fi

    waiting_reasons=$(kube get pods --namespace "$namespace" --selector "$selector" \
      -o 'jsonpath={range .items[*].status.containerStatuses[*]}{.state.waiting.reason}{"\n"}{end}')
    if grep -Fqx CreateContainerConfigError <<<"$waiting_reasons"; then
      echo "Integration Job ${job_name} has an invalid container configuration" >&2
      return 1
    fi

    sleep "$poll_interval_seconds"
  done

  echo "Integration Job ${job_name} did not finish within ${job_wait_timeout_seconds}s" >&2
  return 124
}

owns_release() {
  local release_values

  if ! release_values=$(helm get values "$release" --namespace "$namespace" \
    --output json 2>/dev/null); then
    return 1
  fi
  grep -Fq -- "$ownership_token" <<<"$release_values"
}

finish() {
  local status=$?
  local cleanup_status=0
  trap - EXIT INT TERM

  if [[ "$installed" == false && "$install_started" == true ]] && owns_release; then
    installed=true
  fi

  if [[ "$installed" == true ]]; then
    collect_diagnostics
    if [[ "$keep_resources" == false ]]; then
      if helm uninstall "$release" --namespace "$namespace" --wait \
        --timeout "$helm_operation_timeout" \
        >"${artifact_dir}/helm-uninstall.txt" 2>&1; then
        :
      else
        cleanup_status=$?
        echo "Failed to uninstall Helm release ${release}; see helm-uninstall.txt" >&2
      fi
    else
      echo "Keeping Helm release ${release} in namespace ${namespace}" >&2
    fi
  fi

  if [[ "$status" -eq 0 && "$cleanup_status" -ne 0 ]]; then
    status=$cleanup_status
  fi
  exit "$status"
}

trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

helm lint --strict "$script_dir" --values "$values_file"

if ! kube get namespace "$namespace" >/dev/null; then
  echo "Namespace ${namespace} must already exist" >&2
  exit 2
fi

if ! existing_release=$(helm list --namespace "$namespace" --all --short \
  --filter "^${release}$" --max 1); then
  echo "Unable to list Helm releases in namespace ${namespace}" >&2
  exit 2
fi
if [[ -n "$existing_release" ]]; then
  echo "Helm release ${release} already exists in namespace ${namespace}" >&2
  exit 2
fi

# Installation and Job waiting are separate so ownership is established only
# after Helm successfully creates this release. Do not use --atomic: diagnostics
# must be collected before the release is removed.
install_started=true
if helm install "$release" "$script_dir" \
  --namespace "$namespace" \
  --values "$values_file" \
  --set-string "runId=${run_id}" \
  --set-string "ownershipToken=${ownership_token}" \
  --timeout "$helm_operation_timeout"; then
  installed=true
else
  install_status=$?
  # Helm may persist a failed release after partially creating its resources.
  # Clean it only when its stored values contain this invocation's marker.
  if owns_release; then
    installed=true
  fi
  exit "$install_status"
fi

job_names=$(kube get jobs --namespace "$namespace" --selector "$selector" \
  -o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}')
job_count=$(awk 'NF { count++ } END { print count + 0 }' <<<"$job_names")
if [[ "$job_count" -ne 1 ]]; then
  echo "Expected one integration Job for release ${release}, found ${job_count}" >&2
  exit 1
fi
job_name=$(awk 'NF { print; exit }' <<<"$job_names")

wait_for_job "$job_name"

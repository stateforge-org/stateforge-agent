#!/usr/bin/env bash

set -euo pipefail

if (( $# != 2 )); then
    printf 'usage: %s CONTAINER_NAME EVIDENCE_DIRECTORY\n' "${0##*/}" >&2
    exit 2
fi

readonly CONTAINER_NAME="$1"
readonly EVIDENCE_DIRECTORY="$2"
readonly ACTION_PATH="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

mkdir -p "${EVIDENCE_DIRECTORY}"

docker inspect "${CONTAINER_NAME}" \
    > "${EVIDENCE_DIRECTORY}/container-inspect.json" 2>&1 || true
docker logs "${CONTAINER_NAME}" \
    > "${EVIDENCE_DIRECTORY}/container.log" 2>&1 || true
docker exec "${CONTAINER_NAME}" \
    systemctl status stateforge-agent.service --no-pager \
    > "${EVIDENCE_DIRECTORY}/agent-service-status.txt" 2>&1 || true
docker exec "${CONTAINER_NAME}" \
    systemctl show stateforge-agent.service \
    --property=Id \
    --property=InvocationID \
    --property=MainPID \
    --property=ActiveState \
    --property=SubState \
    --property=ExecMainStartTimestamp \
    --property=FragmentPath \
    > "${EVIDENCE_DIRECTORY}/agent-service-properties.txt" 2>&1 || true
docker exec "${CONTAINER_NAME}" \
    journalctl -u stateforge-agent.service --no-pager -n 500 \
    > "${EVIDENCE_DIRECTORY}/agent-journal.txt" 2>&1 || true
docker exec "${CONTAINER_NAME}" \
    journalctl -u stateforge-agent.service --no-pager -n 500 -o json \
    > "${EVIDENCE_DIRECTORY}/agent-journal.jsonl" \
    2> "${EVIDENCE_DIRECTORY}/agent-journal-json-errors.txt" || true
docker exec "${CONTAINER_NAME}" bash -lc \
    "journalctl --no-pager -n 500 -u 'stateforge-test-*'" \
    > "${EVIDENCE_DIRECTORY}/provider-journal.txt" 2>&1 || true
docker exec "${CONTAINER_NAME}" bash -lc \
    "journalctl --no-pager -n 500 -o json -u 'stateforge-test-*'" \
    > "${EVIDENCE_DIRECTORY}/provider-journal.jsonl" \
    2> "${EVIDENCE_DIRECTORY}/provider-journal-json-errors.txt" || true
/bin/bash "${ACTION_PATH}/build-invocation-index.sh" \
    "${EVIDENCE_DIRECTORY}/agent-journal.jsonl" \
    "${EVIDENCE_DIRECTORY}/provider-journal.jsonl" \
    > "${EVIDENCE_DIRECTORY}/systemd-invocations.json" \
    2> "${EVIDENCE_DIRECTORY}/systemd-invocation-errors.txt" || true
for error_file in \
    "${EVIDENCE_DIRECTORY}/agent-journal-json-errors.txt" \
    "${EVIDENCE_DIRECTORY}/provider-journal-json-errors.txt" \
    "${EVIDENCE_DIRECTORY}/systemd-invocation-errors.txt"; do
    if [[ ! -s "${error_file}" ]]; then
        rm -f "${error_file}"
    fi
done
docker exec "${CONTAINER_NAME}" bash -lc \
    'set -euo pipefail
     systemctl list-units --all --type=service --no-legend --plain \
       "stateforge-test-*" | awk "{print \$1}" | while read -r unit; do
       [[ -n "${unit}" ]] || continue
       printf "[%s]\n" "${unit}"
       systemctl show "${unit}" \
         --property=Id \
         --property=InvocationID \
         --property=MainPID \
         --property=ActiveState \
         --property=SubState \
         --property=ExecMainStartTimestamp \
         --property=FragmentPath
     done' \
    > "${EVIDENCE_DIRECTORY}/provider-service-properties.txt" 2>&1 || true
docker exec "${CONTAINER_NAME}" ps -ef \
    > "${EVIDENCE_DIRECTORY}/processes.txt" 2>&1 || true
docker exec "${CONTAINER_NAME}" bash -lc \
    'find /run/stateforge-agent/providers -maxdepth 1 -type f \
       -name "*.sfp.json" -print -exec cat {} \;' \
    > "${EVIDENCE_DIRECTORY}/provider-descriptors.txt" 2>&1 || true
docker exec "${CONTAINER_NAME}" bash -lc \
    'find /var/lib/stateforge-provider-fixtures -xdev \
       -mindepth 1 -maxdepth 2 \
       -printf "%M\\t%u\\t%g\\t%s\\t%T@\\t%p\\n" | sort' \
    > "${EVIDENCE_DIRECTORY}/provider-state-metadata.txt" 2>&1 || true
docker exec "${CONTAINER_NAME}" bash -lc \
    'find /var/lib/stateforge-provider-fixtures -xdev -type f \
       -print0 | sort -z | xargs -0 -r sha256sum' \
    > "${EVIDENCE_DIRECTORY}/provider-state-sha256.txt" 2>&1 || true

mkdir -p "${EVIDENCE_DIRECTORY}/provider-identities"
while IFS= read -r identity_path; do
    state_name="$(basename -- "$(dirname -- "${identity_path}")")"
    if [[ ! "${state_name}" =~ ^[a-z][a-z0-9-]{0,20}$ ]]; then
        printf 'Skipped unsafe Provider state name: %s\n' "${state_name}" \
            >> "${EVIDENCE_DIRECTORY}/provider-identities/errors.txt"
        continue
    fi
    docker cp "${CONTAINER_NAME}:${identity_path}" \
        "${EVIDENCE_DIRECTORY}/provider-identities/${state_name}.json" \
        2>> "${EVIDENCE_DIRECTORY}/provider-identities/errors.txt" || true
done < <(
    docker exec "${CONTAINER_NAME}" find \
        /var/lib/stateforge-provider-fixtures \
        -mindepth 2 -maxdepth 2 -type f -name identity.json -print \
        2>/dev/null || true
)
if [[ ! -s "${EVIDENCE_DIRECTORY}/provider-identities/errors.txt" ]]; then
    rm -f "${EVIDENCE_DIRECTORY}/provider-identities/errors.txt"
fi

docker exec "${CONTAINER_NAME}" rpm -V --nodeps stateforge-agent \
    > "${EVIDENCE_DIRECTORY}/rpm-final-verify.txt" 2>&1 || true

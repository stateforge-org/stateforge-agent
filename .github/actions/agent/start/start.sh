#!/usr/bin/env bash

set -euo pipefail

if (( $# != 3 )); then
    printf 'usage: %s CONTAINER_NAME TIMEOUT_SECONDS OUTPUT_PATH\n' \
        "${0##*/}" >&2
    exit 2
fi

readonly CONTAINER_NAME="$1"
readonly TIMEOUT_SECONDS="$2"
readonly OUTPUT_PATH="$3"
readonly ACTION_PATH="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

if [[ ! "${TIMEOUT_SECONDS}" =~ ^[1-9][0-9]*$ ]]; then
    printf 'Agent startup timeout must be a positive integer.\n' >&2
    exit 2
fi

docker exec "${CONTAINER_NAME}" systemctl daemon-reload
initial_state="$(
    docker exec "${CONTAINER_NAME}" \
        systemctl is-active stateforge-agent.service 2>/dev/null || true
)"
if [[ "${initial_state}" != "inactive" && "${initial_state}" != "failed" ]]; then
    printf 'Agent service must be stopped before startup; found %s.\n' \
        "${initial_state}" >&2
    exit 2
fi
docker exec "${CONTAINER_NAME}" systemctl start stateforge-agent.service

deadline=$((SECONDS + TIMEOUT_SECONDS))
expected_invocation_id=""
failure_reason=""
while (( SECONDS < deadline )); do
    state="$(
        docker exec "${CONTAINER_NAME}" \
            systemctl is-active stateforge-agent.service 2>/dev/null || true
    )"
    invocation_id="$(
        docker exec "${CONTAINER_NAME}" \
            systemctl show stateforge-agent.service \
                --property=InvocationID --value 2>/dev/null || true
    )"

    if [[ -z "${expected_invocation_id}" ]]; then
        if [[ "${invocation_id}" =~ ^[0-9a-f]{32}$ ]]; then
            expected_invocation_id="${invocation_id}"
        elif [[ "${state}" == "failed" || "${state}" == "inactive" ]]; then
            failure_reason="Agent startup ended before an invocation was created."
            break
        else
            sleep 1
            continue
        fi
    fi

    journal="$(
        docker exec "${CONTAINER_NAME}" \
            journalctl -u stateforge-agent.service --no-pager -o json
    )"
    set +e
    readiness="$(
        printf '%s\n' "${journal}" \
            | /bin/bash "${ACTION_PATH}/check-ready-invocation.sh" \
                "${expected_invocation_id}" "${invocation_id}" "${state}"
    )"
    readiness_status=$?
    set -e
    case "${readiness_status}" in
        0)
            {
                printf 'service-state=%s\n' "${state}"
                printf 'service-invocation-id=%s\n' \
                    "${expected_invocation_id}"
            } >> "${OUTPUT_PATH}"
            exit 0
            ;;
        3)
            ;;
        4)
            failure_reason="Agent startup invocation was replaced or stopped."
            break
            ;;
        *)
            printf 'Agent readiness classifier failed with status %s.\n' \
                "${readiness_status}" >&2
            exit 1
            ;;
    esac
    if [[ -n "${readiness}" ]]; then
        printf 'Unexpected Agent readiness output: %s\n' \
            "${readiness}" >&2
        exit 1
    fi
    sleep 1
done

docker exec "${CONTAINER_NAME}" \
    systemctl status stateforge-agent.service --no-pager || true
docker exec "${CONTAINER_NAME}" \
    journalctl -u stateforge-agent.service --no-pager || true
printf '%s\n' "${failure_reason:-Agent service did not become ready.}" >&2
exit 1

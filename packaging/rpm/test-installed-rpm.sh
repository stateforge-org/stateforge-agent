#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIRECTORY="$(
    cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd
)"
readonly SOURCE_ROOT="$(cd -- "${SCRIPT_DIRECTORY}/../.." && pwd)"

RPM_PATH=""
EVIDENCE_DIRECTORY="${SOURCE_ROOT}/dist/rpm-test-evidence"
CONTAINER_IMAGE="stateforge-agent-rpm-test:fedora42"
CONTAINER_NAME="stateforge-agent-rpm-test-${RANDOM}-$$"

usage() {
    printf '%s\n' \
        'Usage: packaging/rpm/test-installed-rpm.sh --rpm PATH [options]' \
        '' \
        'Options:' \
        '  --evidence-directory PATH  Destination for verification evidence.' \
        '  --container-image IMAGE     Test image tag.'
}

parse_arguments() {
    while (($# > 0)); do
        case "$1" in
            --rpm)
                RPM_PATH="$2"
                shift 2
                ;;
            --evidence-directory)
                EVIDENCE_DIRECTORY="$2"
                shift 2
                ;;
            --container-image)
                CONTAINER_IMAGE="$2"
                shift 2
                ;;
            --help|-h)
                usage
                exit 0
                ;;
            *)
                printf 'Unknown argument: %s\n' "$1" >&2
                usage >&2
                exit 2
                ;;
        esac
    done
}

cleanup() {
    docker logs "${CONTAINER_NAME}" 2>/dev/null || true
    docker rm --force "${CONTAINER_NAME}" >/dev/null 2>&1 || true
}

wait_for_systemd() {
    local state

    for _ in {1..30}; do
        state="$(docker exec "${CONTAINER_NAME}" \
            systemctl is-system-running 2>/dev/null || true)"
        if [[ "${state}" == "running" || "${state}" == "degraded" ]]; then
            return
        fi
        sleep 1
    done
    printf 'systemd did not become ready in %s.\n' "${CONTAINER_NAME}" >&2
    return 1
}

main() {
    local artifact_directory artifact_name evidence_directory

    parse_arguments "$@"
    [[ -f "${RPM_PATH}" ]] || {
        printf 'RPM does not exist: %s\n' "${RPM_PATH}" >&2
        exit 1
    }
    command -v docker >/dev/null 2>&1 || {
        printf 'Docker is required to test the installed RPM.\n' >&2
        exit 1
    }

    artifact_directory="$(cd -- "$(dirname -- "${RPM_PATH}")" && pwd -P)"
    artifact_name="$(basename -- "${RPM_PATH}")"
    mkdir -p "${EVIDENCE_DIRECTORY}"
    evidence_directory="$(cd -- "${EVIDENCE_DIRECTORY}" && pwd -P)"

    docker build \
        --file "${SCRIPT_DIRECTORY}/Dockerfile.test" \
        --tag "${CONTAINER_IMAGE}" \
        "${SCRIPT_DIRECTORY}"
    trap cleanup EXIT
    docker run --detach \
        --name "${CONTAINER_NAME}" \
        --privileged \
        --cgroupns=host \
        --tmpfs /run \
        --tmpfs /run/lock \
        --volume /sys/fs/cgroup:/sys/fs/cgroup:rw \
        --mount "type=bind,source=${artifact_directory},target=/artifacts,readonly" \
        --mount "type=bind,source=${evidence_directory},target=/evidence" \
        "${CONTAINER_IMAGE}" >/dev/null

    wait_for_systemd
    docker exec \
        --env "RPM_PATH=/artifacts/${artifact_name}" \
        "${CONTAINER_NAME}" \
        /usr/local/bin/test-installed-rpm

    if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
        printf 'evidence-directory=%s\n' "${evidence_directory}" \
            >> "${GITHUB_OUTPUT}"
    fi
}

main "$@"

#!/usr/bin/env bash

set -euo pipefail

readonly SCRIPT_DIRECTORY="$(
    cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd
)"
readonly SOURCE_ROOT="$(cd -- "${SCRIPT_DIRECTORY}/../.." && pwd)"

OUTPUT_DIRECTORY="${SOURCE_ROOT}/dist/rpm"
CONTAINER_IMAGE="stateforge-agent-rpm-builder:fedora42"

usage() {
    printf '%s\n' \
        'Usage: packaging/rpm/build-rpm.sh [options]' \
        '' \
        'Options:' \
        '  --output-directory PATH  Destination for RPMs and metadata.' \
        '  --container-image IMAGE  Builder image tag.'
}

absolute_directory() {
    local directory="$1"

    mkdir -p "${directory}"
    (cd -- "${directory}" && pwd -P)
}

parse_arguments() {
    while (($# > 0)); do
        case "$1" in
            --output-directory)
                OUTPUT_DIRECTORY="$2"
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

write_action_outputs() {
    local metadata_path="$1"
    local rpm_path source_rpm_path version rpm_sha256

    rpm_path="$(find "${OUTPUT_DIRECTORY}" -maxdepth 1 -type f \
        -name 'stateforge-agent-*.rpm' ! -name '*.src.rpm' -print -quit)"
    source_rpm_path="$(find "${OUTPUT_DIRECTORY}" -maxdepth 1 -type f \
        -name 'stateforge-agent-*.src.rpm' -print -quit)"
    if [[ -z "${rpm_path}" || -z "${source_rpm_path}" ]]; then
        printf 'Expected one binary RPM and one source RPM.\n' >&2
        exit 1
    fi
    version="$(python3 -c \
        'import json,sys; print(json.load(open(sys.argv[1]))["project_version"])' \
        "${metadata_path}")"
    rpm_sha256="$(python3 -c \
        'import json,sys; print(json.load(open(sys.argv[1]))["rpm"]["sha256"])' \
        "${metadata_path}")"

    printf 'Built %s\nSHA-256: %s\n' "${rpm_path}" "${rpm_sha256}"
    if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
        {
            printf 'rpm-path=%s\n' "${rpm_path}"
            printf 'source-rpm-path=%s\n' "${source_rpm_path}"
            printf 'metadata-path=%s\n' "${metadata_path}"
            printf 'version=%s\n' "${version}"
            printf 'rpm-sha256=%s\n' "${rpm_sha256}"
        } >> "${GITHUB_OUTPUT}"
    fi
}

main() {
    local output_directory

    parse_arguments "$@"
    command -v docker >/dev/null 2>&1 || {
        printf 'Docker is required to build the Fedora RPM.\n' >&2
        exit 1
    }
    output_directory="$(absolute_directory "${OUTPUT_DIRECTORY}")"
    OUTPUT_DIRECTORY="${output_directory}"
    rm -f -- "${OUTPUT_DIRECTORY}"/*.rpm \
        "${OUTPUT_DIRECTORY}/SHA256SUMS" \
        "${OUTPUT_DIRECTORY}/build-metadata.json"

    docker build \
        --file "${SCRIPT_DIRECTORY}/Dockerfile.builder" \
        --tag "${CONTAINER_IMAGE}" \
        "${SCRIPT_DIRECTORY}"
    docker run --rm \
        --env "HOST_UID=$(id -u)" \
        --env "HOST_GID=$(id -g)" \
        --mount "type=bind,source=${SOURCE_ROOT},target=/workspace,readonly" \
        --mount "type=bind,source=${OUTPUT_DIRECTORY},target=/output" \
        "${CONTAINER_IMAGE}"

    write_action_outputs "${OUTPUT_DIRECTORY}/build-metadata.json"
}

main "$@"

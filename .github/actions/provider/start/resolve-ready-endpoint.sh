#!/usr/bin/env bash

set -euo pipefail

if (( $# != 4 )); then
    printf 'usage: %s IDENTITY_PATH DISCOVERY_DIRECTORY UNIT_NAME PREVIOUS_GENERATION\n' \
        "${0##*/}" >&2
    exit 2
fi

readonly IDENTITY_PATH="$1"
readonly DISCOVERY_DIRECTORY="$2"
readonly UNIT_NAME="$3"
readonly PREVIOUS_GENERATION="$4"
readonly UUID_PATTERN='^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

if [[ ! "${PREVIOUS_GENERATION}" =~ ^[0-9]+$ ]]; then
    exit 2
fi

systemctl is-active --quiet "${UNIT_NAME}"
[[ -f "${IDENTITY_PATH}" ]]

identity_before="$(cat -- "${IDENTITY_PATH}")"
provider_instance_id="$(
    jq -er '.provider_instance_id | strings' <<< "${identity_before}" \
        2>/dev/null
)"
identity_generation="$(
    jq -er \
        --argjson previous_generation "${PREVIOUS_GENERATION}" \
        '.last_endpoint_generation
         | numbers
         | select(. == floor and . > $previous_generation)' \
        <<< "${identity_before}" 2>/dev/null
)"
[[ "${provider_instance_id}" =~ ${UUID_PATTERN} ]]

descriptor_path="${DISCOVERY_DIRECTORY}/${provider_instance_id}.sfp.json"
[[ -f "${descriptor_path}" ]]
descriptor_before="$(cat -- "${descriptor_path}")"
descriptor_provider_instance_id="$(
    jq -er '.provider_instance_id | strings' <<< "${descriptor_before}" \
        2>/dev/null
)"
descriptor_generation="$(
    jq -er '.endpoint_generation | numbers | select(. == floor and . >= 1)' \
        <<< "${descriptor_before}" 2>/dev/null
)"
transport_type="$(
    jq -er '.transport.type | strings' <<< "${descriptor_before}" \
        2>/dev/null
)"
socket_path="$(
    jq -er '.transport.socket_path | strings' <<< "${descriptor_before}" \
        2>/dev/null
)"

[[ "${descriptor_provider_instance_id}" == "${provider_instance_id}" ]]
[[ "${descriptor_generation}" == "${identity_generation}" ]]
[[ "${transport_type}" == "unix" ]]
[[ "${socket_path}" == \
    "${DISCOVERY_DIRECTORY}/${provider_instance_id}.${identity_generation}.sock" ]]
test -S "${socket_path}"

# Reject a mixed observation if either atomically published file changed while
# its peer was being inspected.
[[ "$(cat -- "${IDENTITY_PATH}")" == "${identity_before}" ]]
[[ "$(cat -- "${descriptor_path}")" == "${descriptor_before}" ]]
systemctl is-active --quiet "${UNIT_NAME}"

printf '%s\t%s\t%s\n' \
    "${provider_instance_id}" "${identity_generation}" "${socket_path}"

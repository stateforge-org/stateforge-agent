#!/usr/bin/env bash

set -euo pipefail

if (( $# != 3 )); then
    printf 'usage: %s PROVIDER_INSTANCE_ID ENDPOINT_GENERATION STATE\n' \
        "${0##*/}" >&2
    exit 2
fi

readonly PROVIDER_INSTANCE_ID="$1"
readonly ENDPOINT_GENERATION="$2"
readonly STATE="$3"
readonly UUID_PATTERN='^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

if [[ ! "${PROVIDER_INSTANCE_ID}" =~ ${UUID_PATTERN} \
    || ! "${ENDPOINT_GENERATION}" =~ ^[1-9][0-9]*$ \
    || ! "${STATE}" =~ ^[a-z][a-z_]*$ ]]; then
    exit 2
fi

journal="$(cat)"
match="$(
    grep -F 'Provider availability changed:' <<< "${journal}" \
        | grep -F "provider_instance_id=${PROVIDER_INSTANCE_ID} " \
        | grep -F "endpoint_generation=${ENDPOINT_GENERATION} " \
        | grep -F "state=${STATE} " \
        | tail -n 1 || true
)"
if [[ -n "${match}" ]]; then
    printf '%s\n' "${match}"
    exit 0
fi

exit 3

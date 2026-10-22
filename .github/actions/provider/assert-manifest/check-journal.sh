#!/usr/bin/env bash

set -euo pipefail

if (( $# != 2 )); then
    printf 'usage: %s PROVIDER_INSTANCE_ID ENDPOINT_GENERATION\n' \
        "${0##*/}" >&2
    exit 2
fi

readonly PROVIDER_INSTANCE_ID="$1"
readonly ENDPOINT_GENERATION="$2"
readonly UUID_PATTERN='^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

if [[ ! "${PROVIDER_INSTANCE_ID}" =~ ${UUID_PATTERN} \
    || ! "${ENDPOINT_GENERATION}" =~ ^[1-9][0-9]*$ ]]; then
    exit 2
fi

journal="$(cat)"
provider_field="provider_instance_id=${PROVIDER_INSTANCE_ID} "
generation_field="endpoint_generation=${ENDPOINT_GENERATION} "

rejection="$(
    grep -F 'Rejecting Provider manifest:' <<< "${journal}" \
        | grep -F "${provider_field}" \
        | grep -F "${generation_field}" \
        | tail -n 1 || true
)"
if [[ -n "${rejection}" ]]; then
    printf '%s\n' "${rejection}"
    exit 1
fi

acceptance="$(
    grep -F 'SFP session established:' <<< "${journal}" \
        | grep -F "${provider_field}" \
        | grep -F "${generation_field}" \
        | tail -n 1 || true
)"
if [[ -n "${acceptance}" ]]; then
    printf '%s\n' "${acceptance}"
    exit 0
fi

exit 3

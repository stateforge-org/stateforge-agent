#!/usr/bin/env bash

set -euo pipefail

if (( $# != 3 )); then
    printf 'usage: %s EXPECTED_INVOCATION CURRENT_INVOCATION STATE\n' \
        "${0##*/}" >&2
    exit 2
fi

readonly EXPECTED_INVOCATION_ID="$1"
readonly CURRENT_INVOCATION_ID="$2"
readonly STATE="$3"
readonly INVOCATION_ID_PATTERN='^[0-9a-f]{32}$'

if [[ ! "${EXPECTED_INVOCATION_ID}" =~ ${INVOCATION_ID_PATTERN} \
    || ! "${CURRENT_INVOCATION_ID}" =~ ${INVOCATION_ID_PATTERN} ]]; then
    exit 2
fi
if [[ "${CURRENT_INVOCATION_ID}" != "${EXPECTED_INVOCATION_ID}" ]]; then
    exit 4
fi

case "${STATE}" in
    active)
        ;;
    activating|reloading)
        exit 3
        ;;
    deactivating|failed|inactive)
        exit 4
        ;;
    *)
        exit 2
        ;;
esac

match=""
while IFS= read -r entry || [[ -n "${entry}" ]]; do
    if [[ -z "${entry}" ]]; then
        continue
    fi
    set +e
    candidate="$(
        jq -cer --arg invocation_id "${EXPECTED_INVOCATION_ID}" '
            select(._SYSTEMD_INVOCATION_ID == $invocation_id)
            | select((.MESSAGE | type) == "string")
            | select(.MESSAGE | contains("StateForge Agent started"))
            | .MESSAGE
        ' <<< "${entry}"
    )"
    candidate_status=$?
    set -e
    case "${candidate_status}" in
        0)
            match="${candidate}"
            ;;
        4)
            ;;
        *)
            exit 2
            ;;
    esac
done

if [[ -n "${match}" ]]; then
    printf '%s\n' "${match}"
    exit 0
fi

exit 3

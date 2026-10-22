#!/usr/bin/env bash

set -euo pipefail

if (( $# != 1 )); then
    printf 'usage: %s CONTAINER_NAME\n' "${0##*/}" >&2
    exit 2
fi

readonly CONTAINER_NAME="$1"

cursor="$(
    docker exec "${CONTAINER_NAME}" \
        journalctl -u stateforge-agent.service --no-pager \
            --show-cursor -n 1 \
        | sed -n 's/^-- cursor: //p'
)"
if [[ -z "${cursor}" || "${cursor}" == *$'\n'* ]]; then
    printf 'Could not capture the Agent journal cursor.\n' >&2
    exit 1
fi

printf '%s\n' "${cursor}"

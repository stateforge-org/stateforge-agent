#!/usr/bin/env bash

set -euo pipefail

if (( $# != 2 )); then
    printf 'usage: %s CHECKOUT_DIRECTORY EXPECTED_COMMIT\n' "${0##*/}" >&2
    exit 2
fi

readonly CHECKOUT_DIRECTORY="$1"
readonly EXPECTED_COMMIT="$2"

if [[ ! "${EXPECTED_COMMIT}" =~ ^[0-9a-f]{40}$ ]]; then
    printf 'Expected dependency commit is not a full SHA: %s\n' \
        "${EXPECTED_COMMIT}" >&2
    exit 2
fi

actual_commit="$(
    git -C "${CHECKOUT_DIRECTORY}" rev-parse --verify 'HEAD^{commit}'
)"
if [[ "${actual_commit}" != "${EXPECTED_COMMIT}" ]]; then
    printf 'Dependency checkout mismatch: expected %s, found %s.\n' \
        "${EXPECTED_COMMIT}" "${actual_commit}" >&2
    exit 1
fi

printf '%s\n' "${actual_commit}"

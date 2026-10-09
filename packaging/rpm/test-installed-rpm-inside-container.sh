#!/usr/bin/env bash

set -euo pipefail

readonly RPM_PATH="${RPM_PATH:?RPM_PATH is required}"
readonly EVIDENCE_DIRECTORY="/evidence"

collect_evidence() {
    mkdir -p "${EVIDENCE_DIRECTORY}"
    rpm -q stateforge-agent > "${EVIDENCE_DIRECTORY}/rpm-query.txt" 2>&1 || true
    rpm -V stateforge-agent > "${EVIDENCE_DIRECTORY}/rpm-verify.txt" 2>&1 || true
    systemctl cat stateforge-agent.service \
        > "${EVIDENCE_DIRECTORY}/service-unit.txt" 2>&1 || true
}

cleanup() {
    local status=$?

    collect_evidence
    if rpm -q stateforge-agent >/dev/null 2>&1; then
        rpm -e stateforge-agent \
            > "${EVIDENCE_DIRECTORY}/rpm-remove.txt" 2>&1 || status=$?
    fi
    if [[ -e /usr/bin/stateforge-agent ]]; then
        printf 'RPM removal left /usr/bin/stateforge-agent behind.\n' \
            >> "${EVIDENCE_DIRECTORY}/rpm-remove.txt"
        status=1
    fi
    exit "${status}"
}

main() {
    local executable import_path

    mkdir -p "${EVIDENCE_DIRECTORY}"
    trap cleanup EXIT

    rpm -Uvh --nodeps "${RPM_PATH}" \
        > "${EVIDENCE_DIRECTORY}/rpm-install.txt"
    rpm -q --qf '%{NAME} %{VERSION}-%{RELEASE} %{ARCH}\n' \
        stateforge-agent > "${EVIDENCE_DIRECTORY}/installed-package.txt"
    rpm -ql stateforge-agent > "${EVIDENCE_DIRECTORY}/installed-files.txt"
    # Dependency availability is a separate runtime gate until every Python
    # dependency is distributed as an RPM. Verify the package-owned bytes here.
    rpm -V --nodeps stateforge-agent \
        > "${EVIDENCE_DIRECTORY}/rpm-verify.txt"

    executable="$(command -v stateforge-agent)"
    import_path="$(cd / && python3 -c \
        'import pathlib,stateforge_agent; print(pathlib.Path(stateforge_agent.__file__).resolve())')"
    printf '%s\n' "${executable}" > "${EVIDENCE_DIRECTORY}/executable-path.txt"
    printf '%s\n' "${import_path}" > "${EVIDENCE_DIRECTORY}/import-path.txt"
    rpm -qf "$(readlink -f "${executable}")" \
        > "${EVIDENCE_DIRECTORY}/executable-owner.txt"
    rpm -qf /usr/lib/systemd/system/stateforge-agent.service \
        > "${EVIDENCE_DIRECTORY}/service-unit-owner.txt"
    [[ "${import_path}" == /usr/* ]] || {
        printf 'Agent import did not resolve from the installed RPM: %s\n' \
            "${import_path}" >&2
        exit 1
    }

    systemctl cat stateforge-agent.service >/dev/null
    collect_evidence
}

main "$@"

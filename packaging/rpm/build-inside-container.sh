#!/usr/bin/env bash

set -euo pipefail

readonly SOURCE_ROOT="/workspace"
readonly OUTPUT_DIRECTORY="/output"
readonly BUILD_ROOT="/var/tmp/stateforge-agent-rpm-build"
readonly RPMBUILD_ROOT="${BUILD_ROOT}/rpmbuild"

project_version() {
    python3 -c \
        'import pathlib, tomllib; print(tomllib.loads(pathlib.Path("pyproject.toml").read_text())["project"]["version"])'
}

copy_source_tree() {
    local destination="$1"

    mkdir -p "${destination}"
    tar -C "${SOURCE_ROOT}" \
        --exclude=.git \
        --exclude=.mypy_cache \
        --exclude=.pytest_cache \
        --exclude=.ruff_cache \
        --exclude=.venv \
        --exclude=build \
        --exclude=dist \
        --exclude='__pycache__' \
        --exclude='*.pyc' \
        -cf - . | tar -C "${destination}" -xf -
}

write_metadata() {
    local version="$1"
    local rpm_path="$2"
    local source_rpm_path="$3"

    VERSION="${version}" \
    RPM_PATH="${rpm_path}" \
    SOURCE_RPM_PATH="${source_rpm_path}" \
    SOURCE_REF="${SOURCE_REF}" \
    SOURCE_SHA="${SOURCE_SHA}" \
        python3 - <<'PY'
import hashlib
import json
import os
import pathlib
import subprocess


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


rpm_path = pathlib.Path(os.environ["RPM_PATH"])
source_rpm_path = pathlib.Path(os.environ["SOURCE_RPM_PATH"])
query = subprocess.run(
    [
        "rpm",
        "-qp",
        "--queryformat",
        "%{NAME}\n%{VERSION}\n%{RELEASE}\n%{ARCH}\n",
        str(rpm_path),
    ],
    check=True,
    capture_output=True,
    text=True,
).stdout.splitlines()
metadata = {
    "schema_version": 1,
    "project_version": os.environ["VERSION"],
    "source": {
        "commit": os.environ["SOURCE_SHA"],
        "ref": os.environ["SOURCE_REF"],
    },
    "rpm": {
        "filename": rpm_path.name,
        "sha256": sha256(rpm_path),
        "name": query[0],
        "version": query[1],
        "release": query[2],
        "architecture": query[3],
    },
    "source_rpm": {
        "filename": source_rpm_path.name,
        "sha256": sha256(source_rpm_path),
    },
}
pathlib.Path("/output/build-metadata.json").write_text(
    json.dumps(metadata, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY
}

main() {
    local version source_directory rpm_path source_rpm_path

    cd -- "${SOURCE_ROOT}"
    version="$(project_version)"
    if [[ ! "${version}" =~ ^[0-9][0-9A-Za-z._+~]*$ ]]; then
        printf 'Version is not valid for an RPM: %s\n' "${version}" >&2
        exit 1
    fi

    rm -rf "${BUILD_ROOT}"
    mkdir -p \
        "${RPMBUILD_ROOT}/BUILD" \
        "${RPMBUILD_ROOT}/BUILDROOT" \
        "${RPMBUILD_ROOT}/RPMS" \
        "${RPMBUILD_ROOT}/SOURCES" \
        "${RPMBUILD_ROOT}/SPECS" \
        "${RPMBUILD_ROOT}/SRPMS" \
        "${OUTPUT_DIRECTORY}"

    source_directory="${BUILD_ROOT}/stateforge-agent-${version}"
    copy_source_tree "${source_directory}"
    tar -C "${BUILD_ROOT}" -czf \
        "${RPMBUILD_ROOT}/SOURCES/stateforge-agent-${version}.tar.gz" \
        "stateforge-agent-${version}"
    cp "${SOURCE_ROOT}/packaging/rpm/SOURCES/"* \
        "${RPMBUILD_ROOT}/SOURCES/"
    cp "${SOURCE_ROOT}/packaging/rpm/SPECS/stateforge-agent.spec" \
        "${RPMBUILD_ROOT}/SPECS/"

    # stateforge-protocol is not distributed as an RPM yet. --nodeps bypasses
    # that packaging gap while still exercising the canonical RPM spec.
    rpmbuild \
        --define "_topdir ${RPMBUILD_ROOT}" \
        --define "version ${version}" \
        --nodeps \
        -ba "${RPMBUILD_ROOT}/SPECS/stateforge-agent.spec"

    rpm_path="$(find "${RPMBUILD_ROOT}/RPMS" -type f -name '*.rpm' -print -quit)"
    source_rpm_path="$(find "${RPMBUILD_ROOT}/SRPMS" -type f -name '*.rpm' -print -quit)"
    if [[ -z "${rpm_path}" || -z "${source_rpm_path}" ]]; then
        printf 'RPM build did not produce both binary and source packages.\n' >&2
        exit 1
    fi

    cp "${rpm_path}" "${source_rpm_path}" "${OUTPUT_DIRECTORY}/"
    rpm_path="${OUTPUT_DIRECTORY}/$(basename -- "${rpm_path}")"
    source_rpm_path="${OUTPUT_DIRECTORY}/$(basename -- "${source_rpm_path}")"
    write_metadata "${version}" "${rpm_path}" "${source_rpm_path}"
    (
        cd -- "${OUTPUT_DIRECTORY}"
        sha256sum -- *.rpm > SHA256SUMS
    )
    chown -R "${HOST_UID:-0}:${HOST_GID:-0}" "${OUTPUT_DIRECTORY}"
}

main "$@"

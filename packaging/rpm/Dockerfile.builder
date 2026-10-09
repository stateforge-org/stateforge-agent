FROM fedora:42

RUN dnf -y install \
        findutils \
        gzip \
        python3-devel \
        python3-hatchling \
        pyproject-rpm-macros \
        rpm-build \
        systemd-rpm-macros \
        tar \
    && dnf clean all

WORKDIR /workspace

ENTRYPOINT ["/workspace/packaging/rpm/build-inside-container.sh"]

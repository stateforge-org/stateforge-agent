%global import_name stateforge_agent

# The repository build command reads this macro from pyproject.toml. Refuse a
# direct build without it so RPM and Python distribution versions cannot drift.
%{!?version:%{error:The version macro must be defined from pyproject.toml}}

# RHEL 9's default Python is older than the project's Python 3.11 minimum.
# Use the supported Python 3.11 stack there; Fedora and newer RHEL releases
# use their default Python 3 interpreter.
%if 0%{?rhel} == 9
%global python3_pkgversion 3.11
%endif

Name:           stateforge-agent
Version:        %{version}
Release:        1%{?dist}
Summary:        Secure execution agent for the StateForge platform

License:        Apache-2.0
URL:            https://github.com/stateforge-org/stateforge-agent
Source0:        %{name}-%{version}.tar.gz
Source1:        %{name}.service
Source2:        %{name}.sysusers
Source3:        %{name}.toml
Source4:        %{name}.tmpfiles

BuildArch:      noarch

BuildRequires:  python%{python3_pkgversion}-devel
BuildRequires:  pyproject-rpm-macros
BuildRequires:  systemd-rpm-macros

%{?systemd_requires}
%{?sysusers_requires_compat}

%description
StateForge Agent is a long-running execution daemon that coordinates local
StateForge Providers with the StateForge Control Plane. It executes authorized
operations inside the customer environment, invokes policy-selected Backup
Engines as operation tools, and reports normalized progress and results to the
Control Plane.


%generate_buildrequires
%pyproject_buildrequires


%prep
%autosetup -n %{name}-%{version}


%build
%pyproject_wheel


%install
%pyproject_install
%pyproject_save_files %{import_name}

install -Dpm 0644 %{SOURCE1} \
    %{buildroot}%{_unitdir}/%{name}.service
install -Dpm 0644 %{SOURCE2} \
    %{buildroot}%{_sysusersdir}/%{name}.conf
install -Dpm 0644 %{SOURCE3} \
    %{buildroot}%{_sysconfdir}/%{name}/config.toml
install -Dpm 0644 %{SOURCE4} \
    %{buildroot}%{_tmpfilesdir}/%{name}.conf
install -d -m 0750 \
    %{buildroot}%{_sysconfdir}/%{name}/conf.d


%check
# The package root intentionally has no runtime imports. This verifies the
# wheel-installed package without requiring every runtime dependency to exist
# as a Fedora RPM during the packaging-only build.
PYTHONPATH=%{buildroot}%{python3_sitelib} \
    %{python3} -c 'import %{import_name}'


%pre
%sysusers_create_compat %{SOURCE2}
%tmpfiles_create_package %{name} %{SOURCE4}


%post
%systemd_post %{name}.service


%preun
%systemd_preun %{name}.service


%postun
%systemd_postun_with_restart %{name}.service


%files -f %{pyproject_files}
%license LICENSE
%doc README.md
%{_bindir}/%{name}
%{_unitdir}/%{name}.service
%{_sysusersdir}/%{name}.conf
%{_tmpfilesdir}/%{name}.conf
%dir %attr(0750,root,stateforge-agent) %{_sysconfdir}/%{name}
%config(noreplace) %attr(0640,root,stateforge-agent) %{_sysconfdir}/%{name}/config.toml
%dir %attr(0750,root,stateforge-agent) %{_sysconfdir}/%{name}/conf.d


%changelog
* Sun Aug 16 2026 StateForge Release Engineering - 0.1.0-1
- Initial RPM package

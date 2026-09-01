"""Run the long-lived StateForge Agent process."""

from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path
from uuid import UUID, uuid4

from stateforge_agent import __version__
from stateforge_agent._config.models import AgentConfiguration
from stateforge_agent._runtime.layout import (
    SYSTEM_RUNTIME_LAYOUT,
    RuntimeLayout,
)
from stateforge_agent._runtime.logging import configure_logging
from stateforge_agent._runtime.provider_descriptor import (
    PROVIDER_DESCRIPTOR_SUFFIX,
    ProviderDescriptorError,
    ProviderEndpointDescriptor,
    load_provider_descriptor,
)
from stateforge_agent._runtime.provider_monitor import (
    ProviderDirectoryError,
    ProviderDirectoryEvent,
    ProviderDirectoryEventKind,
    ProviderDirectoryMonitor,
)
from stateforge_agent._sfp import (
    ProviderSessionManager,
    SfpConnectionError,
    SfpVersion,
)

_LOGGER = logging.getLogger(__name__)
_SUPPORTED_SFP_VERSIONS = (SfpVersion(major=1, minor=0),)
_UUID_VERSION = 4


def run_agent(
    configuration: AgentConfiguration,
    *,
    runtime_layout: RuntimeLayout = SYSTEM_RUNTIME_LAYOUT,
) -> int:
    """Run the Agent until a normal process-termination signal arrives."""
    configure_logging(configuration.logging)
    try:
        return asyncio.run(_run_agent(configuration, runtime_layout))
    except KeyboardInterrupt:
        return 0


async def _run_agent(
    configuration: AgentConfiguration,
    runtime_layout: RuntimeLayout = SYSTEM_RUNTIME_LAYOUT,
) -> int:
    """Prepare runtime services and drive the monitoring loop."""
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed_signals = _install_signal_handlers(loop, stop_event)
    monitor = ProviderDirectoryMonitor(
        runtime_layout.provider_discovery_directory
    )

    def reload_provider_descriptor(
        provider_instance_id: UUID,
    ) -> ProviderEndpointDescriptor:
        """Reload one canonical descriptor before a reconnect attempt."""
        return load_provider_descriptor(
            monitor.directory
            / f"{provider_instance_id}{PROVIDER_DESCRIPTOR_SUFFIX}",
            discovery_directory=monitor.directory,
        )

    sessions = ProviderSessionManager(
        agent_runtime_id=uuid4(),
        agent_software_version=__version__,
        supported_sfp_versions=_SUPPORTED_SFP_VERSIONS,
        reconnect_initial_delay_seconds=(
            configuration.providers.reconnect_initial_delay_seconds
        ),
        reconnect_max_delay_seconds=(
            configuration.providers.reconnect_max_delay_seconds
        ),
        descriptor_loader=reload_provider_descriptor,
    )

    try:
        monitor.prepare()
        _LOGGER.info("StateForge Agent started")
        async for events in monitor.watch(stop_event):
            await _process_events(monitor, sessions, events)
    except ProviderDirectoryError:
        _LOGGER.exception("Provider directory monitoring failed")
        return 1
    finally:
        await sessions.close()
        for installed_signal in installed_signals:
            loop.remove_signal_handler(installed_signal)

    _LOGGER.info("StateForge Agent stopped")
    return 0


def _install_signal_handlers(
    loop: asyncio.AbstractEventLoop,
    stop_event: asyncio.Event,
) -> tuple[signal.Signals, ...]:
    """Arrange graceful shutdown without doing work in signal handlers."""
    installed: list[signal.Signals] = []
    for handled_signal in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(handled_signal, stop_event.set)
        except (NotImplementedError, RuntimeError):
            continue
        installed.append(handled_signal)
    return tuple(installed)


async def _process_events(
    monitor: ProviderDirectoryMonitor,
    sessions: ProviderSessionManager,
    events: tuple[ProviderDirectoryEvent, ...],
) -> None:
    """Turn validated discovery events into active local SFP sessions."""
    for event in events:
        if event.kind is ProviderDirectoryEventKind.REMOVED:
            _LOGGER.info(
                "Provider endpoint descriptor removed: path=%s",
                event.entry.path,
            )
            provider_instance_id = _descriptor_provider_instance_id(
                event.entry.path
            )
            if provider_instance_id is not None:
                await sessions.remove(provider_instance_id)
            continue

        try:
            descriptor = load_provider_descriptor(
                event.entry.path,
                discovery_directory=monitor.directory,
            )
        except ProviderDescriptorError as error:
            _LOGGER.warning(
                "Ignoring invalid Provider endpoint descriptor: path=%s "
                "error=%s",
                event.entry.path,
                error,
            )
            continue

        try:
            session = await sessions.establish(descriptor)
        except SfpConnectionError as error:
            _LOGGER.warning(
                "Cannot establish SFP session: provider_instance_id=%s "
                "endpoint_generation=%s error=%s",
                descriptor.provider_instance_id,
                descriptor.endpoint_generation,
                error,
            )
            continue

        _LOGGER.info(
            "SFP session established: event=%s provider_instance_id=%s "
            "endpoint_generation=%s sfp_version=%s "
            "provider_runtime_id=%s session_id=%s",
            event.kind.value,
            session.provider_instance_id,
            session.endpoint_generation,
            session.selected_sfp_version,
            session.provider_runtime_id,
            session.session_id,
        )


def _descriptor_provider_instance_id(path: Path) -> UUID | None:
    """Recover a Provider identity from a removed canonical descriptor name."""
    if not path.name.endswith(PROVIDER_DESCRIPTOR_SUFFIX):
        return None
    value = path.name.removesuffix(PROVIDER_DESCRIPTOR_SUFFIX)
    try:
        provider_instance_id = UUID(value)
    except ValueError:
        return None
    if (
        provider_instance_id.version != _UUID_VERSION
        or str(provider_instance_id) != value
    ):
        return None
    return provider_instance_id

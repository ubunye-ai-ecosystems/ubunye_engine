"""Built-in engine hooks wrapping the existing telemetry backends."""

from ubunye.telemetry.hooks.events import EventLoggerHook
from ubunye.telemetry.hooks.monitors import LegacyMonitorsHook, MonitorHook
from ubunye.telemetry.hooks.otel import OTelHook
from ubunye.telemetry.hooks.prometheus import PrometheusHook

__all__ = [
    "EventLoggerHook",
    "LegacyMonitorsHook",
    "MonitorHook",
    "OTelHook",
    "PrometheusHook",
]

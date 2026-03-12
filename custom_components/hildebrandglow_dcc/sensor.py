"""Platform for sensor integration."""

from __future__ import annotations

from abc import ABC, abstractmethod
import asyncio
from collections.abc import Callable
from datetime import datetime, time, timedelta
import logging

import requests
from requests.exceptions import ConnectionError, HTTPError, Timeout

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfEnergy
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.update_coordinator import (
    CoordinatorEntity,
    DataUpdateCoordinator,
    UpdateFailed,
)
from homeassistant.util import dt as dt_util

from .const import CONF_DAILY_INTERVAL, CONF_TARIFF_INTERVAL, DOMAIN

_LOGGER = logging.getLogger(__name__)

# --- COORDINATOR CLASSES ---


class DataCoordinator(DataUpdateCoordinator):
    """Data update coordinator for daily usage and cost sensors."""

    def __init__(self, hass: HomeAssistant, glowmarkt_resource, daily_interval):
        """Initialize daily data coordinator."""
        self.resource = glowmarkt_resource
        super().__init__(
            hass,
            _LOGGER,
            name=f"Daily Data {glowmarkt_resource.classifier}",
            update_interval=timedelta(minutes=daily_interval),
        )

    async def _async_update_data(self):
        """Fetch data from daily usage API endpoint."""
        _LOGGER.debug(
            "DataCoordinator updating for resource %s", self.resource.classifier
        )
        try:
            value = await daily_data(self.hass, self.resource)
            # If value is None, do not raise an exception,
            # which allows the coordinator to keep its previous state.
            if value is None:
                return None
            return value
        except HTTPError as ex:
            raise UpdateFailed(
                f"HTTP Error fetching daily data: {ex}, Status Code: {ex.response.status_code}"
            ) from ex
        except Timeout as ex:
            raise UpdateFailed(f"Timeout fetching daily data: {ex}") from ex
        except ConnectionError as ex:
            raise UpdateFailed(f"Connection error fetching daily data: {ex}") from ex
        except Exception as ex:
            _LOGGER.exception("Unexpected exception fetching daily data: %s", ex)
            raise UpdateFailed(f"Unknown error fetching daily data: {ex}") from ex


class ExportDataCoordinator(DataUpdateCoordinator):
    """Data update coordinator for export sensors.

    Fetches half-hourly export data and imports it into HA's long-term
    statistics via async_import_statistics, ensuring data is attributed
    to the correct timestamps even when the DCC data arrives late.
    Also provides today's/yesterday's total as a fallback sensor value.
    """

    def __init__(self, hass: HomeAssistant, glowmarkt_resource, daily_interval):
        """Initialize export data coordinator."""
        self.resource = glowmarkt_resource
        self._statistic_id = None  # Set by the Export sensor after creation
        super().__init__(
            hass,
            _LOGGER,
            name=f"Export Data {glowmarkt_resource.classifier}",
            update_interval=timedelta(minutes=daily_interval),
        )

    async def _async_update_data(self):
        """Fetch export data and import historical statistics."""
        _LOGGER.debug(
            "ExportDataCoordinator updating for resource %s", self.resource.classifier
        )
        try:
            # Import historical half-hourly data into HA statistics
            if self._statistic_id:
                await self._import_export_statistics()

            # Also return today's/yesterday's value for the sensor display
            value = await daily_export_data(self.hass, self.resource)
            if value is None:
                return None
            return value
        except HTTPError as ex:
            raise UpdateFailed(
                f"HTTP Error fetching export data: {ex}, Status Code: {ex.response.status_code}"
            ) from ex
        except Timeout as ex:
            raise UpdateFailed(f"Timeout fetching export data: {ex}") from ex
        except ConnectionError as ex:
            raise UpdateFailed(f"Connection error fetching export data: {ex}") from ex
        except Exception as ex:
            _LOGGER.exception("Unexpected exception fetching export data: %s", ex)
            raise UpdateFailed(f"Unknown error fetching export data: {ex}") from ex

    async def _import_export_statistics(self):
        """Fetch half-hourly export data for the last 2 days and import as statistics."""
        from homeassistant.components.recorder.models import StatisticData, StatisticMetaData  # noqa: E501
        from homeassistant.components.recorder.statistics import (
            async_import_statistics,
            statistics_during_period,
            get_instance,
        )

        now = dt_util.utcnow()
        utc_offset = -int(dt_util.now().utcoffset().total_seconds() / 60)

        # Fetch last 2 days of half-hourly data to catch delayed DCC data
        t_from = (now - timedelta(days=2)).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + timedelta(minutes=utc_offset)
        t_to = now.replace(second=0, microsecond=0)

        try:
            readings = await self.hass.async_add_executor_job(
                self.resource.get_readings, t_from, t_to, "PT30M", "sum", utc_offset
            )
        except Exception as ex:
            _LOGGER.warning("Failed to fetch half-hourly export data: %s", ex)
            return

        if not readings:
            _LOGGER.debug("No half-hourly export readings returned")
            return

        # Aggregate half-hourly readings into hourly buckets
        hourly = {}
        for ts, val in readings:
            if isinstance(ts, (int, float)):
                dt = datetime.utcfromtimestamp(ts).replace(
                    minute=0, second=0, microsecond=0, tzinfo=dt_util.UTC
                )
            else:
                dt = ts.replace(minute=0, second=0, microsecond=0)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=dt_util.UTC)
            if dt not in hourly:
                hourly[dt] = 0.0
            hourly[dt] += val.value

        if not hourly:
            return

        # Get last known sum from HA statistics to continue the cumulative total
        sorted_hours = sorted(hourly.items())
        earliest = sorted_hours[0][0]

        last_sum = 0.0
        try:
            last_stats = await get_instance(self.hass).async_add_executor_job(
                statistics_during_period,
                self.hass,
                earliest - timedelta(days=7),
                earliest,
                {self._statistic_id},
                "hour",
                None,
                {"sum"},
            )
            if (
                self._statistic_id in last_stats
                and len(last_stats[self._statistic_id]) > 0
            ):
                last_sum = last_stats[self._statistic_id][-1]["sum"]
                _LOGGER.debug("Last known export sum: %s", last_sum)
        except Exception as ex:
            _LOGGER.debug("Could not fetch last statistics: %s", ex)

        # Build statistics entries
        stats = []
        cumulative = last_sum
        for dt, kwh in sorted_hours:
            cumulative += kwh
            stats.append(
                StatisticData(
                    start=dt,
                    state=round(kwh, 6),
                    sum=round(cumulative, 6),
                )
            )

        if not stats:
            return

        metadata = StatisticMetaData(
            has_mean=False,
            has_sum=True,
            name=f"{self.resource.classifier} export",
            source="recorder",
            statistic_id=self._statistic_id,
            unit_of_measurement="kWh",
        )

        _LOGGER.debug(
            "Importing %d hourly export statistics for %s",
            len(stats),
            self._statistic_id,
        )
        async_import_statistics(self.hass, metadata, stats)


class TariffCoordinator(DataUpdateCoordinator):
    """Data update coordinator for the tariff sensors."""

    def __init__(self, hass: HomeAssistant, resource, tariff_interval) -> None:
        """Initialize tariff coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            name=f"Tariff Data {resource.classifier}",
            update_interval=timedelta(minutes=tariff_interval),
        )
        self.resource = resource

    async def _async_update_data(self):
        """Fetch data from tariff API endpoint."""
        _LOGGER.debug(
            "TariffCoordinator updating for resource %s", self.resource.classifier
        )
        try:
            tariff = await tariff_data(self.hass, self.resource)
            if tariff is None:
                # If tariff_data returns None, it means no data was successfully fetched.
                # Raise UpdateFailed to mark coordinator unavailable and propagate to sensors.
                raise UpdateFailed(
                    f"No tariff data received for {self.resource.classifier}"
                )
            return tariff
        except HTTPError as ex:
            _LOGGER.error(
                "HTTP Error fetching tariff data for %s: %s, Status Code: %s",
                self.resource.classifier,
                ex,
                ex.response.status_code,
            )
            raise UpdateFailed(f"Failed to fetch tariff data: {ex}") from ex
        except Exception as ex:
            _LOGGER.exception(
                "Error fetching tariff data for %s: %s", self.resource.classifier, ex
            )
            raise UpdateFailed(f"Failed to fetch tariff data: {ex}") from ex


# --- HELPER FUNCTIONS ---


def supply_type(resource) -> str:
    """Return supply type."""
    if "electricity.consumption" in resource.classifier:
        return "electricity"
    if "electricity.export" in resource.classifier:
        return "electricity"
    if "gas.consumption" in resource.classifier:
        return "gas"
    _LOGGER.error("Unknown classifier: %s. Please open an issue", resource.classifier)
    return "unknown"


def device_name(resource, virtual_entity) -> str:
    """Return device name. Includes name of virtual entity if it exists."""
    supply = supply_type(resource)
    if virtual_entity.name is not None:
        name = f"{virtual_entity.name} smart {supply} meter"
    else:
        name = f"Smart {supply} meter"
    return name


async def daily_data(hass: HomeAssistant, resource) -> float:
    """Get Summ for the day from the API."""
    _LOGGER.debug("Fetching today's data")
    now = dt_util.utcnow()
    utc_offset = -int(dt_util.now().utcoffset().total_seconds() / 60)
    _LOGGER.debug("UTC offset is: %s", utc_offset)

    try:
        await hass.async_add_executor_job(resource.catchup)
        _LOGGER.debug(
            "Successful GET to https://api.glowmarkt.com/api/v0-1/resource/%s/catchup",
            resource.id,
        )
    except HTTPError as ex:
        _LOGGER.error("HTTP Error: %s, Status Code: %s", ex, ex.response.status_code)
    except Timeout as ex:
        _LOGGER.error("Timeout: %s", ex)
    except ConnectionError as ex:
        _LOGGER.error("Cannot connect: %s", ex)
    except Exception as ex:  # pylint: disable=broad-except
        _LOGGER.exception("Unexpected exception: %s. Please open an issue", ex)
    t_from = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
        minutes=utc_offset
    )
    t_to = now.replace(second=0, microsecond=0)

    try:
        _LOGGER.debug(
            "Get readings from %s to %s for %s when now= %s",
            t_from,
            t_to,
            resource.classifier,
            now,
        )
        readings = await hass.async_add_executor_job(
            resource.get_readings, t_from, t_to, "P1D", "sum", utc_offset
        )
        _LOGGER.debug("Successfully got daily usage for resource id %s", resource.id)
        _LOGGER.debug(
            "Readings for %s has %s entries", resource.classifier, len(readings)
        )
        if not readings:
            _LOGGER.debug("nothing returned")
        else:
            v = readings[0][1].value
            _LOGGER.debug(
                "%s First reading %s at %s",
                resource.classifier,
                readings[0][0],
                readings[0][1].value,
            )
            if len(readings) > 1:
                v += readings[1][1].value
                _LOGGER.debug(
                    "%s Second reading %s at %s",
                    resource.classifier,
                    readings[1][0],
                    readings[1][1].value,
                )
            return v
    except HTTPError as ex:
        _LOGGER.error(
            "HTTP Error fetching daily data: %s, Status Code: %s",
            ex,
            ex.response.status_code,
        )
        return None
    except Timeout as ex:
        _LOGGER.error("Timeout: %s", ex)
        return None
    except ConnectionError as ex:
        _LOGGER.error("Cannot connect: %s", ex)
        return None
    except Exception as ex:
        _LOGGER.exception("Unexpected exception: %s. Please open an issue", ex)
        return None


async def daily_export_data(hass: HomeAssistant, resource) -> float:
    """Get export total from the API, with fallback to yesterday's settled data.

    Export data from the DCC is typically delayed by up to 24 hours.
    We first try to fetch today's data. If the API returns 0 or no data,
    we fall back to yesterday's settled total so the sensor always shows
    a meaningful value.
    """
    _LOGGER.debug("Fetching export data")
    now = dt_util.utcnow()
    utc_offset = -int(dt_util.now().utcoffset().total_seconds() / 60)

    try:
        await hass.async_add_executor_job(resource.catchup)
    except HTTPError as ex:
        _LOGGER.error("HTTP Error: %s, Status Code: %s", ex, ex.response.status_code)
    except Timeout as ex:
        _LOGGER.error("Timeout: %s", ex)
    except ConnectionError as ex:
        _LOGGER.error("Cannot connect: %s", ex)
    except Exception as ex:  # pylint: disable=broad-except
        _LOGGER.exception("Unexpected exception: %s. Please open an issue", ex)

    # Try today's data first (same as daily_data)
    today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
        minutes=utc_offset
    )
    t_to = now.replace(second=0, microsecond=0)
    today_value = await _fetch_export_readings(
        hass, resource, today_midnight, t_to, utc_offset, "today"
    )

    if today_value is not None and today_value > 0:
        _LOGGER.debug("Using today's export value: %s", today_value)
        return today_value

    # Today returned 0 or None — fall back to yesterday's settled data
    yesterday_midnight = today_midnight - timedelta(days=1)
    yesterday_value = await _fetch_export_readings(
        hass, resource, yesterday_midnight, today_midnight, utc_offset, "yesterday"
    )

    if yesterday_value is not None and yesterday_value > 0:
        _LOGGER.debug(
            "Today's export is 0/unavailable, using yesterday's settled value: %s",
            yesterday_value,
        )
        return yesterday_value

    # Both are 0 or None — return today's value (likely genuinely 0)
    _LOGGER.debug("Both today and yesterday export are 0 or unavailable")
    return today_value


async def _fetch_export_readings(
    hass: HomeAssistant, resource, t_from, t_to, utc_offset, label: str
) -> float | None:
    """Fetch export readings for a given time range."""
    try:
        _LOGGER.debug(
            "Get %s export readings from %s to %s for %s",
            label,
            t_from,
            t_to,
            resource.classifier,
        )
        readings = await hass.async_add_executor_job(
            resource.get_readings, t_from, t_to, "P1D", "sum", utc_offset
        )
        if not readings:
            _LOGGER.debug("No %s export readings returned", label)
            return None

        v = readings[0][1].value
        if len(readings) > 1:
            v += readings[1][1].value
        return v
    except HTTPError as ex:
        _LOGGER.error(
            "HTTP Error fetching %s export data: %s, Status Code: %s",
            label,
            ex,
            ex.response.status_code,
        )
        return None
    except Timeout as ex:
        _LOGGER.error("Timeout fetching %s export data: %s", label, ex)
        return None
    except ConnectionError as ex:
        _LOGGER.error("Cannot connect fetching %s export data: %s", label, ex)
        return None
    except Exception as ex:
        _LOGGER.exception(
            "Unexpected exception fetching %s export data: %s. Please open an issue",
            label,
            ex,
        )
        return None


async def tariff_data(hass: HomeAssistant, resource):
    """Get tariff data from the API."""
    try:
        tariff = await hass.async_add_executor_job(resource.get_tariff)
        _LOGGER.debug(
            "Successful GET to https://api.glowmarkt.com/api/v0-1/resource/%s/tariff",
            resource.id,
        )
        return tariff
    except UnboundLocalError:
        supply = supply_type(resource)
        _LOGGER.warning(
            "No tariff data found for %s meter (id: %s). If you don't see tariff data for this meter in the Bright app, please disable the associated rate and standing charge sensors",
            supply,
            resource.id,
        )
        return None
    except HTTPError as ex:
        _LOGGER.error(
            "HTTP Error fetching tariff data for %s: %s, Status Code: %s",
            resource.classifier,
            ex,
            ex.response.status_code,
        )
        return None
    except Timeout as ex:
        _LOGGER.error(
            "Timeout fetching tariff data for %s: %s", resource.classifier, ex
        )
        return None
    except ConnectionError as ex:
        _LOGGER.error(
            "Connection error fetching tariff data for %s: %s", resource.classifier, ex
        )
        return None
    except Exception as ex:
        _LOGGER.exception(
            "Unexpected exception fetching tariff data for %s: %s. Please open an issue",
            resource.classifier,
            ex,
        )
        return None


async def _delayed_first_refresh(coordinator: DataUpdateCoordinator, delay: int = 5):
    """Perform first refresh after a delay."""
    _LOGGER.debug(
        "Scheduling delayed first refresh for %s in %d seconds", coordinator.name, delay
    )
    await asyncio.sleep(delay)
    await coordinator.async_request_refresh()
    _LOGGER.debug("Completed delayed first refresh for %s", coordinator.name)


# --- SENSOR BASE CLASS ---


class GlowDCCSensor(CoordinatorEntity, SensorEntity, ABC):
    """Base class for Hildebrand Glow DCC sensors."""

    def __init__(
        self, coordinator: DataUpdateCoordinator, resource, virtual_entity
    ) -> None:
        super().__init__(coordinator)
        self.resource = resource
        self.virtual_entity = virtual_entity

    @property
    def device_info(self) -> DeviceInfo:
        """Return device information."""
        identifier_resource = self.resource
        if hasattr(self, "meter") and self.meter is not None:
            identifier_resource = self.meter.resource

        return DeviceInfo(
            identifiers={(DOMAIN, identifier_resource.id)},
            manufacturer="Hildebrand",
            model="Glow (DCC)",
            name=device_name(identifier_resource, self.virtual_entity),
        )

    @callback
    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        if self.coordinator.data is not None:
            self._update_native_value(self.coordinator.data)
        self.async_write_ha_state()

    @abstractmethod
    def _update_native_value(self, data):
        """Abstract method to set the native value based on coordinator data."""
        pass


# --- SENSOR CLASSES ---


class Usage(GlowDCCSensor):
    """Sensor object for daily usage."""

    _attr_device_class = SensorDeviceClass.ENERGY
    _attr_has_entity_name = True
    _attr_name = "Usage (today)"
    _attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
    _attr_state_class = SensorStateClass.TOTAL_INCREASING

    def __init__(
        self, coordinator: DataUpdateCoordinator, resource, virtual_entity
    ) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator, resource, virtual_entity)
        self._attr_unique_id = f"{resource.id}_usage_today"
        _LOGGER.debug("Created Usage sensor with unique_id: %s", self._attr_unique_id)

    @property
    def icon(self) -> str | None:
        """Icon to use in the frontend."""
        if self.resource.classifier == "gas.consumption":
            return "mdi:fire"
        return None

    @callback
    def _update_native_value(self, data: float) -> None:
        """Set the native value for usage sensor from coordinator data."""
        self._attr_native_value = round(data, 2)


class Cost(GlowDCCSensor):
    """Sensor usage for daily cost."""

    _attr_device_class = SensorDeviceClass.MONETARY
    _attr_has_entity_name = True
    _attr_name = "Cost (today)"
    _attr_native_unit_of_measurement = "GBP"
    _attr_state_class = SensorStateClass.TOTAL

    def __init__(
        self, coordinator: DataUpdateCoordinator, resource, virtual_entity
    ) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator, resource, virtual_entity)
        self.meter = None
        self._attr_unique_id = f"{resource.id}_cost_today"
        _LOGGER.debug("Created Cost sensor with unique_id: %s", self._attr_unique_id)

    @callback
    def _update_native_value(self, data: float) -> None:
        """Set the native value for cost sensor from coordinator data."""
        self._attr_native_value = round(data / 100, 2)


class Export(GlowDCCSensor):
    """Sensor object for daily electricity export.

    Shows today's export data when available from the API, falling back
    to yesterday's settled total when today's data hasn't arrived yet
    (DCC export data is typically delayed by up to 24 hours).
    """

    _attr_device_class = SensorDeviceClass.ENERGY
    _attr_has_entity_name = True
    _attr_name = "Export (today)"
    _attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
    _attr_state_class = SensorStateClass.TOTAL_INCREASING
    _attr_icon = "mdi:solar-power"

    def __init__(
        self, coordinator: ExportDataCoordinator, resource, virtual_entity
    ) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator, resource, virtual_entity)
        self._attr_unique_id = f"{resource.id}_export_today"
        _LOGGER.debug(
            "Created Export sensor with unique_id: %s", self._attr_unique_id
        )

    async def async_added_to_hass(self) -> None:
        """Register the statistic_id with the coordinator when added to HA."""
        await super().async_added_to_hass()
        # Tell the coordinator our entity_id so it can import statistics
        if hasattr(self.coordinator, "_statistic_id"):
            self.coordinator._statistic_id = self.entity_id
            _LOGGER.debug(
                "Registered statistic_id %s with ExportDataCoordinator",
                self.entity_id,
            )

    @callback
    def _update_native_value(self, data: float) -> None:
        """Set the native value for export sensor from coordinator data."""
        self._attr_native_value = round(data, 2)

    @property
    def device_info(self) -> DeviceInfo:
        """Return device information, grouped with the electricity meter."""
        return DeviceInfo(
            identifiers={(DOMAIN, self.resource.id)},
            manufacturer="Hildebrand",
            model="Glow (DCC)",
            name=device_name(self.resource, self.virtual_entity),
        )


class Standing(CoordinatorEntity, SensorEntity):
    """An entity using CoordinatorEntity."""

    _attr_device_class = SensorDeviceClass.MONETARY
    _attr_has_entity_name = True
    _attr_name = "Standing charge"
    _attr_native_unit_of_measurement = "GBP"
    _attr_entity_registry_enabled_default = False

    def __init__(
        self, coordinator: DataUpdateCoordinator, resource, virtual_entity
    ) -> None:
        """Pass coordinator to CoordinatorEntity."""
        super().__init__(coordinator)

        self._attr_unique_id = f"{resource.id}_standing_charge"
        _LOGGER.debug(
            "Created Standing sensor with unique_id: %s", self._attr_unique_id
        )

        self.resource = resource
        self.virtual_entity = virtual_entity

    @callback
    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        if self.coordinator.data:
            value = (
                float(self.coordinator.data.current_rates.standing_charge.value) / 100
            )
            self._attr_native_value = round(value, 4)
            self.async_write_ha_state()

    @property
    def device_info(self) -> DeviceInfo:
        """Return device information."""
        return DeviceInfo(
            identifiers={(DOMAIN, self.resource.id)},
            manufacturer="Hildebrand",
            model="Glow (DCC)",
            name=device_name(self.resource, self.virtual_entity),
        )


class Rate(CoordinatorEntity, SensorEntity):
    """An entity using CoordinatorEntity."""

    _attr_device_class = None
    _attr_has_entity_name = True
    _attr_icon = "mdi:cash-multiple"
    _attr_name = "Rate"
    _attr_native_unit_of_measurement = "GBP/kWh"
    _attr_entity_registry_enabled_default = False

    def __init__(
        self, coordinator: DataUpdateCoordinator, resource, virtual_entity
    ) -> None:
        """Pass coordinator to CoordinatorEntity."""
        super().__init__(coordinator)

        self._attr_unique_id = f"{resource.id}_rate"
        _LOGGER.debug("Created Rate sensor with unique_id: %s", self._attr_unique_id)

        self.resource = resource
        self.virtual_entity = virtual_entity

    @callback
    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        if self.coordinator.data:
            value = float(self.coordinator.data.current_rates.rate.value) / 100
            self._attr_native_value = round(value, 4)
            self.async_write_ha_state()

    @property
    def device_info(self) -> DeviceInfo:
        """Return device information."""
        return DeviceInfo(
            identifiers={(DOMAIN, self.resource.id)},
            manufacturer="Hildebrand",
            model="Glow (DCC)",
            name=device_name(self.resource, self.virtual_entity),
        )


# --- ASYNC SETUP ENTRY FUNCTION ---


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: Callable
) -> bool:
    """Set up the sensor platform."""
    _LOGGER.debug("Starting async_setup_entry in sensor platform.")
    entities: list = []
    meters: dict = {}
    daily_coordinators: dict[str, DataCoordinator] = {}
    tariff_coordinators: dict[str, TariffCoordinator] = {}

    glowmarkt = hass.data[DOMAIN][entry.entry_id]["client"]
    # Get the daily and tariff intervals from the stored data, with a fallback default.
    daily_interval = hass.data[DOMAIN][entry.entry_id].get(CONF_DAILY_INTERVAL, 15)
    tariff_interval = hass.data[DOMAIN][entry.entry_id].get(CONF_TARIFF_INTERVAL, 60)

    virtual_entities: dict = {}
    try:
        _LOGGER.debug("Fetching virtual entities from API...")
        virtual_entities = await hass.async_add_executor_job(
            glowmarkt.get_virtual_entities
        )
        _LOGGER.debug("Successful GET to %svirtualentity", glowmarkt.url)
    except HTTPError as ex:
        _LOGGER.error(
            "HTTP Error fetching virtual entities: Status Code %s - %s",
            ex.response.status_code,
            ex,
        )
        return False
    except (Timeout, ConnectionError) as ex:
        _LOGGER.error("Failed to get virtual entities: %s", ex)
        return False
    except Exception as ex:
        _LOGGER.exception("Unexpected exception: %s. Please open an issue", ex)
        return False

    for virtual_entity in virtual_entities:
        _LOGGER.debug("Found virtual entity: %s", virtual_entity.name)
        resources: dict = {}
        try:
            _LOGGER.debug(
                "Fetching resources for virtual entity %s...", virtual_entity.name
            )
            resources = await hass.async_add_executor_job(virtual_entity.get_resources)
            _LOGGER.debug(
                "Successful GET to %svirtualentity/%s/resources",
                glowmarkt.url,
                virtual_entity.id,
            )
        except HTTPError as ex:
            _LOGGER.error(
                "HTTP Error fetching resources for %s: Status Code %s - %s",
                virtual_entity.name,
                ex.response.status_code,
                ex,
            )
            continue
        except (Timeout, ConnectionError) as ex:
            _LOGGER.error("Failed to get resources: %s", ex)
            continue
        except Exception as ex:
            _LOGGER.exception("Unexpected exception: %s. Please open an issue", ex)
            continue

        for resource in resources:
            _LOGGER.debug(
                "Processing resource with classifier: %s", resource.classifier
            )
            if resource.classifier in ["electricity.consumption", "gas.consumption"]:
                coordinator_key = f"{virtual_entity.id}_{resource.classifier}"
                if coordinator_key not in daily_coordinators:
                    daily_coordinators[coordinator_key] = DataCoordinator(
                        hass, resource, daily_interval
                    )
                    # Schedule delayed first refresh instead of immediate call
                    hass.async_create_task(
                        _delayed_first_refresh(daily_coordinators[coordinator_key], 5)
                    )

                usage_sensor = Usage(
                    daily_coordinators[coordinator_key], resource, virtual_entity
                )
                entities.append(usage_sensor)
                meters[resource.classifier] = usage_sensor
                _LOGGER.debug(
                    "Added Usage sensor to list for entity %s", resource.classifier
                )

                if coordinator_key not in tariff_coordinators:
                    tariff_coordinators[coordinator_key] = TariffCoordinator(
                        hass, resource, tariff_interval
                    )
                    # Schedule delayed first refresh instead of immediate call
                    hass.async_create_task(
                        _delayed_first_refresh(tariff_coordinators[coordinator_key], 5)
                    )

                standing_sensor = Standing(
                    tariff_coordinators[coordinator_key], resource, virtual_entity
                )
                entities.append(standing_sensor)
                _LOGGER.debug(
                    "Added Standing sensor to list for entity %s", resource.classifier
                )

                rate_sensor = Rate(
                    tariff_coordinators[coordinator_key], resource, virtual_entity
                )
                entities.append(rate_sensor)
                _LOGGER.debug(
                    "Added Rate sensor to list for entity %s", resource.classifier
                )

            elif resource.classifier == "electricity.export":
                coordinator_key = f"{virtual_entity.id}_{resource.classifier}"
                if coordinator_key not in daily_coordinators:
                    daily_coordinators[coordinator_key] = ExportDataCoordinator(
                        hass, resource, daily_interval
                    )
                    hass.async_create_task(
                        _delayed_first_refresh(daily_coordinators[coordinator_key], 5)
                    )

                export_sensor = Export(
                    daily_coordinators[coordinator_key], resource, virtual_entity
                )
                entities.append(export_sensor)
                _LOGGER.debug(
                    "Added Export sensor to list for entity %s", resource.classifier
                )

        for resource in resources:
            if resource.classifier == "gas.consumption.cost":
                coordinator_key = f"{virtual_entity.id}_{resource.classifier}"
                if coordinator_key not in daily_coordinators:
                    daily_coordinators[coordinator_key] = DataCoordinator(
                        hass, resource, daily_interval
                    )
                    # Schedule delayed first refresh instead of immediate call
                    hass.async_create_task(
                        _delayed_first_refresh(daily_coordinators[coordinator_key], 5)
                    )

                cost_sensor = Cost(
                    daily_coordinators[coordinator_key], resource, virtual_entity
                )
                cost_sensor.meter = meters["gas.consumption"]
                entities.append(cost_sensor)
                _LOGGER.debug("Added Gas Cost sensor to list.")
            elif resource.classifier == "electricity.consumption.cost":
                coordinator_key = f"{virtual_entity.id}_{resource.classifier}"
                if coordinator_key not in daily_coordinators:
                    daily_coordinators[coordinator_key] = DataCoordinator(
                        hass, resource, daily_interval
                    )
                    # Schedule delayed first refresh instead of immediate call
                    hass.async_create_task(
                        _delayed_first_refresh(daily_coordinators[coordinator_key], 5)
                    )

                cost_sensor = Cost(
                    daily_coordinators[coordinator_key], resource, virtual_entity
                )
                cost_sensor.meter = meters["electricity.consumption"]
                entities.append(cost_sensor)
                _LOGGER.debug("Added Electricity Cost sensor to list.")

    _LOGGER.debug("Calling async_add_entities with %s entities", len(entities))
    async_add_entities(entities)
    _LOGGER.debug("async_add_entities call completed.")

    return True

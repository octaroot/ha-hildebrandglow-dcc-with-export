"""Platform for sensor integration."""

from __future__ import annotations

from abc import ABC, abstractmethod
import asyncio
from collections.abc import Callable
from datetime import datetime, timedelta
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


# --- EXTERNAL STATISTICS HELPER ---


async def _import_external_statistics(
    hass: HomeAssistant,
    resource,
    statistic_id: str,
    statistic_name: str,
    imported_hours: set[int],
) -> None:
    """Fetch half-hourly data and import as external statistics.

    Uses async_add_external_statistics with a non-recorder source
    to avoid conflicts with HA's auto-compiled recorder statistics.
    Tracks imported hours in-memory to avoid duplicate inserts.
    """
    try:
        from homeassistant.components.recorder.models import (
            StatisticData,
            StatisticMeanType,
            StatisticMetaData,
        )
        from homeassistant.components.recorder.statistics import (
            async_add_external_statistics,
            get_instance,
            statistics_during_period,
        )
    except ImportError:
        _LOGGER.warning("Recorder statistics API not available, skipping import")
        return

    try:
        now = dt_util.utcnow()
        utc_offset = -int(dt_util.now().utcoffset().total_seconds() / 60)

        t_from = (now - timedelta(days=2)).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + timedelta(minutes=utc_offset)
        t_to = now.replace(second=0, microsecond=0)

        readings = await hass.async_add_executor_job(
            resource.get_readings, t_from, t_to, "PT30M", "sum", utc_offset
        )

        if not readings:
            _LOGGER.debug("No half-hourly readings returned for %s", statistic_id)
            return

        # Aggregate into hourly buckets using integer epoch timestamps
        hourly: dict[int, float] = {}
        for ts, val in readings:
            if isinstance(ts, (int, float)):
                hour_ts = int(ts) // 3600 * 3600
            else:
                hour_ts = int(
                    ts.replace(minute=0, second=0, microsecond=0).timestamp()
                )
            hourly[hour_ts] = hourly.get(hour_ts, 0.0) + val.value

        if not hourly:
            return

        # Skip already-imported hours and the current incomplete hour
        current_hour_ts = int(
            now.replace(minute=0, second=0, microsecond=0).timestamp()
        )
        new_hours = {
            ts: kwh
            for ts, kwh in hourly.items()
            if ts not in imported_hours and ts != current_hour_ts
        }

        if not new_hours:
            _LOGGER.debug("No new completed hours to import for %s", statistic_id)
            return

        # Get last known cumulative sum
        sorted_new = sorted(new_hours.items())
        last_sum = 0.0
        try:
            last_stats = await get_instance(hass).async_add_executor_job(
                statistics_during_period,
                hass,
                datetime.utcfromtimestamp(
                    sorted_new[0][0] - 7 * 86400
                ).replace(tzinfo=dt_util.UTC),
                datetime.utcfromtimestamp(sorted_new[0][0]).replace(
                    tzinfo=dt_util.UTC
                ),
                {statistic_id},
                "hour",
                None,
                {"sum"},
            )
            if statistic_id in last_stats and len(last_stats[statistic_id]) > 0:
                last_sum = last_stats[statistic_id][-1]["sum"]
        except Exception as ex:
            _LOGGER.debug(
                "Could not fetch last statistics for %s: %s", statistic_id, ex
            )

        # Build statistics entries
        stats = []
        cumulative = last_sum
        for hour_ts, kwh in sorted_new:
            cumulative += kwh
            dt = datetime.utcfromtimestamp(hour_ts).replace(tzinfo=dt_util.UTC)
            stats.append(
                StatisticData(
                    start=dt,
                    state=round(kwh, 6),
                    sum=round(cumulative, 6),
                )
            )

        metadata = StatisticMetaData(
            has_mean=False,
            has_sum=True,
            mean_type=StatisticMeanType.NONE,
            name=statistic_name,
            source=DOMAIN,
            statistic_id=statistic_id,
            unit_of_measurement="kWh",
            unit_class="energy",
        )

        _LOGGER.debug(
            "Importing %d new external statistics for %s",
            len(stats),
            statistic_id,
        )
        async_add_external_statistics(hass, metadata, stats)

        # Mark as imported
        imported_hours.update(new_hours.keys())

    except Exception as ex:
        _LOGGER.warning(
            "Failed to import external statistics for %s: %s", statistic_id, ex
        )


# --- COORDINATOR CLASSES ---


class DataCoordinator(DataUpdateCoordinator):
    """Data update coordinator for daily usage and cost sensors.

    Also imports half-hourly data as external statistics for consumption
    resources (electricity/gas) for proper Energy Dashboard resolution.
    """

    def __init__(self, hass: HomeAssistant, glowmarkt_resource, daily_interval):
        """Initialize daily data coordinator."""
        self.resource = glowmarkt_resource
        self._imported_hours: set[int] = set()
        self._external_stat_id: str | None = None
        self._external_stat_name: str | None = None

        # Set up external statistics for consumption resources
        classifier = glowmarkt_resource.classifier
        if classifier == "electricity.consumption":
            self._external_stat_id = f"{DOMAIN}:electricity_consumption"
            self._external_stat_name = "Electricity Consumption"
        elif classifier == "gas.consumption":
            self._external_stat_id = f"{DOMAIN}:gas_consumption"
            self._external_stat_name = "Gas Consumption"

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

        # Import external statistics for consumption resources
        if self._external_stat_id:
            await _import_external_statistics(
                self.hass,
                self.resource,
                self._external_stat_id,
                self._external_stat_name,
                self._imported_hours,
            )

        try:
            value = await daily_data(self.hass, self.resource)
            if value is None:
                return None
            return value
        except HTTPError as ex:
            raise UpdateFailed(
                f"HTTP Error fetching daily data: {ex}, "
                f"Status Code: {ex.response.status_code}"
            ) from ex
        except Timeout as ex:
            raise UpdateFailed(f"Timeout fetching daily data: {ex}") from ex
        except ConnectionError as ex:
            raise UpdateFailed(
                f"Connection error fetching daily data: {ex}"
            ) from ex
        except Exception as ex:
            _LOGGER.exception("Unexpected exception fetching daily data: %s", ex)
            raise UpdateFailed(
                f"Unknown error fetching daily data: {ex}"
            ) from ex


class ExportDataCoordinator(DataUpdateCoordinator):
    """Data update coordinator for export sensors.

    Fetches today's/yesterday's value for sensor display, and imports
    half-hourly data as external statistics for Energy Dashboard use.
    """

    def __init__(self, hass: HomeAssistant, glowmarkt_resource, daily_interval):
        """Initialize export data coordinator."""
        self.resource = glowmarkt_resource
        self._imported_hours: set[int] = set()
        super().__init__(
            hass,
            _LOGGER,
            name=f"Export Data {glowmarkt_resource.classifier}",
            update_interval=timedelta(minutes=daily_interval),
        )

    async def _async_update_data(self):
        """Fetch export data and import external statistics."""
        _LOGGER.debug(
            "ExportDataCoordinator updating for resource %s",
            self.resource.classifier,
        )

        # Import external statistics
        await _import_external_statistics(
            self.hass,
            self.resource,
            f"{DOMAIN}:electricity_export",
            "Electricity Export",
            self._imported_hours,
        )

        # Return today's/yesterday's value for the sensor display
        return await _fetch_export_sensor_value(self.hass, self.resource)


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
                "Error fetching tariff data for %s: %s",
                self.resource.classifier,
                ex,
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
    _LOGGER.error(
        "Unknown classifier: %s. Please open an issue", resource.classifier
    )
    return "unknown"


def device_name(resource, virtual_entity) -> str:
    """Return device name. Includes name of virtual entity if it exists."""
    supply = supply_type(resource)
    if virtual_entity.name is not None:
        return f"{virtual_entity.name} smart {supply} meter"
    return f"Smart {supply} meter"


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
        _LOGGER.error(
            "HTTP Error: %s, Status Code: %s", ex, ex.response.status_code
        )
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
        _LOGGER.debug(
            "Successfully got daily usage for resource id %s", resource.id
        )
        _LOGGER.debug(
            "Readings for %s has %s entries",
            resource.classifier,
            len(readings),
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
        _LOGGER.exception(
            "Unexpected exception: %s. Please open an issue", ex
        )
        return None


async def _fetch_export_sensor_value(
    hass: HomeAssistant, resource
) -> float | None:
    """Get export value for sensor display: today if available, else yesterday."""
    now = dt_util.utcnow()
    utc_offset = -int(dt_util.now().utcoffset().total_seconds() / 60)

    today_midnight = now.replace(
        hour=0, minute=0, second=0, microsecond=0
    ) + timedelta(minutes=utc_offset)
    t_to = now.replace(second=0, microsecond=0)

    today_val = await _get_reading(
        hass, resource, today_midnight, t_to, utc_offset
    )
    if today_val is not None and today_val > 0:
        return today_val

    yesterday_midnight = today_midnight - timedelta(days=1)
    yesterday_val = await _get_reading(
        hass, resource, yesterday_midnight, today_midnight, utc_offset
    )
    if yesterday_val is not None and yesterday_val > 0:
        _LOGGER.debug(
            "Using yesterday's export value as fallback: %s", yesterday_val
        )
        return yesterday_val

    return today_val


async def _get_reading(
    hass: HomeAssistant, resource, t_from, t_to, utc_offset
) -> float | None:
    """Fetch a single daily reading for a time range."""
    try:
        readings = await hass.async_add_executor_job(
            resource.get_readings, t_from, t_to, "P1D", "sum", utc_offset
        )
        if not readings:
            return None
        v = readings[0][1].value
        if len(readings) > 1:
            v += readings[1][1].value
        return v
    except Exception as ex:
        _LOGGER.debug("Error fetching reading: %s", ex)
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
            "No tariff data found for %s meter (id: %s). If you don't see "
            "tariff data for this meter in the Bright app, please disable "
            "the associated rate and standing charge sensors",
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
            "Connection error fetching tariff data for %s: %s",
            resource.classifier,
            ex,
        )
        return None
    except Exception as ex:
        _LOGGER.exception(
            "Unexpected exception fetching tariff data for %s: %s. "
            "Please open an issue",
            resource.classifier,
            ex,
        )
        return None


async def _delayed_first_refresh(
    coordinator: DataUpdateCoordinator, delay: int = 5
):
    """Perform first refresh after a delay."""
    _LOGGER.debug(
        "Scheduling delayed first refresh for %s in %d seconds",
        coordinator.name,
        delay,
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
        _LOGGER.debug(
            "Created Usage sensor with unique_id: %s", self._attr_unique_id
        )

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
        _LOGGER.debug(
            "Created Cost sensor with unique_id: %s", self._attr_unique_id
        )

    @callback
    def _update_native_value(self, data: float) -> None:
        """Set the native value for cost sensor from coordinator data."""
        self._attr_native_value = round(data / 100, 2)


class Export(GlowDCCSensor):
    """Sensor for daily electricity export.

    Shows today's export when the API has data, otherwise falls back to
    yesterday's settled value (DCC export data can be delayed ~24h).
    Half-hourly data is imported as external statistics by the coordinator.
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

    @callback
    def _update_native_value(self, data: float) -> None:
        """Set the native value for export sensor from coordinator data."""
        self._attr_native_value = round(data, 2)

    @property
    def device_info(self) -> DeviceInfo:
        """Return device information."""
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
                float(
                    self.coordinator.data.current_rates.standing_charge.value
                )
                / 100
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
        _LOGGER.debug(
            "Created Rate sensor with unique_id: %s", self._attr_unique_id
        )
        self.resource = resource
        self.virtual_entity = virtual_entity

    @callback
    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        if self.coordinator.data:
            value = (
                float(self.coordinator.data.current_rates.rate.value) / 100
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


# --- SETUP HELPERS ---


def _setup_consumption_sensors(
    hass,
    resource,
    virtual_entity,
    daily_interval,
    tariff_interval,
    daily_coordinators,
    tariff_coordinators,
    entities,
    meters,
):
    """Set up Usage, Standing, and Rate sensors for a consumption resource."""
    coordinator_key = f"{virtual_entity.id}_{resource.classifier}"
    if coordinator_key not in daily_coordinators:
        daily_coordinators[coordinator_key] = DataCoordinator(
            hass, resource, daily_interval
        )
        hass.async_create_task(
            _delayed_first_refresh(daily_coordinators[coordinator_key], 5)
        )

    usage_sensor = Usage(
        daily_coordinators[coordinator_key], resource, virtual_entity
    )
    entities.append(usage_sensor)
    meters[resource.classifier] = usage_sensor
    _LOGGER.debug("Added Usage sensor for %s", resource.classifier)

    if coordinator_key not in tariff_coordinators:
        tariff_coordinators[coordinator_key] = TariffCoordinator(
            hass, resource, tariff_interval
        )
        hass.async_create_task(
            _delayed_first_refresh(tariff_coordinators[coordinator_key], 5)
        )

    entities.append(
        Standing(
            tariff_coordinators[coordinator_key], resource, virtual_entity
        )
    )
    entities.append(
        Rate(tariff_coordinators[coordinator_key], resource, virtual_entity)
    )
    _LOGGER.debug(
        "Added Standing and Rate sensors for %s", resource.classifier
    )


def _setup_export_sensor(
    hass,
    resource,
    virtual_entity,
    daily_interval,
    daily_coordinators,
    entities,
):
    """Set up Export sensor for an electricity export resource."""
    coordinator_key = f"{virtual_entity.id}_{resource.classifier}"
    if coordinator_key not in daily_coordinators:
        daily_coordinators[coordinator_key] = ExportDataCoordinator(
            hass, resource, daily_interval
        )
        hass.async_create_task(
            _delayed_first_refresh(daily_coordinators[coordinator_key], 5)
        )

    entities.append(
        Export(
            daily_coordinators[coordinator_key], resource, virtual_entity
        )
    )
    _LOGGER.debug("Added Export sensor for %s", resource.classifier)


def _setup_cost_sensor(
    hass,
    resource,
    virtual_entity,
    daily_interval,
    daily_coordinators,
    entities,
    meters,
):
    """Set up Cost sensor for a cost resource."""
    coordinator_key = f"{virtual_entity.id}_{resource.classifier}"
    if coordinator_key not in daily_coordinators:
        daily_coordinators[coordinator_key] = DataCoordinator(
            hass, resource, daily_interval
        )
        hass.async_create_task(
            _delayed_first_refresh(daily_coordinators[coordinator_key], 5)
        )

    cost_sensor = Cost(
        daily_coordinators[coordinator_key], resource, virtual_entity
    )
    if resource.classifier == "gas.consumption.cost":
        cost_sensor.meter = meters.get("gas.consumption")
    elif resource.classifier == "electricity.consumption.cost":
        cost_sensor.meter = meters.get("electricity.consumption")
    entities.append(cost_sensor)
    _LOGGER.debug("Added Cost sensor for %s", resource.classifier)


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
    daily_interval = hass.data[DOMAIN][entry.entry_id].get(
        CONF_DAILY_INTERVAL, 15
    )
    tariff_interval = hass.data[DOMAIN][entry.entry_id].get(
        CONF_TARIFF_INTERVAL, 60
    )

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
        _LOGGER.exception(
            "Unexpected exception: %s. Please open an issue", ex
        )
        return False

    for virtual_entity in virtual_entities:
        _LOGGER.debug("Found virtual entity: %s", virtual_entity.name)
        resources: dict = {}
        try:
            _LOGGER.debug(
                "Fetching resources for virtual entity %s...",
                virtual_entity.name,
            )
            resources = await hass.async_add_executor_job(
                virtual_entity.get_resources
            )
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
            _LOGGER.exception(
                "Unexpected exception: %s. Please open an issue", ex
            )
            continue

        for resource in resources:
            _LOGGER.debug(
                "Processing resource with classifier: %s",
                resource.classifier,
            )
            if resource.classifier in [
                "electricity.consumption",
                "gas.consumption",
            ]:
                _setup_consumption_sensors(
                    hass,
                    resource,
                    virtual_entity,
                    daily_interval,
                    tariff_interval,
                    daily_coordinators,
                    tariff_coordinators,
                    entities,
                    meters,
                )
            elif resource.classifier == "electricity.export":
                _setup_export_sensor(
                    hass,
                    resource,
                    virtual_entity,
                    daily_interval,
                    daily_coordinators,
                    entities,
                )

        for resource in resources:
            if resource.classifier in [
                "gas.consumption.cost",
                "electricity.consumption.cost",
            ]:
                _setup_cost_sensor(
                    hass,
                    resource,
                    virtual_entity,
                    daily_interval,
                    daily_coordinators,
                    entities,
                    meters,
                )

    _LOGGER.debug(
        "Calling async_add_entities with %s entities", len(entities)
    )
    async_add_entities(entities)
    _LOGGER.debug("async_add_entities call completed.")

    return True

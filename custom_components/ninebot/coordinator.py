from __future__ import annotations

from datetime import UTC, datetime, timedelta
import logging
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import NinebotApiAuthError, NinebotApiConnectionError, NinebotCliClient
from .const import CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL, DOMAIN

LOGGER = logging.getLogger(__package__)

_VEHICLE_REFRESH_INTERVAL = timedelta(hours=1)
_DETAIL_REFRESH_INTERVAL = timedelta(minutes=10)


class NinebotDataUpdateCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Coordinate Ninebot API polling."""

    config_entry: ConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        client: NinebotCliClient,
    ) -> None:
        self.config_entry = entry
        self._client = client
        self._last_vehicle_sync_at: datetime | None = None
        self._last_detail_sync_at: dict[str, datetime] = {}
        self._known_vehicles: list[dict[str, Any]] = []
        update_interval = timedelta(
            seconds=entry.options.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)
        )
        super().__init__(
            hass,
            logger=LOGGER,
            name=DOMAIN,
            update_interval=update_interval,
        )

    async def async_first_sync(self) -> None:
        """Sync the vehicle list on integration load or reload."""
        try:
            self._known_vehicles = await self._client.async_get_device_list()
        except NinebotApiAuthError as err:
            raise ConfigEntryAuthFailed from err
        except NinebotApiConnectionError as err:
            LOGGER.debug("Ninebot vehicle discovery failed: %s", err)
            self._known_vehicles = []
        self._last_vehicle_sync_at = datetime.now(UTC)

    async def _async_update_data(self) -> dict[str, Any]:
        previous = self.data or {"devices": {}}
        previous_devices = previous.get("devices", {})

        await self._maybe_refresh_vehicles()
        statuses = await self._fetch_status_for_known_devices()

        merged: dict[str, dict[str, Any]] = {}
        for vehicle in self._known_vehicles:
            sn = vehicle.get("sn")
            if not isinstance(sn, str) or not sn:
                continue
            existing = previous_devices.get(sn, {"sn": sn, "info": vehicle, "state": {}})
            existing_state = existing.get("state", {})
            status_state = statuses.get(sn, {})
            # This merge is the single point where prior state (including detail fields)
            # survives into the next cycle. _apply_details only overwrites new values on top.
            base_state = {**existing_state, **status_state}
            merged[sn] = {
                **existing,
                "info": vehicle,
                "state": base_state,
            }

        # Carry over any prior devices no longer reported.
        for sn, payload in previous_devices.items():
            if sn not in merged:
                merged[sn] = payload

        await self._apply_details(merged, previous_devices)

        return {"devices": merged}

    async def _maybe_refresh_vehicles(self) -> None:
        last = self._last_vehicle_sync_at
        if last is not None and (datetime.now(UTC) - last) < _VEHICLE_REFRESH_INTERVAL:
            return
        try:
            self._known_vehicles = await self._client.async_get_device_list()
            self._last_vehicle_sync_at = datetime.now(UTC)
        except NinebotApiAuthError as err:
            raise ConfigEntryAuthFailed from err
        except NinebotApiConnectionError as err:
            LOGGER.debug("Ninebot vehicle discovery failed: %s", err)
            if last is None:
                self._known_vehicles = []

    async def _fetch_status_for_known_devices(self) -> dict[str, dict[str, Any]]:
        statuses: dict[str, dict[str, Any]] = {}
        for vehicle in self._known_vehicles:
            sn = vehicle.get("sn")
            if not isinstance(sn, str) or not sn:
                continue
            try:
                statuses[sn] = await self._client.async_get_device_status(sn)
            except NinebotApiAuthError as err:
                raise ConfigEntryAuthFailed from err
            except NinebotApiConnectionError as err:
                raise UpdateFailed(str(err) or "Failed to fetch Ninebot status") from err
        return statuses

    async def _apply_details(
        self,
        merged: dict[str, dict[str, Any]],
        previous_devices: dict[str, dict[str, Any]],
    ) -> None:
        now = datetime.now(UTC)
        for sn, payload in merged.items():
            last = self._last_detail_sync_at.get(sn)
            if last is not None and (now - last) < _DETAIL_REFRESH_INTERVAL:
                continue

            # new_state already inherits prior state via the base_state merge in
            # _async_update_data, so prior travel/battery fields survive without an
            # explicit setdefault. We only need to layer new detail values on top.
            new_state: dict[str, Any] = dict(payload.get("state", {}))

            try:
                travel = await self._client.async_get_device_travel(sn)
            except NinebotApiConnectionError as err:
                LOGGER.debug("Ninebot travel fetch failed for %s: %s", sn, err)
                travel = {}
            except NinebotApiAuthError as err:
                raise ConfigEntryAuthFailed from err

            travel_fields = {k: v for k, v in travel.items() if k.startswith(("month_", "last_"))}
            if travel_fields:
                new_state.update(travel_fields)

            try:
                battery = await self._client.async_get_battery(sn)
            except NinebotApiConnectionError as err:
                LOGGER.debug("Ninebot battery fetch failed for %s: %s", sn, err)
                battery = {}
            except NinebotApiAuthError as err:
                raise ConfigEntryAuthFailed from err

            battery_fields = self._client._normalize_battery(battery)
            for key in ("bms_voltage", "batt_temp", "bms_cycles", "bms_score", "charging_power"):
                if key in battery_fields:
                    new_state[key] = battery_fields[key]

            payload["state"] = new_state
            self._last_detail_sync_at[sn] = now

    async def async_request_device_status_refresh(self, sn: str) -> None:
        try:
            status = await self._client.async_get_device_status(sn)
        except NinebotApiAuthError as err:
            raise ConfigEntryAuthFailed from err
        except NinebotApiConnectionError as err:
            raise UpdateFailed(str(err) or "Failed to fetch Ninebot status") from err

        # Serialize against _async_update_data to avoid torn state on coordinator.data.
        async with self._refresh_mutex:
            data = self.data or {"devices": {}}
            devices = data.get("devices", {})
            device = devices.get(sn)
            if device is None:
                return

            current_state = device.get("state", {})
            preserved_keys = (
                "bms_voltage",
                "batt_temp",
                "bms_cycles",
                "bms_score",
                "charging_power",
            )
            preserved = {key: current_state[key] for key in preserved_keys if key in current_state}
            travel_state = {
                key: value
                for key, value in current_state.items()
                if key.startswith(("month_", "last_"))
            }
            updated_state = {**travel_state, **preserved, **status}
            current_visible_state = {
                key: key_value for key, key_value in current_state.items() if key != "raw"
            }
            updated_visible_state = {
                key: value for key, value in updated_state.items() if key != "raw"
            }
            if updated_visible_state == current_visible_state:
                return

            updated_device = {
                **device,
                "state": updated_state,
            }
            self.async_set_updated_data({
                **data,
                "devices": {
                    **devices,
                    sn: updated_device,
                },
            })

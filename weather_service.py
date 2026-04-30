"""
NEA weather access via data.gov.sg real-time APIs.

The v2 API is preferred. Older v1 endpoints are kept as a fallback because
they still expose the same NEA data if the newer path changes availability.
"""

from __future__ import annotations

import logging
from datetime import datetime
from difflib import SequenceMatcher

import requests

logger = logging.getLogger(__name__)

TWO_HOUR_V2 = "https://api-open.data.gov.sg/v2/real-time/api/two-hr-forecast"
TWENTY_FOUR_HOUR_V2 = "https://api-open.data.gov.sg/v2/real-time/api/twenty-four-hr-forecast"
FOUR_DAY_V2 = "https://api-open.data.gov.sg/v2/real-time/api/four-day-outlook"

TWO_HOUR_V1 = "https://api.data.gov.sg/v1/environment/2-hour-weather-forecast"
TWENTY_FOUR_HOUR_V1 = "https://api.data.gov.sg/v1/environment/24-hour-weather-forecast"
AIR_TEMPERATURE_V1 = "https://api.data.gov.sg/v1/environment/air-temperature"
RELATIVE_HUMIDITY_V1 = "https://api.data.gov.sg/v1/environment/relative-humidity"
PSI_V1 = "https://api.data.gov.sg/v1/environment/psi"
PM25_V1 = "https://api.data.gov.sg/v1/environment/pm25"

DEFAULT_AREA = "Yishun"

REGION_AREAS = {
    "north": {
        "admiralty", "ang mo kio", "canberra", "choa chu kang", "khatib",
        "kranji", "lim chu kang", "marsiling", "seletar", "sembawang",
        "sengkang", "serangoon", "woodlands", "yio chu kang", "yishun",
    },
    "south": {
        "bukit merah", "harbourfront", "keppel", "marina bay", "orchard",
        "outram", "queenstown", "sentosa", "southern islands", "tanjong pagar",
        "telok blangah",
    },
    "east": {
        "bedok", "changi", "eunos", "geylang", "hougang", "joo chiat",
        "katong", "pasir ris", "paya lebar", "pulau ubin", "simei",
        "tampines", "toa payoh",
    },
    "west": {
        "boon lay", "bukit batok", "bukit panjang", "bukit timah", "clementi",
        "jurong", "jurong east", "jurong west", "pioneer", "tuas",
        "western islands", "western water catchment",
    },
    "central": {
        "bishan", "bukit timah", "city", "downtown core", "kallang",
        "macritchie", "mandai", "marina centre", "newton", "novena",
        "potong pasir", "river valley", "rochor", "toa payoh",
    },
}


def _get_json(url: str) -> dict:
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _v2_data(url: str) -> dict:
    payload = _get_json(url)
    if payload.get("code") not in (None, 0):
        raise ValueError(payload.get("errorMsg") or payload.get("message") or "data.gov.sg returned an error")
    return payload.get("data") or {}


def _parse_dt(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _format_dt(value: str) -> str:
    dt = _parse_dt(value)
    if not dt:
        return value or ""
    return dt.strftime("%a %-d %b, %H:%M")


def _format_period(period: dict) -> str:
    time_period = period.get("timePeriod") or period.get("time") or {}
    label = time_period.get("text") or f"{_format_dt(time_period.get('start', ''))} to {_format_dt(time_period.get('end', ''))}"
    regions = period.get("regions") or {}
    region_text = []
    for region in ("north", "south", "east", "west", "central"):
        value = regions.get(region)
        if isinstance(value, dict):
            value = value.get("text")
        if value:
            region_text.append(f"{region.title()}: {value}")
    return f"- {label}: {'; '.join(region_text)}"


def _period_is_within_valid(period: dict, valid_start: str = "", valid_end: str = "") -> bool:
    if not valid_start or not valid_end:
        return True
    time_period = period.get("timePeriod") or period.get("time") or {}
    start = _parse_dt(time_period.get("start", ""))
    valid_start_dt = _parse_dt(valid_start)
    valid_end_dt = _parse_dt(valid_end)
    if not start or not valid_start_dt or not valid_end_dt:
        return True
    return valid_start_dt <= start <= valid_end_dt


def _forecast_text(value) -> str:
    if isinstance(value, dict):
        return value.get("text") or value.get("summary") or value.get("code") or ""
    return value or ""


def _match_area(area: str, forecasts: list[dict]) -> dict | None:
    if not forecasts:
        return None
    wanted = " ".join((area or DEFAULT_AREA).lower().split())
    exact = [item for item in forecasts if item.get("area", "").lower() == wanted]
    if exact:
        return exact[0]
    contains = [item for item in forecasts if wanted in item.get("area", "").lower()]
    if contains:
        return contains[0]
    return max(
        forecasts,
        key=lambda item: SequenceMatcher(None, wanted, item.get("area", "").lower()).ratio(),
    )


def _region_for_area(area: str) -> str:
    wanted = " ".join((area or DEFAULT_AREA).lower().split())
    for region, areas in REGION_AREAS.items():
        if wanted in areas:
            return region
    best_region = "national"
    best_score = 0.0
    for region, areas in REGION_AREAS.items():
        for candidate in areas:
            score = SequenceMatcher(None, wanted, candidate).ratio()
            if score > best_score:
                best_region = region
                best_score = score
    return best_region if best_score >= 0.62 else "national"


def _latest_station_reading(url: str, area: str) -> dict:
    payload = _get_json(url)
    stations = (payload.get("metadata") or {}).get("stations") or []
    readings = ((payload.get("items") or [{}])[0]).get("readings") or []
    station_by_id = {station.get("id"): station for station in stations}
    wanted = " ".join((area or DEFAULT_AREA).lower().split())
    reading_by_station = {reading.get("station_id"): reading.get("value") for reading in readings}
    available = [
        {
            "station": station,
            "value": reading_by_station.get(station.get("id")),
        }
        for station in stations
        if station.get("id") in reading_by_station
    ]
    if not available:
        return {}

    exact = [
        item for item in available
        if (item["station"].get("name") or "").lower() == wanted
    ]
    if exact:
        selected = exact[0]
    else:
        selected = max(
            available,
            key=lambda item: SequenceMatcher(None, wanted, (item["station"].get("name") or "").lower()).ratio(),
        )
    station = selected.get("station") or station_by_id.get(selected.get("station_id")) or {}
    return {"station": station.get("name", ""), "value": selected.get("value")}


def _air_quality(area: str) -> dict:
    region = _region_for_area(area)
    result = {"region": region}
    try:
        item = (_get_json(PSI_V1).get("items") or [{}])[0]
        readings = item.get("readings") or {}
        result["psi_24h"] = (readings.get("psi_twenty_four_hourly") or {}).get(region)
        result["pm25_24h"] = (readings.get("pm25_twenty_four_hourly") or {}).get(region)
    except Exception as e:
        logger.warning(f"NEA PSI fetch failed: {e}")
    try:
        item = (_get_json(PM25_V1).get("items") or [{}])[0]
        readings = item.get("readings") or {}
        result["pm25_1h"] = (readings.get("pm25_one_hourly") or {}).get(region)
    except Exception as e:
        logger.warning(f"NEA PM2.5 fetch failed: {e}")
    return result


def _current_conditions(area: str) -> dict:
    conditions = {}
    try:
        conditions["temperature"] = _latest_station_reading(AIR_TEMPERATURE_V1, area)
    except Exception as e:
        logger.warning(f"NEA air temperature fetch failed: {e}")
    try:
        conditions["humidity"] = _latest_station_reading(RELATIVE_HUMIDITY_V1, area)
    except Exception as e:
        logger.warning(f"NEA relative humidity fetch failed: {e}")
    conditions["air_quality"] = _air_quality(area)
    return conditions


def _two_hour_forecast(area: str = "") -> dict:
    try:
        data = _v2_data(TWO_HOUR_V2)
        items = data.get("items") or data.get("records") or []
        record = items[0] if items else {}
        forecasts = record.get("forecasts") or []
        valid = record.get("valid_period") or record.get("validPeriod") or {}
        return {
            "source": "v2",
            "updated": record.get("update_timestamp") or record.get("updatedTimestamp") or data.get("updatedTimestamp", ""),
            "valid_start": valid.get("start", ""),
            "valid_end": valid.get("end", ""),
            "valid_text": valid.get("text", ""),
            "forecasts": [
                {"area": item.get("area", ""), "forecast": _forecast_text(item.get("forecast"))}
                for item in forecasts
            ],
        }
    except Exception as e:
        logger.warning(f"NEA 2-hour v2 weather failed: {e}")

    payload = _get_json(TWO_HOUR_V1)
    item = (payload.get("items") or [{}])[0]
    valid = item.get("valid_period") or {}
    return {
        "source": "v1",
        "updated": item.get("update_timestamp", ""),
        "valid_start": valid.get("start", ""),
        "valid_end": valid.get("end", ""),
        "valid_text": "",
        "forecasts": item.get("forecasts") or [],
    }


def _twenty_four_hour_forecast() -> dict:
    try:
        data = _v2_data(TWENTY_FOUR_HOUR_V2)
        record = (data.get("records") or [{}])[0]
        general = record.get("general") or {}
        valid = general.get("validPeriod") or {}
        return {
            "updated": record.get("updatedTimestamp", ""),
            "general": general,
            "valid_start": valid.get("start", ""),
            "valid_end": valid.get("end", ""),
            "periods": record.get("periods") or [],
        }
    except Exception as e:
        logger.warning(f"NEA 24-hour v2 weather failed: {e}")

    item = (_get_json(TWENTY_FOUR_HOUR_V1).get("items") or [{}])[0]
    return {
        "updated": item.get("update_timestamp", ""),
        "general": item.get("general") or {},
        "valid_start": (item.get("valid_period") or {}).get("start", ""),
        "valid_end": (item.get("valid_period") or {}).get("end", ""),
        "periods": item.get("periods") or [],
    }


def _four_day_forecast() -> dict:
    data = _v2_data(FOUR_DAY_V2)
    record = (data.get("records") or [{}])[0]
    return {
        "updated": record.get("updatedTimestamp", ""),
        "forecasts": record.get("forecasts") or [],
    }


def build_weather_brief(area: str = "", include_24h: bool = True, include_4day: bool = False) -> str:
    area = " ".join((area or DEFAULT_AREA).split()) or DEFAULT_AREA
    two_hour = _two_hour_forecast(area)
    forecasts = two_hour.get("forecasts") or []
    matched = _match_area(area, forecasts)

    lines = [f"*NEA weather: {matched.get('area', area) if matched else area}*"]
    if two_hour.get("updated"):
        lines.append(f"Updated: {_format_dt(two_hour['updated'])} SGT")
    if two_hour.get("valid_text"):
        lines.append(f"2-hour forecast valid: {two_hour['valid_text']}")
    elif two_hour.get("valid_start") or two_hour.get("valid_end"):
        lines.append(f"2-hour forecast valid: {_format_dt(two_hour.get('valid_start', ''))} to {_format_dt(two_hour.get('valid_end', ''))} SGT")
    if matched:
        lines.append(f"Nowcast: {matched.get('forecast', '')}")
    elif forecasts:
        lines.append("Nowcast: area not found; closest NEA areas available are " + ", ".join(item["area"] for item in forecasts[:8]))
    else:
        lines.append("Nowcast: unavailable.")

    conditions = _current_conditions(matched.get("area", area) if matched else area)
    current_parts = []
    temperature = conditions.get("temperature") or {}
    humidity_now = conditions.get("humidity") or {}
    air_quality = conditions.get("air_quality") or {}
    if temperature.get("value") is not None:
        current_parts.append(f"Temp {temperature['value']} deg C at {temperature.get('station') or 'nearest station'}")
    if humidity_now.get("value") is not None:
        current_parts.append(f"Humidity {humidity_now['value']}% at {humidity_now.get('station') or 'nearest station'}")
    aq_parts = []
    if air_quality.get("psi_24h") is not None:
        aq_parts.append(f"24h PSI {air_quality['psi_24h']}")
    if air_quality.get("pm25_1h") is not None:
        aq_parts.append(f"1h PM2.5 {air_quality['pm25_1h']} ug/m3")
    if air_quality.get("pm25_24h") is not None:
        aq_parts.append(f"24h PM2.5 {air_quality['pm25_24h']} ug/m3")
    if aq_parts:
        current_parts.append(f"Air quality ({air_quality.get('region', 'national').title()}): " + ", ".join(aq_parts))
    if current_parts:
        lines.append("Current readings: " + "; ".join(current_parts))

    if include_24h:
        day = _twenty_four_hour_forecast()
        general = day.get("general") or {}
        forecast = _forecast_text(general.get("forecast"))
        temp = general.get("temperature") or {}
        humidity = general.get("relativeHumidity") or general.get("relative_humidity") or {}
        wind = general.get("wind") or {}
        wind_speed = wind.get("speed") or {}
        if day.get("updated"):
            lines.append(f"\n24-hour update: {_format_dt(day['updated'])} SGT")
        summary = [forecast] if forecast else []
        if temp:
            summary.append(f"{temp.get('low')} to {temp.get('high')} deg C")
        if humidity:
            summary.append(f"RH {humidity.get('low')} to {humidity.get('high')}%")
        if wind:
            summary.append(f"Wind {wind.get('direction', '')} {wind_speed.get('low')} to {wind_speed.get('high')} km/h".strip())
        if summary:
            lines.append("General: " + "; ".join(summary))
        periods = day.get("periods") or []
        if periods:
            lines.append("Periods:")
            clean_periods = [
                period for period in periods
                if _period_is_within_valid(period, day.get("valid_start", ""), day.get("valid_end", ""))
            ]
            lines.extend(_format_period(period) for period in clean_periods[:3])

    if include_4day:
        outlook = _four_day_forecast()
        forecasts = outlook.get("forecasts") or []
        if forecasts:
            lines.append("\n4-day outlook:")
            for item in forecasts[:4]:
                forecast = item.get("forecast") or {}
                temp = item.get("temperature") or {}
                lines.append(
                    f"- {item.get('day', '')} {_format_dt(item.get('timestamp', ''))}: "
                    f"{forecast.get('summary') or forecast.get('text', '')}; "
                    f"{temp.get('low')} to {temp.get('high')} deg C"
                )

    lines.append("\nSource: NEA/MSS via data.gov.sg real-time weather API.")
    return "\n".join(lines)

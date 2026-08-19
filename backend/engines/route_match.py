"""Distance from a point to railway polylines (route matching)."""
from __future__ import annotations
from math import radians, sin, cos, atan2, sqrt
from typing import Sequence


def _haversine_m(a_lat: float, a_lon: float, b_lat: float, b_lon: float) -> float:
    r = 6371000.0
    p1, p2 = radians(a_lat), radians(b_lat)
    dp = radians(b_lat - a_lat)
    dl = radians(b_lon - a_lon)
    x = sin(dp / 2) ** 2 + cos(p1) * cos(p2) * sin(dl / 2) ** 2
    return 2 * r * atan2(sqrt(x), sqrt(max(0.0, 1 - x)))


def _point_to_segment_m(
    lat: float, lon: float,
    a_lat: float, a_lon: float,
    b_lat: float, b_lon: float,
) -> float:
    """Approximate closest distance on a short segment using local ENU projection."""
    # meters per degree at mid-lat
    mid_lat = (a_lat + b_lat) / 2.0
    m_per_deg_lat = 111_320.0
    m_per_deg_lon = 111_320.0 * cos(radians(mid_lat))
    ax = (a_lon - lon) * m_per_deg_lon
    ay = (a_lat - lat) * m_per_deg_lat
    bx = (b_lon - lon) * m_per_deg_lon
    by = (b_lat - lat) * m_per_deg_lat
    # segment vector from A to B in local frame relative to point P at origin
    # Re-express: P at (0,0), A at (ax,ay) wait — actually place P as origin:
    # A = (a_lon, a_lat) relative to P
    ax = (a_lon - lon) * m_per_deg_lon
    ay = (a_lat - lat) * m_per_deg_lat
    bx = (b_lon - lon) * m_per_deg_lon
    by = (b_lat - lat) * m_per_deg_lat
    abx, aby = bx - ax, by - ay
    apx, apy = -ax, -ay  # from A to P
    ab2 = abx * abx + aby * aby
    if ab2 < 1e-6:
        return sqrt(ax * ax + ay * ay)
    t = max(0.0, min(1.0, (apx * abx + apy * aby) / ab2))
    cx, cy = ax + t * abx, ay + t * aby
    return sqrt(cx * cx + cy * cy)


def distance_to_polyline_m(
    lat: float,
    lon: float,
    coords_lonlat: Sequence[Sequence[float]],
) -> float:
    """coords as [[lon, lat], ...] GeoJSON order."""
    if len(coords_lonlat) < 2:
        if len(coords_lonlat) == 1:
            return _haversine_m(lat, lon, coords_lonlat[0][1], coords_lonlat[0][0])
        return float("inf")
    best = float("inf")
    for i in range(len(coords_lonlat) - 1):
        lon1, lat1 = coords_lonlat[i][0], coords_lonlat[i][1]
        lon2, lat2 = coords_lonlat[i + 1][0], coords_lonlat[i + 1][1]
        d = _point_to_segment_m(lat, lon, lat1, lon1, lat2, lon2)
        if d < best:
            best = d
    return best


def min_distance_to_segments_m(
    lat: float,
    lon: float,
    segments: Sequence[Sequence[Sequence[float]]],
) -> float | None:
    """segments: list of polylines each [[lon,lat],...]. None if no segments."""
    if not segments:
        return None
    best = float("inf")
    for poly in segments:
        d = distance_to_polyline_m(lat, lon, poly)
        if d < best:
            best = d
    return best if best != float("inf") else None


def route_match_score(distance_m: float | None, max_accept_m: float = 120.0) -> float:
    if distance_m is None:
        return 0.5  # unknown geometry — neutral, not optimistic
    if distance_m <= 25:
        return 1.0
    if distance_m >= max_accept_m:
        return 0.0
    return max(0.0, 1.0 - (distance_m - 25) / (max_accept_m - 25))

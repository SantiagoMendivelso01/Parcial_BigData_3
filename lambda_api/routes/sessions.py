from datetime import date as date_type
from flask import Blueprint, request, jsonify, abort
from db import query_to_list

sessions_bp = Blueprint("sessions", __name__)

VALID_DEVICES = {"mobile", "desktop", "tablet", "unknown"}


@sessions_bp.route("/sessions/summary")
def sessions_summary():
    country = request.args.get("country", "").upper() or None
    device  = request.args.get("device",  "").lower() or None
    date    = request.args.get("date",    "") or None

    if date:
        try:
            date_type.fromisoformat(date)
        except ValueError:
            abort(400, description="date debe tener formato YYYY-MM-DD")

    if device and device not in VALID_DEVICES:
        abort(400, description=f"device debe ser uno de: {sorted(VALID_DEVICES)}")

    filters = []
    params  = []

    if date:
        filters.append("event_date = %s")
        params.append(date)
    if country:
        filters.append("country = %s")
        params.append(country)
    if device:
        filters.append("device_type = %s")
        params.append(device)

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    sql = f"""
        SELECT device_type, country, event_date::text as event_date,
               avg_time_seconds, total_sessions, unique_users
        FROM time_by_device_country
        {where}
        ORDER BY total_sessions DESC
        LIMIT 100
    """

    rows = query_to_list(sql, tuple(params))
    total_sessions = sum(r["total_sessions"] or 0 for r in rows)
    total_users    = sum(r["unique_users"]    or 0 for r in rows)

    return jsonify({
        "date":           date,
        "filters":        {"country": country, "device": device},
        "total_sessions": total_sessions,
        "total_users":    total_users,
        "count":          len(rows),
        "data":           rows,
    })


@sessions_bp.route("/filters")
def available_filters():
    countries = query_to_list("SELECT DISTINCT country FROM time_by_device_country ORDER BY country")
    devices   = query_to_list("SELECT DISTINCT device_type FROM time_by_device_country ORDER BY device_type")
    return jsonify({
        "countries": [r["country"] for r in countries],
        "devices":   [r["device_type"] for r in devices],
    })

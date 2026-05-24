from datetime import date as date_type
from flask import Blueprint, request, jsonify, abort
from db import query_to_list

sessions_bp = Blueprint("sessions", __name__)

VALID_DEVICES = {"mobile", "desktop", "tablet", "unknown"}

@sessions_bp.route("/sessions/summary")
def sessions_summary():
    country = request.args.get("country", "").upper() or None
    device  = request.args.get("device",  "").lower() or None
    date    = request.args.get("date", str(date_type.today()))

    try:
        date_type.fromisoformat(date)
    except ValueError:
        abort(400, description="date debe tener formato YYYY-MM-DD")

    if device and device not in VALID_DEVICES:
        abort(400, description=f"device debe ser uno de: {sorted(VALID_DEVICES)}")

    filters = ["event_date = %s"]
    params  = [date]

    if country:
        filters.append("country = %s")
        params.append(country)

    if device:
        filters.append("device_type = %s")
        params.append(device)

    where = " AND ".join(filters)
    sql = f"""
        SELECT device_type, country, avg_time_seconds, total_sessions, unique_users
        FROM time_by_device_country
        WHERE {where}
        ORDER BY total_sessions DESC
    """

    rows = query_to_list(sql, tuple(params))
    total_sessions = sum(r["total_sessions"] or 0 for r in rows)
    total_users    = sum(r["unique_users"]    or 0 for r in rows)

    return jsonify({
        "date": date,
        "filters": {"country": country, "device": device},
        "total_sessions": total_sessions,
        "total_users": total_users,
        "count": len(rows),
        "data": rows,
    })

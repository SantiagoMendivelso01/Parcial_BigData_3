from datetime import date as date_type
from flask import Blueprint, request, jsonify, abort
from db import query_to_list

pages_bp = Blueprint("pages", __name__)

@pages_bp.route("/pages/top")
def top_pages():
    metric = request.args.get("metric", "time_on_page")
    date   = request.args.get("date", str(date_type.today()))
    limit  = request.args.get("limit", "20")

    if metric not in ("bounce_rate", "time_on_page"):
        abort(400, description="metric debe ser 'bounce_rate' o 'time_on_page'")

    try:
        date_type.fromisoformat(date)
    except ValueError:
        abort(400, description="date debe tener formato YYYY-MM-DD")

    try:
        limit_int = max(1, min(int(limit), 100))
    except ValueError:
        abort(400, description="limit debe ser entero")

    if metric == "time_on_page":
        sql = """
            SELECT page_url, page_type, avg_time_seconds, total_visits
            FROM top_pages_by_time
            WHERE event_date = %s
            ORDER BY avg_time_seconds DESC
            LIMIT %s
        """
    else:
        sql = """
            SELECT page_type, total_sessions, bounce_sessions, bounce_rate_pct
            FROM bounce_rate
            WHERE event_date = %s
            ORDER BY bounce_rate_pct DESC
            LIMIT %s
        """

    rows = query_to_list(sql, (date, limit_int))
    return jsonify({"metric": metric, "date": date, "count": len(rows), "data": rows})

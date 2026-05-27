from datetime import date as date_type
from flask import Blueprint, request, jsonify, abort
from db import query_to_list

pages_bp = Blueprint("pages", __name__)


@pages_bp.route("/pages/top")
def top_pages():
    metric = request.args.get("metric", "time_on_page")
    date   = request.args.get("date",   "") or None
    limit  = request.args.get("limit",  "20")

    if metric not in ("bounce_rate", "time_on_page"):
        abort(400, description="metric debe ser 'bounce_rate' o 'time_on_page'")

    if date:
        try:
            date_type.fromisoformat(date)
        except ValueError:
            abort(400, description="date debe tener formato YYYY-MM-DD")

    try:
        limit_int = max(1, min(int(limit), 100))
    except ValueError:
        abort(400, description="limit debe ser un entero entre 1 y 100")

    if metric == "time_on_page":
        where = "WHERE event_date = %s" if date else ""
        params = (date, limit_int) if date else (limit_int,)
        sql = f"""
            SELECT page_url, page_type,
                   ROUND(AVG(avg_time_seconds)::numeric, 2) as avg_time_seconds,
                   SUM(total_visits) as total_visits
            FROM top_pages_by_time
            {where}
            GROUP BY page_url, page_type
            ORDER BY avg_time_seconds DESC
            LIMIT %s
        """
    else:
        where = "WHERE event_date = %s" if date else ""
        params = (date, limit_int) if date else (limit_int,)
        sql = f"""
            SELECT page_type,
                   SUM(total_sessions) as total_sessions,
                   SUM(bounce_sessions) as bounce_sessions,
                   ROUND(AVG(bounce_rate_pct)::numeric, 2) as bounce_rate_pct
            FROM bounce_rate
            {where}
            GROUP BY page_type
            ORDER BY bounce_rate_pct DESC
            LIMIT %s
        """

    rows = query_to_list(sql, params)
    return jsonify({
        "metric": metric,
        "date":   date,
        "count":  len(rows),
        "data":   rows,
    })


@pages_bp.route("/dates")
def available_dates():
    sql = """
        SELECT DISTINCT event_date::text as date
        FROM bounce_rate
        ORDER BY event_date DESC
        LIMIT 30
    """
    rows = query_to_list(sql)
    return jsonify({"dates": [r["date"] for r in rows]})

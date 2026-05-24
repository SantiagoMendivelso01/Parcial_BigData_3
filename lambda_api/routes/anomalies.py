from datetime import date as date_type
from flask import Blueprint, request, jsonify, abort
from db import query_to_list

anomalies_bp = Blueprint("anomalies", __name__)

@anomalies_bp.route("/anomalies")
def list_anomalies():
    date         = request.args.get("date", str(date_type.today()))
    anomaly_type = request.args.get("type", None)
    min_zscore   = request.args.get("min_zscore", "0")

    try:
        date_type.fromisoformat(date)
    except ValueError:
        abort(400, description="date debe tener formato YYYY-MM-DD")

    try:
        min_zscore_f = float(min_zscore)
    except ValueError:
        abort(400, description="min_zscore debe ser numero")

    filters = ["event_date = %s", "z_score >= %s"]
    params  = [date, min_zscore_f]

    if anomaly_type:
        filters.append("anomaly_type = %s")
        params.append(anomaly_type)

    where = " AND ".join(filters)
    sql = f"""
        SELECT session_id, user_id, page_url, page_type,
               ROUND(z_score::numeric, 4) AS z_score,
               anomaly_type, anomaly_field,
               ROUND(anomaly_value::numeric, 2) AS anomaly_value,
               detected_at
        FROM anomalies
        WHERE {where}
        ORDER BY z_score DESC
        LIMIT 500
    """

    rows = query_to_list(sql, tuple(params))

    type_counts = {}
    for r in rows:
        t = r.get("anomaly_type", "unknown")
        type_counts[t] = type_counts.get(t, 0) + 1

    return jsonify({
        "date": date,
        "filters": {"type": anomaly_type, "min_zscore": min_zscore_f},
        "total": len(rows),
        "by_type": type_counts,
        "data": rows,
    })

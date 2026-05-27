import os
from flask import Flask, jsonify
from routes.pages import pages_bp
from routes.sessions import sessions_bp
from routes.anomalies import anomalies_bp

app = Flask(__name__)
app.register_blueprint(pages_bp)
app.register_blueprint(sessions_bp)
app.register_blueprint(anomalies_bp)

@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "shopstream-api"})

@app.errorhandler(400)
def bad_request(e):
    return jsonify({"error": "bad_request", "message": str(e)}), 400

@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "not_found", "message": str(e)}), 404

if __name__ == "__main__":
    app.run(debug=True, port=5000)

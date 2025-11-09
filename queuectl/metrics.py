import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from . import db


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        counts = db.job_counts()
        payload = json.dumps(counts).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


def serve(port: int = 8000):
    db.init_db()
    server = ThreadedHTTPServer(("", port), MetricsHandler)
    print(f"Metrics server listening on 0.0.0.0:{port} (endpoint /metrics)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()

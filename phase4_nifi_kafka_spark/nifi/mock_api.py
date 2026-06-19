# Q2: NiFi — REST API Source
from http.server import HTTPServer, BaseHTTPRequestHandler
import json

# ── SOURCE DATA ───────────────────────────────────────────────────────────────
# Five hardcoded transaction records that NiFi's InvokeHTTP processor fetches.
# status=ERROR records are routed to an alert folder in the NiFi flow;
# status=SUCCESS records go to a processed folder via RouteOnAttribute.
RECORDS = [
    {"id": 1, "name": "Transaction A", "amount": 250.0,  "status": "SUCCESS"},
    {"id": 2, "name": "Transaction B", "amount": 1200.0, "status": "ERROR"},
    {"id": 3, "name": "Transaction C", "amount": 75.5,   "status": "SUCCESS"},
    {"id": 4, "name": "Transaction D", "amount": 980.0,  "status": "ERROR"},
    {"id": 5, "name": "Transaction E", "amount": 45.0,   "status": "SUCCESS"},
]

# ── REQUEST HANDLER ───────────────────────────────────────────────────────────
class MockAPIHandler(BaseHTTPRequestHandler):
    # do_GET: responds to every GET request with HTTP 200 + full records list as JSON.
    # NiFi's InvokeHTTP processor calls this on each scheduled fetch.
    def do_GET(self):
        body = json.dumps(RECORDS).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # Suppress default per-request access log lines so terminal output stays clean
    def log_message(self, *args):
        print(f'[Mock API] {args[0]} {args[1]}')

# ── SERVER STARTUP ────────────────────────────────────────────────────────────
if __name__ == '__main__':
    server = HTTPServer(('localhost', 8888), MockAPIHandler)
    print('Mock API running at http://localhost:8888 — press Ctrl+C to stop')
    server.serve_forever()

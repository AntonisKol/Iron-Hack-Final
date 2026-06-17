from http.server import HTTPServer, BaseHTTPRequestHandler
import json

RECORDS = [
    {"id": 1, "name": "Transaction A", "amount": 250.0,  "status": "SUCCESS"},
    {"id": 2, "name": "Transaction B", "amount": 1200.0, "status": "ERROR"},
    {"id": 3, "name": "Transaction C", "amount": 75.5,   "status": "SUCCESS"},
    {"id": 4, "name": "Transaction D", "amount": 980.0,  "status": "ERROR"},
    {"id": 5, "name": "Transaction E", "amount": 45.0,   "status": "SUCCESS"},
]

class MockAPIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(RECORDS).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        print(f'[Mock API] {args[0]} {args[1]}')

if __name__ == '__main__':
    server = HTTPServer(('localhost', 8888), MockAPIHandler)
    print('Mock API running at http://localhost:8888 — press Ctrl+C to stop')
    server.serve_forever()

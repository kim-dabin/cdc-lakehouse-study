"""Internal-only, fixed-command endpoint for Airflow. No arbitrary shell execution."""
import json
import subprocess
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

jobs = {}
mutex = threading.Lock()

def execute(job_id):
    path = Path('artifacts/runs') / f'airflow-{job_id}.log'
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w') as output:
        proc = subprocess.run(['python', 'scripts/pipeline.py', '--wait'], stdout=output, stderr=subprocess.STDOUT)
    with mutex:
        jobs[job_id] = {'state': 'success' if proc.returncode == 0 else 'failed', 'log': str(path)}

class Handler(BaseHTTPRequestHandler):
    def respond(self, status, data):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
    def do_POST(self):
        if self.path != '/runs':
            return self.respond(404, {})
        with mutex:
            if any(x['state']=='running' for x in jobs.values()):
                return self.respond(409, {'error':'A pipeline is already running'})
            job_id = str(uuid.uuid4())
            jobs[job_id] = {'state':'running'}
        threading.Thread(target=execute, args=(job_id,), daemon=True).start()
        self.respond(202, {'id':job_id})
    def do_GET(self):
        if self.path == '/health':
            return self.respond(200, {'status':'ok'})
        job_id = self.path.removeprefix('/runs/')
        with mutex:
            state = jobs.get(job_id)
        self.respond(200 if state else 404, state or {})

if __name__ == '__main__':
    ThreadingHTTPServer(('0.0.0.0',8080), Handler).serve_forever()

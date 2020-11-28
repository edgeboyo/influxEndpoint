from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from chirpstack_api.as_pb import integration
from google.protobuf.json_format import Parse

from influxdb import InfluxDBClient

import json

from datetime import datetime

class Handler(BaseHTTPRequestHandler):
    # True -  JSON marshaler
    # False - Protobuf marshaler (binary)
    json = True

    integrator = InfluxDBClient('localhost', 8086, 'root', 'root', 'traceB')

    def do_POST(self):
        self.send_response(200)
        self.end_headers()
        query_args = parse_qs(urlparse(self.path).query)

        content_len = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_len)

        if query_args["event"][0] == "up":
            self.up(body)

        elif query_args["event"][0] == "join":
            self.join(body)

        else:
            print("handler for event %s is not implemented" % query_args["event"][0])

    def up(self, body):
        mjson = body.decode("utf8")
        mjson = "[" + mjson + "]"
        #print(mjson)
        mjson = json.loads(mjson)
        #print(mjson)
        json_body = [
    {
        "measurement": "event_seen",
        "time": datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "fields": {
            "present": 1
        }
    }
]
        up = self.unmarshal(body, integration.UplinkEvent())
        chk = self.integrator.write_points(json_body)
        print(chk)

    def join(self, body):
        join = self.unmarshal(body, integration.JoinEvent())
        print("Device: %s joined with DevAddr: %s" % (join.dev_eui.hex(), join.dev_addr.hex()))

    def unmarshal(self, body, pl):
        if self.json:
            return Parse(body, pl)

        pl.ParseFromString(body)
        return pl

httpd = HTTPServer(('', 8090), Handler)
httpd.serve_forever()

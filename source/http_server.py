import logging
from http.server import BaseHTTPRequestHandler

from pathlib2 import Path

from source.utils import Nodes


class HTTPServerRequestHandler(BaseHTTPRequestHandler):
    http_logger = logging.getLogger('monitor_http')

    def do_GET(self):
        try:
            # Check the file extension required and
            # set the right mime type
            if self.path.endswith(".css"):
                # Open the static file requested and send it
                f = Path("source" + self.path)
                self.send_response(200)
                self.send_header('Content-type', 'text/css')
                self.end_headers()
                self.wfile.write(f.read_bytes())
                return
            else:
                self.send_response(200)
                self.send_header('Content-type', 'text/html')
                self.end_headers()
                tabul_nodes = [[n.node_num, n.bid_id, n.price, n.deal_id, n.task_id, n.task_uptime, n.status.name] for n
                               in
                               Nodes.get_nodes()]

                html = """<html>
                       <head>
                       <link rel="stylesheet" type="text/css" href="css/sonm-auto.css">
                       </head>
                       <table border="1"><tr>
                       <th>Node</th><th>Order id</th><th>Order Price</th>
                       <th>Deal id</th><th>Task id</th><th>Task uptime</th><th>Node status</th>
                       </tr>"""
                for row in tabul_nodes:
                    html += "<tr>"
                    for cell in row:
                        html += "<td>{}</td>".format(cell)
                    html += "</tr>"
                html += "</table></html>"
                self.wfile.write(bytes(html, "utf8"))
                return
        except IOError:
            self.send_error(404, 'File Not Found: %s' % self.path)

    def log_message(self, format, *args):
        self.http_logger.info("%s - - [%s] %s\n" %
                              (self.address_string(),
                               self.log_date_time_string(),
                               format % args))

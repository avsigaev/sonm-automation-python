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
                content, mime = self.get_css()
            else:
                content, mime = self.get_html()
            self.send_response(200)
            self.send_header('Content-type', mime)
            self.end_headers()
            self.wfile.write(content)
        except IOError:
            self.send_error(404, 'File Not Found: %s' % self.path)

    def get_html(self):
        mime = 'text/html'
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
        content = bytes(html, "utf8")
        return content, mime

    def get_css(self):
        # Open the static file requested and send it
        mime = 'text/css'
        f = Path("resources" + self.path)
        content = f.read_bytes()
        return content, mime

    def log_message(self, format, *args):
        self.http_logger.info("%s - - [%s] %s\n" %
                              (self.address_string(),
                               self.log_date_time_string(),
                               format % args))

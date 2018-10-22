import logging
from http.server import BaseHTTPRequestHandler

from source.utils import Nodes


class HTTPServerRequestHandler(BaseHTTPRequestHandler):
    http_logger = logging.getLogger('monitor_http')

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        tabul_nodes = [[n.node_num, n.bid_id, n.deal_id, n.task_id, n.task_uptime, n.status.name] for n in
                       Nodes.get_nodes()]

        html = """<html><table border="1"><tr>
        <th>Node</th><th>Order id</th><th>Deal id</th><th>Task id</th><th>Task uptime</th><th>Node status</th>
        </tr>"""
        for row in tabul_nodes:
            html += "<tr>"
            for cell in row:
                html += "<td>{}</td>".format(cell)
            html += "</tr>"
        html += "</table></html>"
        self.wfile.write(bytes(html, "utf8"))
        return

    def log_message(self, format, *args):
        self.http_logger.info("%s - - [%s] %s\n" %
                              (self.address_string(),
                               self.log_date_time_string(),
                               format % args))

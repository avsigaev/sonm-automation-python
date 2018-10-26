import base64
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

from pathlib2 import Path

from source.utils import Nodes
from source.config import Config

logger = logging.getLogger("monitor")


class SonmHttpServer:
    KEEP_RUNNING = True


class HTTPServerRequestHandler(BaseHTTPRequestHandler):
    http_logger = logging.getLogger('monitor_http')

    KEY = ''

    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_authhead(self):
        self.send_response(401)
        self.send_header('WWW-Authenticate', 'Basic realm=\"Test\"')
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):

        if self.headers.get('Authorization') is None:
            self.do_authhead()
            self.wfile.write('no auth header received'.encode('utf-8'))
        elif self.headers.get('Authorization') == 'Basic ' + self.KEY:
            self.handle_get()
        else:
            self.do_authhead()
            self.wfile.write('not authenticated'.encode('utf-8'))

    def handle_get(self):
        try:
            # TODO simple auth, price, clean state force (for task) , stop all (for task)
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
        tabul_nodes = [[n.node_tag, n.bid_id, n.price, n.deal_id, n.task_id, n.task_uptime, n.status.name] for n
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


def run_http_server():
    if "http_server" in Config.base_config and "run" in Config.base_config["http_server"]:
        if not Config.base_config["http_server"]["run"]:
            return
        if "password" in Config.base_config["http_server"] and "user" in Config.base_config["http_server"]:
            passwd_ = "{}:{}".format(Config.base_config["http_server"]["user"],
                                     Config.base_config["http_server"]["password"])
            HTTPServerRequestHandler.KEY = base64.b64encode(passwd_.encode('utf-8')).decode('utf-8')
        logger.info('Starting HTTP server...')
        server = HTTPServer(('0.0.0.0', Config.base_config["http_server"]["port"]), HTTPServerRequestHandler)
        logger.info("Agent started on port: {}".format(Config.base_config["http_server"]["port"]))

        thread = get_http_thread(server)

        while SonmHttpServer.KEEP_RUNNING:
            if not thread.is_alive():
                thread = get_http_thread(server)
            time.sleep(1)
        logger.info("Http server stopped")


def get_http_thread(server):
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    return thread

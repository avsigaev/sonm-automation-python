import base64


def parse_tag(order_):
    return base64.b64decode(order_).decode().strip("\0")
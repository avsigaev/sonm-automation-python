from datetime import datetime


def log(s):
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " " + s)
import json
import logging
import subprocess
import time


class Cli:
    def __init__(self, cli_):
        self.cli = cli_
        self.logger = logging.getLogger("monitor")

    def exec(self, param, retry=False, attempts=3, sleep_time=1):
        command = [self.cli] + param
        command.append("--json")
        attempt = 1
        errors_ = []
        while True:
            result = subprocess.run(command, stdout=subprocess.PIPE)
            if result.returncode == 0:
                break
            errors_.append(str(result.stdout.decode("utf-8")))
            if not retry or attempt > attempts:
                break
            attempt += 1
            time.sleep(sleep_time)
        if result.returncode != 0:
            self.logger.error("Failed to execute command: {}".format(' '.join(command)))
            self.logger.error('\n'.join(errors_))
            return None
        if result.stdout.decode("utf-8") == "null":
            return {}
        return json.loads(result.stdout.decode("utf-8"))

    def save_task_logs(self, deal_id, task_id, rownum, filename):
        command = [self.cli, "task", "logs", deal_id, task_id, "--tail", rownum]
        with open(filename, "w") as outfile:
            subprocess.call(command, stdout=outfile)

    def order_create(self, bid_file):
        return self.exec(["order", "create", bid_file])

    def order_list(self, number_of_nodes):
        return self.exec(["order", "list", "--timeout=2m", "--limit", str(number_of_nodes)], retry=True)

    def order_status(self, order_id):
        return self.exec(["order", "status", str(order_id)], retry=True)

    def deal_list(self, number_of_nodes):
        return self.exec(["deal", "list", "--timeout=2m", "--limit", str(number_of_nodes)], retry=True)

    def deal_status(self, deal_id):
        return self.exec(["deal", "status", deal_id, "--expand"], retry=True)

    def deal_close(self, deal_id, bl_worker=False):
        close_d_command = ["deal", "close", deal_id]
        if bl_worker:
            close_d_command += ["--blacklist", "worker"]
        return self.exec(close_d_command, retry=True)

    def task_status(self, deal_id, task_id):
        return self.exec(["task", "status", deal_id, task_id, "--timeout=2m"], retry=True)

    def task_start(self, deal_id, task_file):
        return self.exec(["task", "start", deal_id, task_file, "--timeout=15m"], retry=True)

    def task_list(self, deal_id, attempts=5, sleep_time=5):
        # TODO temp workaround!!!
        attempt = 1
        while True:
            resp = self.exec(["task", "list", deal_id, "--timeout=2m"], retry=True)
            if resp and "error" in resp.keys():
                if attempt > attempts:
                    self.logger.error("Received response: {}".format(resp))
                    break
                self.logger.error("Attempt {},  received response: {}".format(attempt, resp))
                attempt += 1
                time.sleep(sleep_time)
            return resp

#!/usr/bin/env python3
import json
import subprocess
import threading
import time

from source.utils import get_sonmcli


def execute_cli_command(command, retry=False, attempts=3, sleep_time=1):
    command.append("--json")
    attempt = 1
    errors_ = []
    while True:
        result = subprocess.run([get_sonmcli()] + command, stdout=subprocess.PIPE)
        if result.returncode == 0:
            break
        errors_.append(str(result.stdout.decode("utf-8")))
        if not retry or attempt > attempts:
            break
        attempt += 1
        time.sleep(sleep_time)
    if result.returncode != 0:
        print("Failed to execute command: {}".format(' '.join(command)))
        print('\n'.join(errors_))
        return None
    if result.stdout.decode("utf-8") == "null":
        return {}
    return json.loads(result.stdout.decode("utf-8"))


def clear_blacklist(address):
    print("Removing " + address + " from blacklist...")
    status = execute_cli_command(["blacklist", "remove", address, "--timeout=2m"])
    if status and status is not None:
        print("[OK] " + address + " successfully removed from blacklist")
    else:
        print("[ERR] Error occurred while removing " + address + "from blacklist.")
    exit(0)


def get_blacklist():
    addresses = execute_cli_command(["blacklist", "list", "--timeout=2m"])
    if addresses and len(addresses) > 1:
        print("Blacklist contains addresses: ")
        for i in addresses["addresses"]:
            print(i)
        print("========")
        return addresses["addresses"]
    else:
        print("Blacklist is empty.")
        exit(0)


def main():
    blacklist = get_blacklist()
    for address in blacklist:
        threading.Thread(target=clear_blacklist, kwargs={'address': address}).start()


if __name__ == "__main__":
    main()

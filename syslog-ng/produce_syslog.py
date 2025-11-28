import socket
import time
from generate_syslog import generate_log

SYSLOG_HOST = "localhost"
SYSLOG_PORT = 5514

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

if __name__ == "__main__":
    print(f"Sending logs to syslog-ng at {SYSLOG_HOST}:{SYSLOG_PORT}")
    while True:
        log = generate_log()
        sock.sendto(log.encode(), (SYSLOG_HOST, SYSLOG_PORT))
        print("SENT:", log)
        time.sleep(0.3)

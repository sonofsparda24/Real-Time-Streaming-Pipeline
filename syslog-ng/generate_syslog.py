import random
import time
import datetime
import sys

HOSTNAMES = ["web-01", "db-01", "auth-01", "cache-01", "gateway-01", "worker-03"]
USERS = ["root", "ubuntu", "admin", "deploy", "www-data"]
IP_ADDRESSES = ["192.168.1.10", "192.168.1.22", "10.0.0.5", "172.16.0.9", "8.8.8.8"]

PROGRAMS = [
    "sshd", "sudo", "cron", "kernel", "nginx", "apache2",
    "systemd", "docker", "mysql", "ufw"
]


def generate_sshd():
    return f"Failed password for {random.choice(USERS)} from {random.choice(IP_ADDRESSES)} port 22 ssh2"


def generate_sudo():
    return f"user {random.choice(USERS)} executed command: /usr/bin/apt update"


def generate_cron():
    return "CRON[987]: (root) CMD (/usr/bin/backup.sh)"


def generate_kernel():
    return "kernel: CPU temperature above threshold, cpu clock throttled"


def generate_nginx():
    return f'access log: "{random.choice(["GET","POST"])} /index.html" 200 from {random.choice(IP_ADDRESSES)}'


def generate_apache():
    return f"[client {random.choice(IP_ADDRESSES)}] File does not exist: /var/www/favicon.ico"


def generate_systemd():
    return "systemd: Starting Daily apt upgrade and clean activities..."


def generate_docker():
    return "docker: Container restarted: app_container_01"


def generate_mysql():
    return "mysql: Slow query detected on table users (execution: 4.2 sec)"


def generate_ufw():
    return f"UFW BLOCK: IN=eth0 SRC={random.choice(IP_ADDRESSES)} DST=192.168.1.5"


GENERATORS = [
    generate_sshd,
    generate_sudo,
    generate_cron,
    generate_kernel,
    generate_nginx,
    generate_apache,
    generate_systemd,
    generate_docker,
    generate_mysql,
    generate_ufw,
]


def generate_log():
    timestamp = datetime.datetime.now().strftime("%b %d %H:%M:%S")
    host = random.choice(HOSTNAMES)
    program = random.choice(PROGRAMS)
    message = random.choice(GENERATORS)()
    return f"{timestamp} {host} {program}: {message}"


if __name__ == "__main__":
    # If an argument is provided, attempt to parse it as an integer
    if len(sys.argv) > 1:
        try:
            num_logs = int(sys.argv[1])
            if num_logs <= 0:
                sys.exit(0)
            for _ in range(num_logs):
                print(generate_log())
                time.sleep(0.5)
            sys.exit(0)
        except ValueError:
            print("Error: argument must be an integer (example: python generate_syslog.py 50)")
            sys.exit(1)

    # Default behavior: infinite log generation
    while True:
        print(generate_log())
        time.sleep(0.5)

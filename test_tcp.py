import socket

host = "149.154.166.110"
port = 443

print(f"Подключаемся к {host}:{port}...")

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(10)

try:
    sock.connect((host, port))
    print("TCP-соединение успешно!")
except Exception as e:
    print("ОШИБКА:")
    print(type(e).__name__)
    print(repr(e))
finally:
    sock.close()
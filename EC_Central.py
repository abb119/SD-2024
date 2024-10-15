import socket
import threading
import sys

# Inicializamos el mapa 2D de la ciudad como un array de bytes
MAP_SIZE = 20  # 20x20 posiciones
city_map = [[0 for _ in range(MAP_SIZE)] for _ in range(MAP_SIZE)]  # Un array de bytes (enteros en este caso)

# Base de datos de taxis (puedes actualizar con IDs reales de taxis)
taxi_db = {1: "Taxi1", 2: "Taxi2", 3: "Taxi3"}

def handle_taxi(conn, addr):
    """Manejar la conexión de un taxi."""
    print(f"Conexión recibida de {addr}")
    try:
        while True:
            # Recibe el mensaje del taxi
            message = conn.recv(1024).decode()
            if not message:
                break

            # Protocolo de mensajes: "ID#ACTION"
            parts = message.split("#")
            taxi_id = int(parts[0])
            action = parts[1]

            # Verificar si el taxi está registrado
            if taxi_id in taxi_db:
                if action == "INIT":
                    response = "OK#Taxi registrado y listo"
                    print(f"Taxi {taxi_id} registrado. Estado: OK")
                elif action == "STATUS":
                    # Actualizar el mapa o realizar acciones según el estado
                    response = f"OK#Estado recibido de Taxi {taxi_id}"
                elif action.startswith("MOVE"):
                    # Protocolo para mover el taxi: "MOVE#x#y"
                    x = int(parts[2])
                    y = int(parts[3])
                    move_taxi_on_map(taxi_id, x, y)
                    response = f"OK#Taxi {taxi_id} movido a ({x}, {y})"
                    send_map(conn)
                else:
                    response = "KO#Acción desconocida"
            else:
                response = "KO#Taxi no registrado"

            # Enviar respuesta
            conn.send(response.encode())
    
    except Exception as e:
        print(f"Error con el taxi {addr}: {e}")
    finally:
        conn.close()

def move_taxi_on_map(taxi_id, x, y):
    """Mueve un taxi en el mapa a la posición (x, y)."""
    # Limpiamos el mapa primero (quitar la posición anterior del taxi)
    for row in range(MAP_SIZE):
        for col in range(MAP_SIZE):
            if city_map[row][col] == taxi_id:
                city_map[row][col] = 0

    # Actualizamos el mapa con la nueva posición del taxi
    city_map[x][y] = taxi_id
    print(f"Taxi {taxi_id} movido a la posición ({x}, {y}) en el mapa.")

def send_map(conn):
    """Envia el mapa completo al taxi."""
    map_as_bytes = bytearray()
    for row in city_map:
        for cell in row:
            map_as_bytes.append(cell)

    conn.send(map_as_bytes)
    print("Mapa enviado al taxi.")

def start_central(listen_port):
    """Iniciar la Central sin Broker ni Base de Datos."""
    # Inicia la central en el puerto indicado por el usuario
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', listen_port))  # Escuchar en todas las interfaces
    server.listen(5)
    print(f"Central iniciada en el puerto {listen_port} y esperando taxis...")

    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_taxi, args=(conn, addr))
        thread.start()

if __name__ == "__main__":
    # Comprobar los argumentos pasados por línea de comandos
    if len(sys.argv) < 2:
        print("Uso: python EC_Central.py <puerto_escucha>")
        sys.exit(1)
    
    # Obtener el puerto de escucha desde los argumentos de la línea de comandos
    listen_port = int(sys.argv[1])  # Puerto de escucha de la Central

    # Iniciar la Central sin Broker ni Base de Datos
    start_central(listen_port)

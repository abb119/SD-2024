# EC_DE.py (Digital Engine del Taxi)
import socket
import time

def handle_sensor_connection(conn):
    """Manejar los mensajes entrantes del sensor."""
    try:
        while True:
            sensor_data = conn.recv(1024).decode()
            if not sensor_data:
                break
            print(f"Digital Engine recibe del sensor: {sensor_data}")
            # Reaccionar a la información del sensor
            if sensor_data == 'o':
                print("Taxi en movimiento")
            elif sensor_data == 'r':
                print("Taxi detenido por semáforo en rojo")
            elif sensor_data == 'p':
                print("Taxi detenido por peatón cruzando")
    except Exception as e:
        print(f"Error en Digital Engine: {e}")
    finally:
        conn.close()

def connect_to_central(taxi_id):
    """Conectar el Digital Engine a la central y recibir comandos."""
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(('192.168.x.x', 8080))  # IP de la Central

    # Iniciar el taxi enviando ID y acción INIT
    init_message = f"{taxi_id}#INIT"
    client.send(init_message.encode())
    
    # Recibir respuesta de la central
    response = client.recv(1024).decode()
    print(f"Respuesta de la central: {response}")

    # Simular movimiento y respuesta de la central según el estado del sensor
    # Aquí puedes implementar la lógica para responder a la central
    client.close()

def start_engine():
    """Iniciar el servidor del Digital Engine para recibir datos del sensor."""
    engine = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    engine.bind(('0.0.0.0', 9090))  # Escuchar conexiones del sensor
    engine.listen(5)
    print("Digital Engine esperando conexión del sensor...")

    while True:
        conn, addr = engine.accept()
        handle_sensor_connection(conn)

if __name__ == "__main__":
    # Iniciar el Digital Engine para comunicarse con el sensor
    taxi_id = 1
    start_engine()
    connect_to_central(taxi_id)


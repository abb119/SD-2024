import socket
import threading
import sys

current_taxi_status = "OK"  # El estado del taxi, cambia si ocurre una contingencia
central_ip = None
central_port = None

def handle_sensor_connection(conn, addr):
    """Manejar los mensajes entrantes del sensor."""
    global current_taxi_status
    try:
        while True:
            sensor_data = conn.recv(1024).decode()
            if not sensor_data:
                break

            print(f"Digital Engine recibe del sensor {addr}: {sensor_data}")

            # Reaccionar a la información del sensor
            if sensor_data == 'o':
                print("Taxi en movimiento")
                if current_taxi_status != "OK":
                    # Si está en estado de contingencia, reanuda el movimiento
                    current_taxi_status = "OK"
                    notify_central(f"TAXI_OK#{addr}#El taxi ha reanudado el viaje.")
            elif sensor_data == 'r':
                print("Taxi detenido por semáforo en rojo")
                if current_taxi_status != "STOP":
                    current_taxi_status = "STOP"
                    notify_central(f"TAXI_ALERT#{addr}#Semáforo rojo, taxi detenido.")
            elif sensor_data == 'p':
                print("Taxi detenido por peatón cruzando")
                if current_taxi_status != "STOP":
                    current_taxi_status = "STOP"
                    notify_central(f"TAXI_ALERT#{addr}#Peatón cruzando, taxi detenido.")
    
    except Exception as e:
        print(f"Error en Digital Engine con sensor {addr}: {e}")
    finally:
        conn.close()

def notify_central(message):
    """Notificar a la central sobre el estado actual del taxi."""
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((central_ip, central_port))
        client.send(message.encode())
        print(f"Mensaje enviado a la Central: {message}")
        client.close()
    except Exception as e:
        print(f"Error al notificar a la Central: {e}")

def connect_to_central(taxi_id, central_ip_arg, central_port_arg):
    """Conectar el Digital Engine a la central y enviar comandos iniciales."""
    global central_ip, central_port
    central_ip = central_ip_arg
    central_port = central_port_arg

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((central_ip, central_port))

    # Iniciar el taxi enviando ID y acción INIT
    init_message = f"{taxi_id}#INIT"
    client.send(init_message.encode())
    
    # Recibir respuesta de la central
    response = client.recv(1024).decode()
    print(f"Respuesta de la central: {response}")

    client.close()

def start_engine(sensor_port):
    """Iniciar el servidor del Digital Engine para recibir datos del sensor."""
    engine = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    engine.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    engine.bind(('0.0.0.0', sensor_port))  # Escuchar en todas las interfaces
    engine.listen(5)
    print(f"Taxi esperando conexión del sensor en el puerto {sensor_port}...")

    while True:
        conn, addr = engine.accept()
        thread = threading.Thread(target=handle_sensor_connection, args=(conn, addr))
        thread.start()

if __name__ == "__main__":
    # Comprobar que se pasaron suficientes argumentos
    if len(sys.argv) < 5:
        print("Uso: python EC_DE.py <taxi_id> <central_ip> <central_port> <sensor_port>")
        sys.exit(1)

    # Leer argumentos desde la línea de comandos
    taxi_id = int(sys.argv[1])        # ID del taxi
    central_ip = sys.argv[2]          # IP de la Central
    central_port = int(sys.argv[3])   # Puerto de la Central
    sensor_port = int(sys.argv[4])    # Puerto para escuchar a los sensores

    # Conectar primero con la Central
    central_thread = threading.Thread(target=connect_to_central, args=(taxi_id, central_ip, central_port))
    central_thread.start()

    # Iniciar el servidor del Digital Engine
    start_engine(sensor_port)

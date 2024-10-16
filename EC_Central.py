import socket
import threading
import sys
from kafka import KafkaProducer, KafkaConsumer
import json

# Base de datos de taxis simulada
taxi_db = {1: "Taxi1", 2: "Taxi2", 3: "Taxi3"}
taxi_status = {1: "AVAILABLE", 2: "AVAILABLE", 3: "AVAILABLE"}  # Estado de taxis

producer = None  # Productor de Kafka para enviar respuestas
taxi_sockets = {}  # Almacena conexiones socket autenticadas por taxi_id


# Configuración del productor de Kafka
def setup_kafka(broker_ip, broker_port):
    global producer
    producer = KafkaProducer(
        bootstrap_servers=f'{broker_ip}:{broker_port}',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


# Función que maneja la autenticación del taxi usando sockets
def handle_taxi_auth(conn, addr):
    try:
        while True:
            # Recibir mensaje del taxi (ej: "taxi_id#INIT")
            message = conn.recv(1024).decode()
            if not message:
                break

            parts = message.split("#")
            taxi_id = int(parts[0])
            action = parts[1]

            if action == "INIT":
                if taxi_id in taxi_db:
                    response = "OK#Taxi autenticado y listo"
                    taxi_status[taxi_id] = "AVAILABLE"
                    taxi_sockets[taxi_id] = conn
                    print(f"Taxi {taxi_id} autenticado correctamente desde {addr}.")
                else:
                    response = "KO#Taxi no registrado"
                    print(f"Taxi {taxi_id} no registrado. Conexión rechazada.")
                conn.send(response.encode())

                # Mantener la conexión abierta si es necesario para futuros mensajes
                if action == "INIT":
                    print(f"Esperando más mensajes del taxi {taxi_id}...")

            else:
                print(f"Acción desconocida de taxi {taxi_id}: {action}")
                conn.send("KO#Acción desconocida".encode())

    except Exception as e:
        print(f"Error en la conexión con el taxi {addr}: {e}")
    finally:
        conn.close()  # Cerrar la conexión una vez que la autenticación ha terminado o el taxi cierra


# Función para iniciar el servidor de autenticación por sockets
def start_taxi_auth_server(central_ip, central_port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((central_ip, central_port))
    server.listen(5)
    print(f"Servidor de autenticación de taxis iniciado en {central_ip}:{central_port}")

    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_taxi_auth, args=(conn, addr))
        thread.start()


# Función que escucha solicitudes de servicio de los taxis vía Kafka
def recibir_solicitudes_taxis():
    consumer = KafkaConsumer(
        'taxi_requests',
        bootstrap_servers=f'{broker_ip}:{broker_port}',
        auto_offset_reset='earliest',
        group_id='central_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Central escuchando solicitudes de taxis vía Kafka...")

    for mensaje in consumer:
        solicitud = mensaje.value
        taxi_id = solicitud.get("taxi_id")
        if taxi_id in taxi_db:
            print(f"Solicitud de taxi {taxi_id}: {solicitud}")
            manejar_solicitud_taxi(solicitud)
        else:
            print(f"Taxi {taxi_id} no registrado.")


# Función para procesar solicitudes de servicio de los taxis
def manejar_solicitud_taxi(solicitud):
    taxi_id = solicitud.get("taxi_id")
    estado = solicitud.get("estado")
    destino = solicitud.get("destino")

    if estado == "END":
        taxi_status[taxi_id] = "AVAILABLE"
        print(f"Taxi {taxi_id} ha completado su viaje al destino {destino} y está disponible.")


# Kafka para recibir y procesar solicitudes de clientes (EC_Customer)
def procesar_solicitudes_clientes():
    consumer = KafkaConsumer(
        'solicitudes_taxi',
        bootstrap_servers=f'{broker_ip}:{broker_port}',
        auto_offset_reset='earliest',
        group_id='central_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Central escuchando solicitudes de clientes...")

    for mensaje in consumer:
        solicitud = mensaje.value
        procesar_solicitud_cliente(solicitud)


def procesar_solicitud_cliente(solicitud):
    cliente_id = solicitud.get("customer_id")
    customer_coords = solicitud.get("customer_coords")
    destino = solicitud.get("destino")

    print(f"Solicitud recibida de cliente {cliente_id}, destino: {destino}, coordenadas: {customer_coords}")

    # Intentar asignar un taxi disponible
    taxi_id = asignar_taxi()

    if taxi_id:
        print(f"Taxi {taxi_id} asignado a cliente {cliente_id}")
        response = {
            "customer_id": cliente_id,
            "taxi_id": taxi_id,
            "status": "OK",
            "mensaje": f"Taxi {taxi_id} ha sido asignado y se dirige a su ubicación."
        }
        # Enviar la solicitud al taxi vía Kafka
        enviar_solicitud_taxi(taxi_id, destino)
    else:
        print(f"No hay taxis disponibles para el cliente {cliente_id}")
        response = {
            "customer_id": cliente_id,
            "status": "NO_TAXI_AVAILABLE",
            "mensaje": "Lo sentimos, no hay taxis disponibles en este momento."
        }

    # Enviar la respuesta al cliente vía Kafka
    producer.send('respuestas_central', value=response)
    producer.flush()


def asignar_taxi():
    """Simula la asignación de un taxi disponible."""
    for taxi_id, status in taxi_status.items():
        if status == "AVAILABLE":
            taxi_status[taxi_id] = "BUSY"
            return taxi_id
    return None


def enviar_solicitud_taxi(taxi_id, destino):
    """Envía una solicitud de servicio al taxi vía Kafka."""
    solicitud = {
        "taxi_id": taxi_id,
        "mensaje": "NUEVO_SERVICIO",
        "destino": destino
    }
    producer.send('taxi_requests', value=solicitud)
    producer.flush()
    print(f"Solicitud de servicio enviada a taxi {taxi_id} para el destino {destino}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Uso: python EC_Central.py <broker_ip> <broker_port> <central_port>")
        sys.exit(1)

    broker_ip = sys.argv[1]
    broker_port = int(sys.argv[2])
    central_port = int(sys.argv[3])

    # Configurar Kafka
    setup_kafka(broker_ip, broker_port)

    # Iniciar el servidor de autenticación de taxis
    threading.Thread(target=start_taxi_auth_server, args=(broker_ip, central_port)).start()

    # Iniciar el procesamiento de solicitudes de clientes y taxis
    threading.Thread(target=procesar_solicitudes_clientes).start()
    threading.Thread(target=recibir_solicitudes_taxis).start()

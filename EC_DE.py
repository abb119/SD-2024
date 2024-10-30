import socket
import threading
import sys
import time
from kafka import KafkaProducer, KafkaConsumer
import json
import queue

# Variables globales
taxi_id = ""
central_ip = ""
central_port = 0
sensor_ip = ""
sensor_port = 0
broker_ip = ""
broker_port = 0
authenticated = False  # Estado de autenticación
producer = None
consumer = None
availability_status = "BUSY"  # Estado de disponibilidad del taxi (AVAILABLE/BUSY)
current_position = [0, 0]  # Posición inicial del taxi
request_queue = queue.Queue()  # Cola para manejar solicitudes de servicio
movement_event = threading.Event()  # Evento para controlar el movimiento
movement_event.set()  # Inicialmente, el taxi puede moverse

# Configuración de Kafka
def setup_kafka(broker_ip, broker_port):
    global producer, consumer
    producer = KafkaProducer(
        bootstrap_servers=f'{broker_ip}:{broker_port}',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5,
        acks='all'
    )
    consumer = KafkaConsumer(
        'taxi_requests',
        bootstrap_servers=f'{broker_ip}:{broker_port}',
        auto_offset_reset='earliest',
        group_id=f'taxi_group_{taxi_id}',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

def autenticar_taxi_con_central():
    global authenticated, availability_status
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((central_ip, central_port))
            mensaje_autenticacion = json.dumps({
                "taxi_id": taxi_id,
                "position": current_position,
                "availability": availability_status,
                "movement": "RUN" if movement_event.is_set() else "STOP"
            })

            s.send(mensaje_autenticacion.encode())
            response = s.recv(1024).decode().strip()
            print(f"Respuesta de la central: {response}")
            if "OK" in response:
                authenticated = True
                availability_status = "AVAILABLE"
                enviar_estado()
                print(f"Taxi {taxi_id} autenticado con éxito y ahora está disponible.")
            else:
                print(f"Error en la autenticación del taxi {taxi_id}: {response}")
    except Exception as e:
        print(f"Error durante la autenticación: {e}")

def recibir_solicitudes_kafka():
    if authenticated:
        print(f"Taxi {taxi_id} esperando solicitudes de servicio vía Kafka...")
        try:
            time.sleep(2)
            for mensaje in consumer:
                solicitud = mensaje.value
                print(f"Debug: Solicitud recibida desde Kafka: {solicitud}")
                
                if str(solicitud.get("taxi_id")) == taxi_id:
                    print(f"Colocando solicitud en la cola para procesarla: {solicitud}")
                    request_queue.put(solicitud)
        except Exception as e:
            print(f"Error al recibir mensajes de Kafka: {e}")

def procesar_solicitudes():
    while True:
        print("Esperando solicitud en la cola...")
        solicitud = request_queue.get()
        print(f"Procesando solicitud: {solicitud}")
        manejar_solicitud(solicitud)
        request_queue.task_done()

def manejar_solicitud(solicitud):
    global availability_status
    if solicitud.get("mensaje") == "NUEVO_SERVICIO":
        origen = solicitud.get("origen")
        destino = solicitud.get("destino")
        cliente_id = solicitud.get("customer_id")
        print(f"Taxi {taxi_id} recibió solicitud para recoger en {origen} y llevar al destino {destino}")
        availability_status = "BUSY"
        enviar_estado()  # Enviar estado actualizado antes de iniciar el viaje
        realizar_viaje(origen, destino, cliente_id)

def enviar_estado():
    message = {
        "taxi_id": taxi_id,
        "availability": availability_status,
        "movement": "RUN" if movement_event.is_set() else "STOP",
        "localizacion": current_position
    }
    try:
        producer.send('taxi_status', value=message)
        producer.flush()
        print(f"Taxi {taxi_id} ha enviado estado: {message['availability']}, {message['movement']}")
    except Exception as e:
        print(f"Error al enviar estado a Kafka: {e}")

def actualizar_posicion_taxi(posicion):
    global current_position
    current_position = posicion
    enviar_estado()

def mover_taxi(destino):
    global movement_event
    destino_coords = destino
    print(f"Iniciando movimiento hacia {destino_coords}")
    while current_position != destino_coords:
        # Esperar hasta que el movimiento esté permitido
        if not movement_event.is_set():
            print(f"Taxi {taxi_id} detenido en la posición: {current_position} debido a movement_status = STOP")
            movement_event.wait()
            print(f"Taxi {taxi_id} reanudando movimiento.")

        # Continuar con el movimiento
        if current_position[0] < destino_coords[0]:
            current_position[0] = (current_position[0] + 1) % 20
        elif current_position[0] > destino_coords[0]:
            current_position[0] = (current_position[0] - 1) % 20

        if current_position[1] < destino_coords[1]:
            current_position[1] = (current_position[1] + 1) % 20
        elif current_position[1] > destino_coords[1]:
            current_position[1] = (current_position[1] - 1) % 20

        actualizar_posicion_taxi(current_position)
        print(f"Taxi {taxi_id} en la posición: {current_position} con movement_status = {'RUN' if movement_event.is_set() else 'STOP'}")
        sys.stdout.flush()
        time.sleep(1)

def realizar_viaje(origen, destino, cliente_id):
    global availability_status
    print(f"Taxi {taxi_id} comenzando viaje hacia origen {origen}...")
    mover_taxi(origen)
    print(f"Taxi {taxi_id} ha llegado a la posición de origen {origen}.")

    # Notificar a la central que el cliente ha sido recogido
    notify_pickup(cliente_id)

    print(f"Taxi {taxi_id} comenzando viaje hacia el destino {destino}...")
    mover_taxi(destino)
    print(f"Taxi {taxi_id} ha llegado al destino {destino}.")

    # Notificar a la central que el viaje ha sido completado
    notify_trip_completion(cliente_id, destino)

    # Regresar a la posición inicial
    print(f"Taxi {taxi_id} regresando a la posición inicial (0, 0)...")
    mover_taxi([0, 0])

    availability_status = "AVAILABLE"
    enviar_estado()

def notify_pickup(cliente_id):
    message = {
        "customer_id": cliente_id,
        "taxi_id": taxi_id,
        "status": "PICKED_UP",
        "mensaje": f"Taxi {taxi_id} ha recogido al cliente {cliente_id}."
    }
    try:
        producer.send('trip_status', value=message)
        producer.flush()
        print(f"Notificado a central que el cliente {cliente_id} ha sido recogido.")
    except Exception as e:
        print(f"Error al notificar recogida del cliente: {e}")


def notify_trip_completion(cliente_id, destination):
    message = {
        "customer_id": cliente_id,
        "taxi_id": taxi_id,
        "status": "TRIP_COMPLETED",
        "mensaje": f"Taxi {taxi_id} ha completado el viaje para el cliente {cliente_id}.",
        "destination": destination
    }
    try:
        producer.send('trip_status', value=message)
        producer.flush()
        print(f"Notificado a central que el viaje para el cliente {cliente_id} ha sido completado.")
    except Exception as e:
        print(f"Error al notificar finalización de viaje: {e}")

def recibir_datos_sensor():
    global movement_event
    previous_status = None
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        print("Intentando vincular el sensor:")
        print(f"IP: {sensor_ip}, Puerto: {sensor_port}")
        
        try:
            s.bind(('0.0.0.0', sensor_port))
            s.listen(1)
            print(f"Taxi está escuchando conexiones del sensor en el puerto {sensor_port}...")
            while True:
                conn, addr = s.accept()
                with conn:
                    print(f"Conexión establecida con el sensor: {addr}")
                    while True:
                        data = conn.recv(1024).decode().strip()
                        if not data:
                            break
                        
                        if previous_status is None or data != previous_status:
                            print(f"Datos recibidos del sensor: {data}")
                            previous_status = data

                        if data in ["KO", "r", "p"]:
                            if movement_event.is_set():
                                movement_event.clear()
                                enviar_estado()
                                print(f"Estado de movimiento cambiado a STOP y enviado a la central.")
                        elif data == "o":
                            if not movement_event.is_set():
                                movement_event.set()
                                enviar_estado()
                                print(f"Estado de movimiento cambiado a RUN y enviado a la central.")
        
        except Exception as e:
            print(f"Error al vincular el sensor en {sensor_ip}:{sensor_port}: {e}")

def start_taxi():
    autenticar_taxi_con_central()
    threading.Thread(target=recibir_datos_sensor, daemon=True).start()
    threading.Thread(target=recibir_solicitudes_kafka, daemon=True).start()
    threading.Thread(target=procesar_solicitudes, daemon=True).start()

    while True:
        time.sleep(1)

# MAIN
if len(sys.argv) != 8:
    print(f"Uso: python EC_DE.py <taxi_id> <central_ip> <central_port> <broker_ip> <broker_port> <sensor_ip> <sensor_port>")
    sys.exit(1)

taxi_id = sys.argv[1]
central_ip = sys.argv[2]
central_port = int(sys.argv[3])
broker_ip = sys.argv[4]
broker_port = int(sys.argv[5])
sensor_ip = sys.argv[6]
sensor_port = int(sys.argv[7])

setup_kafka(broker_ip, broker_port)
start_taxi()

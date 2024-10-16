import socket
import threading
import sys
import time
from kafka import KafkaProducer, KafkaConsumer
import json

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

# Configuración de Kafka
def setup_kafka(broker_ip, broker_port):
    """Configura el productor y consumidor de Kafka"""
    global producer, consumer
    producer = KafkaProducer(
        bootstrap_servers=f'{broker_ip}:{broker_port}',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
  
    consumer = KafkaConsumer(
        'taxi_requests',
        bootstrap_servers=f'{broker_ip}:{broker_port}',
        auto_offset_reset='earliest',
        group_id=f'taxi_group_{taxi_id}',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )

def autenticar_taxi_con_central():
    """Autentica el taxi con la central usando sockets."""
    global authenticated
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((central_ip, central_port))
            mensaje_autenticacion = f"{taxi_id}#INIT"
            s.send(mensaje_autenticacion.encode())

            # Esperar la respuesta de la central
            response = s.recv(1024).decode()
            print(f"Respuesta de la central: {response}")
            if "OK" in response:
                authenticated = True
                print(f"Taxi {taxi_id} autenticado con éxito.")
            else:
                print(f"Error en la autenticación del taxi {taxi_id}.")
                
            # Mantener la conexión abierta si es necesario
            while True:
                time.sleep(1)  # Mantener la conexión activa para futuros mensajes

    except Exception as e:
        print(f"Error durante la autenticación: {e}")

def recibir_solicitudes_kafka():
    """Escucha solicitudes de servicio desde la central mediante Kafka."""
    if authenticated:
        print(f"Taxi {taxi_id} esperando solicitudes de servicio vía Kafka...")
        for mensaje in consumer:
            solicitud = mensaje.value
            if solicitud.get("taxi_id") == taxi_id:
                print(f"Solicitud recibida: {solicitud}")
                manejar_solicitud(solicitud)
    else:
        print("El taxi no está autenticado. No se pueden recibir solicitudes.")

def manejar_solicitud(solicitud):
    """Procesa una solicitud de servicio recibida a través de Kafka."""
    if solicitud.get("mensaje") == "NUEVO_SERVICIO":
        destino = solicitud.get("destino")
        print(f"Taxi {taxi_id} recibió solicitud para llevar al destino {destino}")
        # Simula el movimiento hacia el destino
        realizar_viaje(destino)

def realizar_viaje(destino):
    """Simula el viaje hacia el destino recibido."""
    print(f"Taxi {taxi_id} comenzando viaje hacia {destino}...")
    time.sleep(5)  # Simula tiempo de viaje
    print(f"Taxi {taxi_id} ha llegado al destino {destino}.")
    enviar_estado("END", destino)

def enviar_estado(estado, destino=None):
    """Envia el estado actual del taxi a la central usando Kafka."""
    message = {
        "taxi_id": taxi_id,
        "estado": estado,
        "destino": destino
    }
    producer.send('taxi_status', value=message)
    producer.flush()
    print(f"Taxi {taxi_id} ha enviado estado: {estado}")

def recibir_datos_sensor():
    """Recibe los datos del sensor EC_S a través de sockets."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((sensor_ip, sensor_port))
        s.listen(1)
        print(f"Esperando conexión de los sensores en {sensor_ip}:{sensor_port}...")
        conn, addr = s.accept()
        with conn:
            print(f"Conexión establecida con el sensor: {addr}")
            while True:
                data = conn.recv(1024).decode()
                if not data:
                    break
                print(f"Datos recibidos del sensor: {data}")
                if data == "KO":
                    print(f"Taxi {taxi_id} detenido por contingencia del sensor.")
                    enviar_estado("STOP")
                elif data == "OK":
                    print(f"Taxi {taxi_id} reanudando viaje.")
                    enviar_estado("RUN")

def start_taxi():
    """Función principal para iniciar el taxi."""
    # Primero autenticar el taxi con la central
    autenticar_taxi_con_central()

    # Iniciar hilo para recibir datos del sensor
    sensor_thread = threading.Thread(target=recibir_datos_sensor)
    sensor_thread.start()

    # Luego escuchar solicitudes de servicio a través de Kafka
    if authenticated:
        recibir_solicitudes_kafka()

# MAIN
if len(sys.argv) != 8:
    print(f"Uso: python EC_DE.py <taxi_id> <central_ip> <central_port> <broker_ip> <broker_port> <sensor_ip> <sensor_port>")
    sys.exit(1)

# Leer argumentos de la línea de comandos
taxi_id = sys.argv[1]
central_ip = sys.argv[2]
central_port = int(sys.argv[3])
broker_ip = sys.argv[4]
broker_port = int(sys.argv[5])
sensor_ip = sys.argv[6]
sensor_port = int(sys.argv[7])


# Configurar Kafka (IP y puerto del broker)
setup_kafka(broker_ip, broker_port)

# Iniciar el taxi
start_taxi()

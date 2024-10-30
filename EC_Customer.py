from kafka import KafkaProducer, KafkaConsumer
import json
import sys
import time

# Variables globales
broker_ip = ""
broker_port = 0
customer_id = ""
request_file = ""  # Se definirá dinámicamente para cada cliente
requests = []  # Lista para almacenar las solicitudes leídas del archivo
current_index = 0  # Mantener el seguimiento de la solicitud actual
CLIENTES_DB_FILE = 'clientes_db.json'

# Configuración de Kafka
producer = None
consumer = None

def setup_kafka():
    global producer, consumer
    try:
        # Configuración del productor
        producer = KafkaProducer(
            bootstrap_servers=f'{broker_ip}:{broker_port}',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # Configuración del consumidor
        consumer = KafkaConsumer(
            f'respuesta_cliente_{customer_id}',
            bootstrap_servers=f'{broker_ip}:{broker_port}',
            auto_offset_reset='latest',
            group_id=f'customer_group_{customer_id}',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        print(f"Cliente {customer_id} conectado a Kafka correctamente.")
    except Exception as e:
        print(f"Error al configurar Kafka en el cliente {customer_id}: {e}")



def cliente_existe(cliente_id):
    """Verificar si el cliente existe en la base de datos."""
    try:
        with open(CLIENTES_DB_FILE, 'r') as file:
            clientes = json.load(file)
            return str(cliente_id) in clientes
    except FileNotFoundError:
        print("Error: La base de datos de clientes no se encontró.")
        return False

def read_requests_from_json():
    """Leer las solicitudes de servicio desde el archivo EC_Request.json del cliente."""
    global requests
    try:
        with open(request_file, 'r') as file:
            data = json.load(file)
            if "Requests" in data:
                requests = [request["Id"] for request in data["Requests"].values()]
                print(f"Solicitudes cargadas desde {request_file}: {requests}")
            else:
                print(f"Formato incorrecto en {request_file}. No se encontró la clave 'Requests'.")
                sys.exit(1)
    except FileNotFoundError:
        print(f"Error: El archivo {request_file} no se encontró.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: No se pudo decodificar el archivo JSON {request_file}. Verifique el formato.")
        sys.exit(1)

def request_service(destination_id):
    """Enviar solicitud de servicio a la central mediante Kafka."""
    print(f"Enviando solicitud de servicio con destino: {destination_id}")
    message = {
        "type": "customer_request",
        "customer_id": customer_id,
        "destination_id": destination_id
    }
    producer.send('solicitudes_taxi', value=message)
    producer.flush()

def wait_for_response():
    taxi_assigned = False
    for message in consumer:
        data = message.value
        if data.get("status") == "OK" and not taxi_assigned:
            print(f"Respuesta recibida de la central: {data}")
            print("Taxi asignado. Esperando finalización del viaje...")
            taxi_assigned = True
        elif data.get("status") == "TRIP_COMPLETED" and taxi_assigned:
            print(f"Respuesta recibida de la central: {data}")
            print("Viaje completado.")
            return True
        elif data.get("status") == "NO_TAXI_AVAILABLE" and not taxi_assigned:
            print(f"Respuesta recibida de la central: {data}")
            print("No hay taxis disponibles.")
            return False
        # Otras respuestas pueden ser manejadas aquí si es necesario

          



def run_customer():
    global current_index
    while current_index < len(requests):
        destination_id = requests[current_index]
        request_service(destination_id)
        if wait_for_response():
            print("Viaje completado.")
        else:
            print("Solicitud fallida.")
        current_index += 1
        print("Esperando 4 segundos antes de solicitar el siguiente destino...")
        time.sleep(4)



# MAIN
if len(sys.argv) != 4:
    print(f"Uso: python EC_Customer.py <broker_ip> <broker_port> <customer_id>")
    sys.exit(1)

# Leer argumentos de la línea de comandos
broker_ip = sys.argv[1]
broker_port = int(sys.argv[2])
customer_id = sys.argv[3]

# Verificar si el cliente existe en la base de datos
if not cliente_existe(customer_id):
    print(f"El cliente con ID {customer_id} no existe en la base de datos.")
    sys.exit(1)

# Definir el archivo de solicitud para este cliente
request_file = f"EC_Request_{customer_id}.json"

# Configurar Kafka
setup_kafka()

# Leer solicitudes desde el archivo JSON
read_requests_from_json()

print(f"Iniciando cliente con ID {customer_id}")
run_customer()

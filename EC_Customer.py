from kafka import KafkaProducer, KafkaConsumer
import json
import sys
import time


# Variables globales
broker_ip = ""
broker_port = 0
customer_id = ""
destination_file = ""
current_line = 0  # Mantener el seguimiento de la línea actual en el archivo
customer_coords = [1, 1]  # Coordenadas iniciales del cliente


# Configuración de Kafka
producer = None
consumer = None


def setup_kafka():
   """Configurar el productor y consumidor de Kafka."""
   global producer, consumer


   # Configuración del productor
   producer = KafkaProducer(
       bootstrap_servers=f'{broker_ip}:{broker_port}',
       value_serializer=lambda v: json.dumps(v).encode('utf-8')
   )


   # Configuración del consumidor
   consumer = KafkaConsumer(
       'respuestas_central',
       bootstrap_servers=f'{broker_ip}:{broker_port}',
       auto_offset_reset='earliest',
       group_id=f'customer_group_{customer_id}',
       value_deserializer=lambda v: json.loads(v.decode('utf-8'))
   )


def read_next_destination():
   """Leer el siguiente destino desde el archivo a partir de la línea actual."""
   global current_line
   try:
       with open(destination_file, 'r') as file:
           lines = file.readlines()
           if current_line < len(lines):
               destination = lines[current_line].strip()
               print(f"Destino leído de la línea {current_line + 1}: {destination}")
               current_line += 1
               return destination
           else:
               return None
   except FileNotFoundError:
       print(f"Error: El archivo {destination_file} no se encontró.")
       sys.exit(1)


def request_service(destination):
   """Enviar solicitud de servicio a la central mediante Kafka."""
   print(f"Enviando solicitud de servicio con destino: {destination} y posición: {customer_coords}")
   message = {
       "type": "customer_request",
       "customer_id": customer_id,
       "customer_coords": customer_coords,
       "destination": destination
   }
   producer.send('solicitudes_taxi', value=message)
   producer.flush()


def wait_for_response():
   """Esperar por una respuesta de la central sobre la solicitud del cliente."""
   for message in consumer:
       data = message.value
       if data.get("customer_id") == customer_id:
           print(f"Respuesta recibida de la central: {data}")
           if data.get("status") == "OK":
               print("Taxi asignado.")
               return True
           else:
               print("No hay taxis disponibles.")
               return False
   return False


def run_customer():
   """Ejecuta el ciclo de solicitud de destinos."""
   while True:
       destination = read_next_destination()
       if destination is None:
           print("No hay más destinos. Finalizando cliente.")
           break


       request_service(destination)
       if wait_for_response():
           print("Viaje completado.")
       else:
           print("Solicitud fallida.")


       print("Esperando 4 segundos antes de solicitar el siguiente destino...")
       time.sleep(4)


# MAIN
if len(sys.argv) != 7:
   print(f"Uso: python EC_Customer.py <broker_ip> <broker_port> <customer_id> <coord_x> <coord_y> <destination_file>")
   sys.exit(1)


# Leer argumentos de la línea de comandos
broker_ip = sys.argv[1]
broker_port = int(sys.argv[2])
customer_id = sys.argv[3]
customer_coords = [int(sys.argv[4]), int(sys.argv[5])]
destination_file = sys.argv[6]


# Configurar Kafka
setup_kafka()


print(f"Iniciando cliente con ID {customer_id}, coordenadas {customer_coords} y destinos en archivo {destination_file}")
run_customer()

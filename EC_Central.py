import socket
import threading
import queue
import sys
from kafka import KafkaProducer, KafkaConsumer
import json
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import os  # Importar os para operaciones de archivo

LOCATIONS_DB_FILE = 'EC_Locations.json' 
producer = None  # Productor de Kafka para enviar respuestas
TAXIS_DB_FILE = 'taxis_db.json'
mapa_queue = queue.Queue()
CLIENTES_DB_FILE = 'clientes_db.json'

# Definir los locks
taxis_db_lock = threading.Lock()
clientes_db_lock = threading.Lock()  # Si es necesario para clientes
# Función para cargar la base de datos de clientes
def cargar_clientes_db():
    with clientes_db_lock:
        try:
            with open(CLIENTES_DB_FILE, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            return {}

def guardar_clientes_db(clientes):
    with clientes_db_lock:
        temp_file = CLIENTES_DB_FILE + '.tmp'
        with open(temp_file, 'w') as file:
            json.dump(clientes, file, indent=4)
        os.replace(temp_file, CLIENTES_DB_FILE)

def agregar_o_actualizar_cliente(cliente_id, localizacion=[0, 0], recogido=False, destination=None):
    clientes = cargar_clientes_db()
    cliente_id = str(cliente_id)  # Asegurarse de que el ID sea una cadena para las claves del JSON
    if cliente_id in clientes:
        clientes[cliente_id]['localizacion'] = localizacion
        clientes[cliente_id]['recogido'] = recogido
        if destination:
            clientes[cliente_id]['destination'] = destination
    else:
        nuevo_cliente = {
            "localizacion": localizacion,
            "recogido": recogido,
            "destination": destination
        }
        clientes[cliente_id] = nuevo_cliente
    guardar_clientes_db(clientes)

def obtener_cliente_por_id(cliente_id):
    clientes = cargar_clientes_db()
    return clientes.get(str(cliente_id), None)


def cargar_locations_db():
    try:
        with open(LOCATIONS_DB_FILE, 'r') as file:
            return json.load(file).get("locations", [])
    except FileNotFoundError:
        return []

def obtener_coordenadas_por_id(destino_id):
    locations = cargar_locations_db()
    for location in locations:
        if location.get("Id") == destino_id:
            pos = location.get("POS")
            # Convertir "x,y" a una lista [x, y]
            return [int(coord) for coord in pos.split(',')]
    return None

# Ejemplo de cómo utilizar estas funciones en el contexto actual
def actualizar_estado_cliente(cliente_id, recogido, destination=None, localizacion=None):
    clientes = cargar_clientes_db()
    if str(cliente_id) in clientes:
        clientes[str(cliente_id)]['recogido'] = recogido
        if destination is not None:
            clientes[str(cliente_id)]['destination'] = destination
        if localizacion is not None:
            clientes[str(cliente_id)]['localizacion'] = localizacion
        guardar_clientes_db(clientes)


        
# Configuración del productor de Kafka
def setup_kafka(broker_ip, broker_port):
    global producer
    producer = KafkaProducer(
        bootstrap_servers=f'{broker_ip}:{broker_port}',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=5,  # Reintentos para mejorar fiabilidad
        acks='all'  # Confirmar que todos los brokers recibieron el mensaje
    )

# Funciones para la Base de Datos JSON
def cargar_taxis_db():
    with taxis_db_lock:
        try:
            with open(TAXIS_DB_FILE, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            return []

def guardar_taxis_db(taxis):
    with taxis_db_lock:
        temp_file = TAXIS_DB_FILE + '.tmp'
        with open(temp_file, 'w') as file:
            json.dump(taxis, file, indent=4)
        os.replace(temp_file, TAXIS_DB_FILE)


def obtener_taxi_por_id(taxi_id):
    taxis = cargar_taxis_db()
    taxi_id = int(taxi_id)  # Convertir siempre el ID a entero
    for taxi in taxis:
        if int(taxi['id']) == taxi_id:
            return taxi
    return None

def agregar_o_actualizar_taxi(taxi_id, availability="AVAILABLE", movement="RUN", localizacion=[0, 0]):
    taxis = cargar_taxis_db()
    taxi_id = int(taxi_id)
    taxi = obtener_taxi_por_id(taxi_id)
    if taxi:
        taxi['availability'] = availability
        taxi['movement'] = movement
        taxi['localizacion'] = localizacion
    else:
        nuevo_taxi = {
            "id": taxi_id,
            "availability": availability,
            "movement": movement,
            "localizacion": localizacion
        }
        taxis.append(nuevo_taxi)
    guardar_taxis_db(taxis)

def obtener_taxi_disponible():
    taxis = cargar_taxis_db()
    for taxi in taxis:
        if taxi['availability'] == "AVAILABLE":
            return taxi
    return None

def actualizar_estado_y_posicion_taxi(taxi_id, availability=None, movement=None, localizacion=None):
    taxis = cargar_taxis_db()
    taxi_id = int(taxi_id)
    for taxi in taxis:
        if taxi['id'] == taxi_id:
            if availability:
                taxi['availability'] = availability
            if movement:
                taxi['movement'] = movement
            if localizacion:
                taxi['localizacion'] = localizacion
            break
    guardar_taxis_db(taxis)

# Función para inicializar el mapa vacío de 20x20
def mostrar_mapa_grafico():
    plt.ion()  # Enable interactive mode
    fig, ax = plt.subplots(figsize=(10, 10))  # Adjusted size for better visualization
    ax.set_xlim(0, 20)
    ax.set_ylim(0, 20)
    ax.invert_yaxis()

    # Draw the initial grid
    for x in range(20):
        for y in range(20):
            ax.add_patch(patches.Rectangle((x, y), 1, 1, fill=True, facecolor='white', edgecolor='black'))

    def actualizar_mapa():
            ax.clear()
            ax.set_xlim(0, 20)
            ax.set_ylim(0, 20)
            ax.invert_yaxis()

            # Draw the grid cells
            for x in range(20):
                for y in range(20):
                    ax.add_patch(patches.Rectangle((x, y), 1, 1, fill=True, facecolor='white', edgecolor='black'))

            # **Add this section to display all destinations from EC_Locations.json**
            locations = cargar_locations_db()
            for location in locations:
                dest_id = location.get("Id")
                pos = location.get("POS")
                x_str, y_str = pos.split(',')
                x = int(x_str) % 20
                y = int(y_str) % 20
                ax.add_patch(patches.Rectangle((x, y), 1, 1, fill=True, facecolor='blue', edgecolor='black'))
                ax.text(x + 0.5, y + 0.5, str(dest_id), ha='center', va='center', fontsize=12, color='white', weight='bold')

            # Display clients on the map if they have not been picked up yet
            clientes = cargar_clientes_db()
            for cliente_id, cliente in clientes.items():
                if cliente['localizacion'] is not None and not cliente.get('recogido', False):
                    x, y = cliente['localizacion']
                    x = x % 20
                    y = y % 20
                    ax.add_patch(patches.Rectangle((x, y), 1, 1, fill=True, facecolor='yellow', edgecolor='black'))
                    ax.text(x + 0.5, y + 0.5, str(cliente_id).lower(), ha='center', va='center', fontsize=12, color='black', weight='bold')


                # Optionally, remove this section if destinations are already displayed
                # if 'destination' in cliente and cliente['destination'] is not None:
                #     dest_x, dest_y = cliente['destination']
                #     dest_x = dest_x % 20
                #     dest_y = dest_y % 20
                #     ax.add_patch(patches.Rectangle((dest_x, dest_y), 1, 1, fill=True, facecolor='blue', edgecolor='black'))
                #     ax.text(dest_x + 0.5, dest_y + 0.5, str(cliente_id).upper(), ha='center', va='center', fontsize=12, color='white', weight='bold')

            # Display taxis on the map
            taxis = cargar_taxis_db()
            for taxi in taxis:
                if taxi['localizacion'] is not None:
                    x, y = taxi['localizacion']
                    x = x % 20
                    y = y % 20
                    # Determine color based on taxi movement status
                    color = 'green' if taxi['availability'] == "AVAILABLE" else 'red'
                    ax.add_patch(patches.Rectangle((x, y), 1, 1, fill=True, facecolor=color, edgecolor='black'))
                    ax.text(x + 0.5, y + 0.5, str(taxi['id']), ha='center', va='center', fontsize=12, color='white', weight='bold')

            plt.draw()


    # Update loop based on queue
    while True:
        if not mapa_queue.empty():
            mapa_queue.get()
            actualizar_mapa()
        plt.pause(0.1)


def agregar_o_actualizar_taxi(taxi_id, availability="BUSY", movement="RUN", localizacion=[0, 0]):
    """
    Agregar o actualizar un taxi en la base de datos.
    Los taxis inicialmente estarán en estado "BUSY".
    """
    taxis = cargar_taxis_db()
    taxi_id = int(taxi_id)
    taxi = obtener_taxi_por_id(taxi_id)
    if taxi:
        taxi['availability'] = availability
        taxi['movement'] = movement
        taxi['localizacion'] = localizacion
    else:
        nuevo_taxi = {
            "id": taxi_id,
            "availability": availability,
            "movement": movement,
            "localizacion": localizacion
        }
        taxis.append(nuevo_taxi)
    guardar_taxis_db(taxis)

def handle_taxi_auth(conn, addr):
    try:
        message = conn.recv(1024).decode().strip()
        if not message:
            print(f"Conexión cerrada por el taxi desde {addr} o mensaje vacío.")
            return
        
        print(f"Mensaje recibido para autenticación: '{message}'")
        
        try:
            # Parsear el mensaje JSON recibido
            taxi_data = json.loads(message)
            taxi_id = int(taxi_data["taxi_id"])  # Asegurarse de que el ID sea int
            position = taxi_data["position"]
            availability = taxi_data["availability"]
            movement = taxi_data["movement"]

            # Verificar si el taxi existe en la base de datos
            taxi = obtener_taxi_por_id(taxi_id)
            if taxi:
                # Taxi existe, actualizar sus detalles y permitir la autenticación
                agregar_o_actualizar_taxi(taxi_id, availability="AVAILABLE", movement=movement, localizacion=position)
                response = "OK#Taxi autenticado y listo"
                print(f"Taxi {taxi_id} autenticado correctamente desde {addr}.")
                conn.send(response.encode())
            else:
                # Rechazar autenticación si el taxi no existe en la base de datos
                response = "KO#Taxi no registrado en la base de datos"
                print(f"Taxi {taxi_id} no registrado en la base de datos. Conexión rechazada.")
                conn.send(response.encode())

            mapa_queue.put("update")

        except json.JSONDecodeError as e:
            print(f"Error al procesar el mensaje recibido del taxi desde {addr}: {e}")
            conn.send("KO#Error en autenticación".encode())
        except Exception as e:
            print(f"Error desconocido al procesar el mensaje del taxi desde {addr}: {e}")
            conn.send("KO#Error en autenticación".encode())

    except Exception as e:
        print(f"Error en la conexión con el taxi {addr}: {e}")
    finally:
        conn.close()

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

def manejar_solicitud_taxi(solicitud):
    taxi_id = int(solicitud.get("taxi_id"))
    availability = solicitud.get("availability")
    movement = solicitud.get("movement")
    localizacion = solicitud.get("localizacion")

    taxi = obtener_taxi_por_id(taxi_id)
    print(f"obtenido taxi{taxi}")
    if taxi:
        # Actualizar los parámetros si han cambiado
        actualizar_estado_y_posicion_taxi(
            taxi_id, 
            availability=availability, 
            movement=movement, 
            localizacion=localizacion  # Actualizar la posición también
        )
        
        # Mostrar mensajes en función de los parámetros actualizados
        if availability:
            print(f"Taxi {taxi_id} actualizado: Disponibilidad -> {availability}")
        if movement:
            print(f"Taxi {taxi_id} actualizado: Movimiento -> {movement}")
        if localizacion:
            print(f"Taxi {taxi_id} actualizado: Posición -> {localizacion}")

        mapa_queue.put("update")


def recibir_solicitudes_taxis():
    consumer = KafkaConsumer(
        'taxi_status',
        bootstrap_servers=f'{broker_ip}:{broker_port}',
        auto_offset_reset='earliest',
        group_id='central_taxi_status_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("Central escuchando estados de taxis vía Kafka...")
    for mensaje in consumer:
        solicitud = mensaje.value
        print(f"solicitud taxi {solicitud}")
        manejar_solicitud_taxi(solicitud)


# Comunicación con Clientes
def procesar_solicitudes_clientes():
    consumer = KafkaConsumer(
        'solicitudes_taxi',
        bootstrap_servers=f'{broker_ip}:{broker_port}',
        auto_offset_reset='earliest',
        group_id='central_solicitudes_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("Central escuchando solicitudes de clientes...")
    for mensaje in consumer:
        solicitud = mensaje.value
        procesar_solicitud_cliente(solicitud)


def procesar_solicitud_cliente(solicitud):
    cliente_id = solicitud.get("customer_id")
    destino_id = solicitud.get("destination_id")  # Cambiado a destination_id recibido desde EC_Customer
    print(f"Solicitud recibida de cliente {cliente_id} para destino {destino_id}")
    
    # Obtener la información del cliente desde la base de datos
    cliente_info = obtener_cliente_por_id(cliente_id)
    if not cliente_info:
        print(f"No se encontró información para el cliente {cliente_id}")
        return

    origen = cliente_info.get("localizacion")
    
    # Buscar las coordenadas del destino usando el ID de destino recibido
    destino_coordenadas = obtener_coordenadas_por_id(destino_id)
    
    # Validar que el destino tenga coordenadas válidas
    if destino_coordenadas is None:
        print(f"No se encontraron coordenadas para el destino {destino_id}")
        return

    taxi = obtener_taxi_disponible()
    if taxi:
        taxi_id = taxi['id']
        print(f"Taxi {taxi_id} asignado a cliente {cliente_id}")
        response = {
            "customer_id": cliente_id,
            "taxi_id": taxi_id,
            "status": "OK",
            "mensaje": f"Taxi {taxi_id} ha sido asignado y se dirige a su ubicación."
        }
        actualizar_estado_y_posicion_taxi(taxi_id, availability="BUSY")  # Actualizar estado del taxi
        enviar_solicitud_taxi(taxi_id, cliente_id, origen, destino_coordenadas)
        enviar_notificacion_cliente(response)  # Enviar respuesta al cliente
    else:
        print(f"No hay taxis disponibles para el cliente {cliente_id}")
        response = {
            "customer_id": cliente_id,
            "status": "NO_TAXI_AVAILABLE",
            "mensaje": "Lo sentimos, no hay taxis disponibles en este momento."
        }
        enviar_notificacion_cliente(response)  # Enviar respuesta al cliente

    # No es necesario llamar a producer.flush() aquí, se maneja dentro de enviar_notificacion_cliente()




def enviar_solicitud_taxi(taxi_id,cliente_id, origen, destino):
    """
    Envía una solicitud a un taxi específico para que recoja a un cliente y lo lleve al destino.
    
    Args:
    - taxi_id (int): ID del taxi al que se le enviará la solicitud.
    - origen (list): Coordenadas de origen del cliente.
    - destino (list): Coordenadas de destino del cliente.
    """
    solicitud = {
    "taxi_id": taxi_id,
    "customer_id": cliente_id,  # Include customer_id
    "mensaje": "NUEVO_SERVICIO",
    "origen": origen,
    "destino": destino
    }
    try:
        producer.send('taxi_requests', value=solicitud)
        producer.flush()
        print(f"Solicitud de servicio enviada a taxi {taxi_id} para origen {origen} y destino {destino}")
    except Exception as e:
        print(f"Error al enviar solicitud de servicio a taxi {taxi_id}: {e}")


def recibir_notificaciones_viajes():
    consumer = KafkaConsumer(
        'trip_status',
        bootstrap_servers=f'{broker_ip}:{broker_port}',
        auto_offset_reset='earliest',
        group_id='central_trip_status_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("Central escuchando notificaciones de viajes...")
    for mensaje in consumer:
        data = mensaje.value
        if data.get("status") == "TRIP_COMPLETED":
            print(f"Notificación de viaje completado recibida: {data}")
            # Actualizar la posición y estado del cliente
            actualizar_estado_cliente(
                cliente_id=data['customer_id'],
                recogido=False,
                localizacion=data['destination']
            )
            enviar_notificacion_cliente(data)
            mapa_queue.put("update")
        elif data.get("status") == "PICKED_UP":
            print(f"Notificación de cliente recogido recibida: {data}")
            # Establecer 'recogido' en True para el cliente
            actualizar_estado_cliente(
                cliente_id=data['customer_id'],
                recogido=True
            )
            mapa_queue.put("update")




def enviar_notificacion_cliente(data):
    try:
        customer_id = data['customer_id']
        topic = f'respuesta_cliente_{customer_id}'  # Tópico específico para el cliente
        producer.send(topic, value=data)
        producer.flush()
        print(f"Notificación enviada al cliente {customer_id} en el tópico {topic}.")
    except Exception as e:
        print(f"Error al enviar notificación al cliente: {e}")



# MAIN
if len(sys.argv) != 4:
    print("Uso: python EC_Central.py <broker_ip> <broker_port> <central_port>")
    sys.exit(1)

broker_ip = sys.argv[1]
broker_port = int(sys.argv[2])
central_port = int(sys.argv[3])

setup_kafka(broker_ip, broker_port)

# Iniciar hilos para autenticación, recepción de mensajes de taxis y solicitudes de clientes
threading.Thread(target=start_taxi_auth_server, args=(broker_ip, central_port)).start()
threading.Thread(target=recibir_solicitudes_taxis).start()
threading.Thread(target=procesar_solicitudes_clientes).start()
threading.Thread(target=recibir_notificaciones_viajes).start()

# Ejecutar la lógica del mapa en el hilo principal
mostrar_mapa_grafico()

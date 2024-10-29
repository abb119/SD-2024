https://docs.google.com/document/d/1SXLJ0PQoAh2w41IpMO_zFeMtKnACnGlYBtX5Xk3pOwc/edit?usp=sharing
PASO 1:
bin/zookeeper-server-start.sh config/zookeeper.properties
PASO 2:
bin/kafka-server-start.sh config/server.properties
PASO 3 TOPICOS(En central hay que cambiar localhost por la ip de central):
bin/kafka-topics.sh --create --topic solicitudes_taxi --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic respuestas_central --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic taxi_requests --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic taxi_status --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
PASOS DE LIBRERIA KAFKA:
python3 -m venv venv
source venv/bin/activate
python -m pip install kafka-python==2.0.0
cambiar en env\Lib\site-packages\kafka\codec.py:
from kafka.vendor import six
from six.moves import range



Ahora tengo el mismo problema que hemos solucionado antes. cuando el sensor del taxi detecta un peaton o un semaforo se lo evia a taxi y taxi le envia a central el estado de taxi stop, luego central cambia el estado del taxi a stop en la base de datos. Cuando sensor vuelve a ok, taxi vuelve a ok y le manda a central el estado de taxi run para que se lo cambie en la base datos (cuando el taxi se mueve su estado es run). Arregla esto porque a central no llega nada.


alumno@clLLIBRE1-17:~/Escritorio/SD-2024$ python3 EC_DE.py 1 172.21.42.10 8087 172.21.42.10 9092 172.21.42.11 8089
Respuesta de la central: OK#Taxi autenticado y listo
Taxi 1 ha enviado estado: AVAILABLE, RUN
Taxi 1 autenticado con éxito y ahora está disponible.
Taxi 1 esperando solicitudes de servicio vía Kafka...
Intentando vincular el sensor:
IP: 172.21.42.11, Puerto: 8089
Taxi está escuchando conexiones del sensor en el puerto 8089...
Debug: Solicitud recibida desde Kafka: {'taxi_id': 1, 'mensaje': 'NUEVO_SERVICIO', 'origen': [5, 5], 'destino': [5, 3]}

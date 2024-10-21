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

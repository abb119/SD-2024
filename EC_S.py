# EC_S.py (Sensor)
import socket
import time
import random

def simulate_sensor():
    """Simula el estado del sensor: 'o' (OK), 'r' (semáforo rojo), 'p' (peatón cruzando)."""
    return random.choice(['o', 'r', 'p'])

def connect_to_engine(engine_ip, engine_port):
    sensor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sensor.connect((engine_ip, engine_port))
    
    try:
        while True:
            # Simular el estado del sensor
            sensor_status = simulate_sensor()
            sensor.send(sensor_status.encode())
            print(f"Sensor envía estado: {sensor_status}")
            time.sleep(2)  # Enviar el estado cada 2 segundos
    except Exception as e:
        print(f"Error en el sensor: {e}")
    finally:
        sensor.close()

if __name__ == "__main__":
    engine_ip = '172.21.243.238'  # Cambia por la IP de EC_DE
    engine_port = 8087  # Puerto donde EC_DE estará escuchando
    connect_to_engine(engine_ip, engine_port)

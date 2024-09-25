import socket
import time
import threading

# Global variable to store the sensor status
current_status = 'o'  # Always starts with 'o' (OK)

def simulate_sensor():
    """Simula el estado del sensor, pero por defecto es 'o' (OK) a menos que se cambie manualmente."""
    global current_status
    while True:
        time.sleep(2)  # Simulate sending every 2 seconds
        yield current_status

def change_sensor_status():
    """Permite introducir manualmente el estado del sensor a través del terminal."""
    global current_status
    while True:
        new_status = input("Introduce nuevo estado del sensor ('o' para OK, 'r' para semáforo rojo, 'p' para peatón): ").strip()
        if new_status in ['o', 'r', 'p']:
            current_status = new_status
            print(f"Estado del sensor cambiado a: {current_status}")
        else:
            print("Estado no válido. Introduce 'o', 'r' o 'p'.")

def connect_to_engine(engine_ip, engine_port):
    sensor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sensor.connect((engine_ip, engine_port))
    
    try:
        sensor_gen = simulate_sensor()  # Generator for sensor status
        while True:
            # Get the next sensor status from the generator (default 'o' unless changed)
            sensor_status = next(sensor_gen)
            sensor.send(sensor_status.encode())
            print(f"Sensor envía estado: {sensor_status}")
    except Exception as e:
        print(f"Error en el sensor: {e}")
    finally:
        sensor.close()

if __name__ == "__main__":
    engine_ip = '172.21.243.240'  # IP del Digital Engine
    engine_port = 8087  # Puerto debe coincidir con el del EC_DE

    # Create a separate thread to allow manual status input
    input_thread = threading.Thread(target=change_sensor_status)
    input_thread.daemon = True  # Daemonize the input thread so it closes with the main program
    input_thread.start()

    # Start the connection and sensor simulation
    connect_to_engine(engine_ip, engine_port)

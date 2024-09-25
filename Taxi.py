# Taxi.py
import socket
import time

def connect_to_central(taxi_id):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(('localhost', 8080))

    # Iniciar el taxi enviando ID y acción INIT
    init_message = f"{taxi_id}#INIT"
    client.send(init_message.encode())
    
    # Recibir respuesta de la central
    response = client.recv(1024).decode()
    print(f"Respuesta de la central: {response}")

    if "OK" in response:
        # Simular el movimiento del taxi
        for i in range(1, 6):
            status_message = f"{taxi_id}#STATUS#{i},{i}"
            client.send(status_message.encode())
            response = client.recv(1024).decode()
            print(f"Respuesta de la central: {response}")
            time.sleep(2)
    
    client.close()

if __name__ == "__main__":
    taxi_id = 1  # ID del taxi
    connect_to_central(taxi_id)

# Central.py
import socket
import threading

# Base de datos de taxis
taxi_db = {1: "Taxi1", 2: "Taxi2", 3: "Taxi3"}

def handle_taxi(conn, addr):
    print(f"Conexión recibida de {addr}")
    try:
        while True:
            # Recibe el mensaje del taxi
            message = conn.recv(1024).decode()
            if not message:
                break
            
            # Protocolo de mensajes: "ID#ACTION"
            parts = message.split("#")
            taxi_id = int(parts[0])
            action = parts[1]

            # Verificar si el taxi está registrado
            if taxi_id in taxi_db:
                if action == "INIT":
                    response = "OK#Taxi registrado y listo"
                    print(f"Taxi {taxi_id} registrado. Estado: OK")
                elif action == "STATUS":
                    response = f"OK#Estado recibido de Taxi {taxi_id}"
                else:
                    response = "KO#Acción desconocida"
            else:
                response = "KO#Taxi no registrado"

            # Enviar respuesta
            conn.send(response.encode())
    
    except Exception as e:
        print(f"Error con el taxi {addr}: {e}")
    finally:
        conn.close()

def start_central():
    # Inicia la central en el puerto 8080
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('localhost', 8080))
    server.listen(5)
    print("Central iniciada y esperando taxis...")
    
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_taxi, args=(conn, addr))
        thread.start()

if __name__ == "__main__":
    start_central()

import grpc
import message_broker_pb2
import message_broker_pb2_grpc
import threading

VALID_TOPICS = ["topic1", "topic2", "topic3"]

def run_publisher():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = message_broker_pb2_grpc.MessageBrokerStub(channel)
        while True:
            try:
                topic = input("Ingrese el tema (o 'salir' para terminar): ")
                if topic == 'salir':
                    print("Saliendo del modo productor.")
                    break
                if topic not in VALID_TOPICS:
                    print(f'Tema desconocido: {topic}. Los temas válidos son: {", ".join(VALID_TOPICS)}')
                    continue
                message = input("Ingrese el mensaje: ")
                if message.lower() == 'salir':
                    print("Saliendo del modo productor.")
                    break
                stub.Publish(message_broker_pb2.PublishRequest(topic=topic, message=message))
            except KeyboardInterrupt:
                print("\nSaliendo del modo productor.")
                break

def run_subscriber():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = message_broker_pb2_grpc.MessageBrokerStub(channel)
        topic = input("Ingrese el tema a suscribir (o 'salir' para terminar): ")
        if topic == 'salir':
            print("Saliendo del modo consumidor.")
            return
        if topic not in VALID_TOPICS:
            print(f'Tema desconocido: {topic}. Los temas válidos son: {", ".join(VALID_TOPICS)}')
            return
        try:
            print("Si desea salir, presione Ctrl+C.")
            responses = stub.Subscribe(message_broker_pb2.SubscribeRequest(topic=topic))
            for response in responses:
                print(f'Mensaje recibido en {response.topic}: {response.message}')
        except KeyboardInterrupt:
            print("\nSaliendo del modo consumidor.")

if __name__ == '__main__':
    choice = input("¿Es usted un productor (p) o un consumidor (c)? ")
    if choice == 'p':
        run_publisher()
    elif choice == 'c':
        run_subscriber()
    else:
        print("Opción no válida. Escriba 'p' para productor o 'c' para consumidor.")

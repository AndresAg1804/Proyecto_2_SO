import grpc
import message_broker_pb2
import message_broker_pb2_grpc
import threading

VALID_TOPICS = ["topic1", "topic2", "topic3"]

def handle_subscription(stub, topic):
    try:
        print(f"Suscribiéndose al tema: {topic}.")
        responses = stub.Subscribe(message_broker_pb2.SubscribeRequest(topic=topic))
        for response in responses:
            print(f'Mensaje recibido en {response.topic}: {response.message}')
    except grpc.RpcError as e:
        print(f"Error de RPC en el tema {topic}: {e}")
    except KeyboardInterrupt:
        print(f"\nSaliendo del tema {topic}.")

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
        topics = input("Ingrese los temas a suscribir, separados por comas (o 'salir' para terminar): ")
        if topics == 'salir':
            print("Saliendo del modo consumidor.")
            return
        topic_list = [topic.strip() for topic in topics.split(',')]
        invalid_topics = [topic for topic in topic_list if topic not in VALID_TOPICS]
        if invalid_topics:
            print(f'Temas desconocidos: {", ".join(invalid_topics)}. Los temas válidos son: {", ".join(VALID_TOPICS)}')
            return

        try:
            print("Si desea salir, presione Ctrl+C.")
            threads = []
            for topic in topic_list:
                thread = threading.Thread(target=handle_subscription, args=(stub, topic))
                thread.start()
                threads.append(thread)
            
            for thread in threads:
                thread.join()
        except KeyboardInterrupt:
            print("\nSaliendo del modo consumidor.")

def main():
    while True:
        choice = input("¿Es usted un productor (p), un consumidor (c) o desea salir (s)? ")
        if choice == 'p':
            run_publisher()
        elif choice == 'c':
            run_subscriber()
        elif choice == 's':
            print("Saliendo del programa.")
            break
        else:
            print("Opción no válida. Escriba 'p' para productor, 'c' para consumidor o 's' para salir.")

if __name__ == '__main__':
    main()

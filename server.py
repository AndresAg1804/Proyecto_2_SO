import grpc
from concurrent import futures
import threading
import time
import logging
import queue
import message_broker_pb2
import message_broker_pb2_grpc

class MessageBrokerServicer(message_broker_pb2_grpc.MessageBrokerServicer):
    def __init__(self):
        self.topics = {"topic1": queue.Queue(maxsize=10), "topic2": queue.Queue(maxsize=10), "topic3": queue.Queue(maxsize=10)}
        self.subscribers = {"topic1": [], "topic2": [], "topic3": []}
        self.locks = {"topic1": threading.Lock(), "topic2": threading.Lock(), "topic3": threading.Lock()}
        self.log_file = 'message_broker.log'
        logging.basicConfig(filename=self.log_file, level=logging.INFO, format='%(asctime)s %(message)s')

    def log_event(self, message):
        logging.info(message)

    def Publish(self, request, context):
        topic = request.topic
        message = request.message
        if topic not in self.topics:
            context.set_details(f'Tema desconocido: {topic}')
            context.set_code(grpc.StatusCode.UNKNOWN)
            return message_broker_pb2.Empty()

        with self.locks[topic]:
            if self.topics[topic].full():
                context.set_details(f'Cola llena para el tema: {topic}')
                context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
                return message_broker_pb2.Empty()
            self.topics[topic].put(message)
            self.log_event(f'Mensaje recibido en el tema {topic}: {message}')
            for subscriber_queue in self.subscribers[topic]:
                subscriber_queue.put(message)

        return message_broker_pb2.Empty()

    def Subscribe(self, request, context):
        topic = request.topic
        if topic not in self.topics:
            context.set_details(f'Tema desconocido: {topic}')
            context.set_code(grpc.StatusCode.UNKNOWN)
            return

        subscriber_queue = queue.Queue()
        with self.locks[topic]:
            self.subscribers[topic].append(subscriber_queue)

        try:
            while True:
                message = subscriber_queue.get()
                self.log_event(f'Mensaje enviado del tema {topic}: {message}')
                yield message_broker_pb2.Message(topic=topic, message=message)
        except grpc.RpcError as e:
            self.log_event(f'Error de RPC en Subscribe: {e}')
        finally:
            with self.locks[topic]:
                self.subscribers[topic].remove(subscriber_queue)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    message_broker_pb2_grpc.add_MessageBrokerServicer_to_server(MessageBrokerServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    logging.info('Servidor iniciado en el puerto 50051')
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logging.info('Servidor detenido manualmente')

if __name__ == '__main__':
    serve()

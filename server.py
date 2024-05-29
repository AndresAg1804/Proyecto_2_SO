import grpc
from concurrent import futures
import threading
import time
import logging
import message_broker_pb2
import message_broker_pb2_grpc

class MessageBrokerServicer(message_broker_pb2_grpc.MessageBrokerServicer):
    def __init__(self):
        self.queues = {"topic1": [], "topic2": [], "topic3": []}
        self.locks = {"topic1": threading.Lock(), "topic2": threading.Lock(), "topic3": threading.Lock()}
        self.max_queue_size = 10
        self.log_file = 'message_broker.log'
        logging.basicConfig(filename=self.log_file, level=logging.INFO, format='%(asctime)s %(message)s')

    def log_event(self, message):
        logging.info(message)

    def Publish(self, request, context):
        topic = request.topic
        message = request.message
        if topic not in self.locks:
            context.set_details(f'Tema desconocido: {topic}')
            context.set_code(grpc.StatusCode.UNKNOWN)
            return message_broker_pb2.Empty()

        with self.locks[topic]:
            if len(self.queues[topic]) >= self.max_queue_size:
                context.set_details(f'Cola llena para el tema: {topic}')
                context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
                return message_broker_pb2.Empty()
            self.queues[topic].append(message)
            self.log_event(f'Mensaje recibido en el tema {topic}: {message}')

        return message_broker_pb2.Empty()

    def Subscribe(self, request, context):
        topic = request.topic
        if topic not in self.locks:
            context.set_details(f'Tema desconocido: {topic}')
            context.set_code(grpc.StatusCode.UNKNOWN)
            return

        while True:
            try:
                with self.locks[topic]:
                    if self.queues[topic]:
                        message = self.queues[topic].pop(0)
                        self.log_event(f'Mensaje enviado del tema {topic}: {message}')
                        yield message_broker_pb2.Message(topic=topic, message=message)
                    else:
                        time.sleep(1)  # Esperar antes de intentar nuevamente
            except Exception as e:
                self.log_event(f'Error en Subscribe: {e}')
                context.set_details(str(e))
                context.set_code(grpc.StatusCode.UNKNOWN)
                break

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    message_broker_pb2_grpc.add_MessageBrokerServicer_to_server(MessageBrokerServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    logging.info('Servidor iniciado en el puerto 50051')
    server.wait_for_termination()

if __name__ == '__main__':
    serve()

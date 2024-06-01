import grpc
from concurrent import futures
import threading
import queue
import logging
import message_broker_pb2
import message_broker_pb2_grpc
import signal

VALID_TOPICS = ["topic1", "topic2", "topic3"]

class MessageBrokerServicer(message_broker_pb2_grpc.MessageBrokerServicer):
    def __init__(self):
        # Inicializa una lista de colas por cada tema
        self.queues = {topic: [] for topic in VALID_TOPICS}
        # Inicializa una cola externa para cada tema
        self.external_queues = {topic: queue.Queue(maxsize=10) for topic in VALID_TOPICS}
        self.locks = {topic: threading.Lock() for topic in VALID_TOPICS}
        self.log_file = 'message_broker.log'
        logging.basicConfig(filename=self.log_file, level=logging.INFO, format='%(asctime)s %(message)s')

    def log_event(self, message):
        logging.info(message)

    def Publish(self, request, context):
        topic = request.topic
        message = request.message
        if topic not in self.queues:
            context.set_details(f'Tema desconocido: {topic}')
            context.set_code(grpc.StatusCode.UNKNOWN)
            return message_broker_pb2.Empty()

        with self.locks[topic]:
            if not self.queues[topic]:  # No hay suscriptores
                try:
                    self.external_queues[topic].put_nowait(message)
                    self.log_event(f'Mensaje almacenado en la cola externa del tema {topic}: {message}')
                except queue.Full:
                    self.log_event(f'Cola externa llena para el tema {topic}')
            else:  # Hay suscriptores
                for q in self.queues[topic]:
                    try:
                        q.put_nowait(message)
                    except queue.Full:
                        self.log_event(f'Cola llena para el suscriptor en el tema {topic}')
        
        self.log_event(f'Mensaje recibido en el tema {topic}: {message}')
        return message_broker_pb2.Empty()

    def Subscribe(self, request, context):
        topic = request.topic
        if topic not in self.queues:
            context.set_details(f'Tema desconocido: {topic}')
            context.set_code(grpc.StatusCode.UNKNOWN)
            return
        
        q = queue.Queue(maxsize=10)
        
        with self.locks[topic]:
            self.queues[topic].append(q)
            # Transfiere mensajes de la cola externa a la nueva cola del suscriptor
            while not self.external_queues[topic].empty():
                try:
                    message = self.external_queues[topic].get_nowait()
                    q.put_nowait(message)
                    self.log_event(f'Mensaje transferido de la cola externa al suscriptor en el tema {topic}: {message}')
                except queue.Full:
                    break

        try:
            while True:
                try:
                    message = q.get(timeout=1)
                    self.log_event(f'Mensaje enviado del tema {topic}: {message}')
                    yield message_broker_pb2.Message(topic=topic, message=message)
                except queue.Empty:
                    pass  # Volver a intentar después de esperar
        except grpc.RpcError as e:
            self.log_event(f'Error de RPC en Subscribe: {e}')
        except Exception as e:
            self.log_event(f'Error en Subscribe: {e}')
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNKNOWN)
        finally:
            with self.locks[topic]:
                self.queues[topic].remove(q)

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
        server.stop(0)

if __name__ == '__main__':
    serve()

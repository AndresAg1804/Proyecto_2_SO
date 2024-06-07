# Anner Angulo Gutierrez
# Marcos Vasquez Diaz

#Referencias:
# https://www.youtube.com/watch?v=nwdL6NOBtGI
# https://www.youtube.com/watch?v=sqlV8mHoils
# https://grpc.io/
# https://github.com/chelseafarley/PythonGrpc
# https://www.ibm.com/topics/message-brokers

import queue
import threading
import logging
import grpc
from concurrent import futures
import message_broker_pb2
import message_broker_pb2_grpc
import time

VALID_TOPICS = ['topic1', 'topic2', 'topic3']  # Ejemplo de temas válidos

class MessageBrokerServicer(message_broker_pb2_grpc.MessageBrokerServicer):
    def __init__(self):
        self.queues = {topic: [] for topic in VALID_TOPICS}
        self.external_queues = {topic: queue.Queue(maxsize=10) for topic in VALID_TOPICS}
        self.locks = {topic: threading.Lock() for topic in VALID_TOPICS}
        self.conditions = {topic: threading.Condition(self.locks[topic]) for topic in VALID_TOPICS}
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

        timeout_seconds = 60
        last_message_time = time.time()
        
        try:
            while True:
                try:
                    message = q.get(timeout=1)
                    last_message_time = time.time()
                    self.log_event(f'Mensaje enviado del tema {topic}: {message}')
                    yield message_broker_pb2.Message(topic=topic, message=message)
                except queue.Empty:  # Volver a intentar después de esperar
                    if time.time() - last_message_time > timeout_seconds:
                        self.log_event(f'Suscripción al tema {topic} terminada por inactividad')
                        break
        except grpc.RpcError as e:
            self.log_event(f'Error de RPC en Subscribe: {e}')
        except Exception as e:
            self.log_event(f'Error en Subscribe: {e}')
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNKNOWN)
        finally:
            with self.locks[topic]:
                self.queues[topic].remove(q)
                # Notificar a otros hilos en espera que una cola ha sido removida
                self.conditions[topic].notify_all()

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

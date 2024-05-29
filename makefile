# Makefile para compilar y ejecutar el programa gRPC

# Variables
PYTHON:=python3
PROTO_FILE:=message_broker.proto
PROTO_PYTHON_FILES:=message_broker_pb2.py message_broker_pb2_grpc.py

# Reglas
all: $(PROTO_PYTHON_FILES)

$(PROTO_PYTHON_FILES): $(PROTO_FILE)
    $(PYTHON) -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. $(PROTO_FILE)

run_server:
    $(PYTHON) server.py

run_client:
    $(PYTHON) client.py

clean:
    rm -f $(PROTO_PYTHON_FILES)
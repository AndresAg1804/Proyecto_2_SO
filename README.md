# Segundo Proyecto Sistemas Operativos - Message Broker

## Integrantes:
- Anner Angulo Gutiérrez
- Marcos Vásquez Díaz

## Objetivos
- Describir el uso de diversas API para creación y gestión de hilos.
- Implementar técnicas de programación asociadas al uso de APIs del sistema.
- Instalar y configurar distintas tecnologías y protocolos de red.
- Implementar técnicas de comunicación y sincronización entre procesos e hilos.

## Descripción
El proyecto consiste en desarrollar una versión simplificada de un “message broker” utilizando el modelo cliente/servidor, es decir, la aplicación deberá permitir la comunicación en la red. La aplicación utilizará el estilo de mensajes “publisher/subscriber”, en el cuál un proceso productor publica un mensaje en un determinado tema, mientras que uno o varios subscriptores a ese tema lo consumen.

## Funcionamiento

1. **Ejecutar el servidor**:
   - En una terminal, ejecute el archivo `server.py`:
     ```
     python server.py
     ```

2. **Ejecutar clientes**:
   - Para cada cliente, abra una nueva terminal.
   - En cada terminal, ejecute el archivo `client.py`:
     ```
     python client.py
     ```

3. **Interacción del cliente**:
   - Al ejecutar el archivo `client.py`, se le pedirá que elija si desea ser productor (p) o consumidor (c).
   - **Modo Productor**:
     - Ingrese el tema al cual desea publicar un mensaje.
     - Luego, ingrese el mensaje que desea publicar en ese tema.
     - Puede salir del modo productor ingresando 'salir'.
   - **Modo Consumidor**:
     - Ingrese los temas a los que desea suscribirse, separados por comas.
     - Una vez suscrito, el consumidor permanecerá recibiendo mensajes.
     - Si no se reciben mensajes en un minuto, el consumidor se desuscribirá del tema y podrá decidir si desea ser consumidor o productor nuevamente.
     - Puede salir del modo consumidor ingresando 'salir' a la hora de escoger el tema.

4. **Temas válidos**:
   - Solamente existen tres temas válidos: `Videojuegos`, `Deportes`, y `Moda`.

5. **Cambio de modo**:
   - En cualquier momento, puede cambiar entre modo productor y consumidor ingresando 'salir' para salir del modo en el que me encuentro.
   - Para terminar la ejecución, ingrese 'salir' cuando vaya a escoger un modo de cliente.
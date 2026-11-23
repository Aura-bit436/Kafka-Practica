
from confluent_kafka import Consumer
from pymongo import MongoClient
import json

# Conexión con MongoDB
cliente = MongoClient('mongodb://localhost:27017/')
db = cliente['invernadero']
coleccion = db['alertas_temperatura']

# Configuración del consumidor Kafka
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'grupo-mongodb-alertas-v2',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

consumer = Consumer(config)

# Suscribirse al tópico
consumer.subscribe(['alertas-criticas'])

print("Consumidor conectado.")
print("Guardando alertas en MongoDB...\n")

try:
    while True:
        mensaje = consumer.poll(1.0)

        if mensaje is None:
            continue

        if mensaje.error():
            print("Error de Kafka:", mensaje.error())
            continue

        datos = json.loads(mensaje.value().decode('utf-8'))

        coleccion.insert_one(datos)

        print("Alerta guardada en MongoDB:")
        print(datos)
        print("--------------------------------")

except KeyboardInterrupt:
    print("\nConsumidor detenido.")

finally:
    consumer.close()
    cliente.close()

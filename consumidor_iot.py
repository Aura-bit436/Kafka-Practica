
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime
import json

# Conexión con Kafka
consumer = KafkaConsumer(
    'telemetria-iot',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='grupo-iot',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Conexión con MongoDB
cliente = MongoClient('mongodb://localhost:27017/')

# Base de datos y colección
db = cliente['invernadero']
coleccion = db['telemetria_enriquecida']

# Información de los sensores
sensores_info = {
    "S-01": {
        "ubicacion": "Invernadero Norte",
        "responsable": "Ana"
    },
    "S-02": {
        "ubicacion": "Invernadero Sur",
        "responsable": "Pedro"
    },
    "S-03": {
        "ubicacion": "Almacén Semillas",
        "responsable": "Laura"
    },
    "S-04": {
        "ubicacion": "Zona de Cuarentena",
        "responsable": "Marc"
    }
}

print("Consumidor IoT iniciado...")
print("Esperando mensajes de Kafka...\n")

try:
    for mensaje in consumer:

        # Obtener los datos originales
        datos = mensaje.value

        sensor_id = datos['sensor_id']

        # Enriquecer el mensaje
        documento = {
            "sensor_id": sensor_id,
            "temperatura": datos['temperatura'],
            "ubicacion": sensores_info[sensor_id]['ubicacion'],
            "responsable": sensores_info[sensor_id]['responsable'],
            "timestamp": datetime.now().isoformat()
        }

        # Guardar en MongoDB
        coleccion.insert_one(documento)

        print("Documento guardado en MongoDB:")
        print(documento)
        print("--------------------------------")

except KeyboardInterrupt:
    print("\nConsumidor detenido.")

finally:
    consumer.close()
    cliente.close()

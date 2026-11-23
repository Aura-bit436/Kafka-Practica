
from kafka import KafkaProducer
import json
import time
import random

# Conexión con Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sensores disponibles
sensores = ["S-01", "S-02", "S-03", "S-04"]

print("Iniciando productor IoT...")
print("Enviando datos a Kafka...\n")

try:
    while True:
        mensaje = {
            "sensor_id": random.choice(sensores),
            "temperatura": round(random.uniform(15, 38), 1)
        }

        producer.send("telemetria-iot", value=mensaje)
        producer.flush()

        print(f"Mensaje enviado: {mensaje}")

        time.sleep(2)

except KeyboardInterrupt:
    print("\nProductor detenido.")

finally:
    producer.close()

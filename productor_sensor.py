
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Conexión con Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensores = ["S-01", "S-02", "S-03", "S-04"]

print("Productor de sensores iniciado...")
print("Enviando datos a telemetria-bruta...\n")

try:
    while True:
        mensaje = {
            "sensor_id": random.choice(sensores),
            "temperatura": round(random.uniform(15, 38), 1),
            "timestamp": datetime.now().isoformat()
        }

        producer.send("telemetria-bruta", value=mensaje)
        producer.flush()

        print(f"Mensaje enviado: {mensaje}")

        time.sleep(1)

except KeyboardInterrupt:
    print("\nProductor detenido.")

finally:
    producer.close()

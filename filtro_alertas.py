
from kafka import KafkaConsumer, KafkaProducer
import json

# Consumidor del tópico de telemetría bruta
consumer = KafkaConsumer(
    'telemetria-bruta',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    group_id='grupo-filtro-alertas',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Productor del tópico de alertas críticas
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Filtro de alertas iniciado...")
print("Esperando temperaturas mayores a 30 °C...\n")

try:
    for mensaje in consumer:
        datos = mensaje.value

        if datos['temperatura'] > 30:
            producer.send('alertas-criticas', value=datos)
            producer.flush()

            print(f"ALERTA ENVIADA: {datos}")
        else:
            print(f"Temperatura normal, descartada: {datos}")

except KeyboardInterrupt:
    print("\nFiltro detenido.")

finally:
    consumer.close()
    producer.close()

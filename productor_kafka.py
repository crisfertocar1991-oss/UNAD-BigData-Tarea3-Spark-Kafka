
from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

# Conexion con Apache Kafka
producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda dato: json.dumps(dato).encode("utf-8")
)

topic = "sensor_data"

print("Iniciando simulacion de sensores...")
print("Presiona Ctrl+C para detener el envio.\n")

try:
    while True:
        # Simulacion de datos de un sensor
        mensaje = {
            "sensor_id": random.randint(1, 10),
            "temperature": round(random.uniform(20, 30), 2),
            "humidity": round(random.uniform(30, 70), 2),
            "timestamp": int(time.time())
        }

        # Enviar mensaje a Kafka
        resultado = producer.send(topic, value=mensaje)
        resultado.get(timeout=10)

        print(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "| Sensor:", mensaje["sensor_id"],
            "| Temperatura:", mensaje["temperature"],
            "| Humedad:", mensaje["humidity"]
        )

        # Enviar un mensaje cada segundo
        time.sleep(1)

except KeyboardInterrupt:
    print("\nSimulacion detenida por el usuario.")

finally:
    producer.flush()
    producer.close()
    print("Productor Kafka finalizado.")


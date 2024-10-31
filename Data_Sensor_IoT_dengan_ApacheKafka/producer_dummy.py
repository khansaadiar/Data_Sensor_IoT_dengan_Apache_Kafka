# Pengumpulan Data Sensor IoT dengan Apache Kafka - Producer
# Sylvia Febrianti - 5027221019
# Khansa Adia Rahma - 5027221071

from confluent_kafka import Producer
import json
import time
import random

def create_producer():
    conf = {'bootstrap.servers': 'localhost:9092'}
    return Producer(conf)

def simulate_sensor_data():
    sensors = {
        'S1': {'min': 65, 'max': 85, 'location': 'Area-1'},
        'S2': {'min': 70, 'max': 90, 'location': 'Area-2'},
        'S3': {'min': 60, 'max': 82, 'location': 'Area-3'}
    }
    
    producer = create_producer()
    print("Mulai mengirim data sensor...")
    
    try:
        while True:
            for sensor_id, config in sensors.items():
                data = {
                    'sensor_id': sensor_id,
                    'suhu': round(random.uniform(config['min'], config['max']), 1),
                    'lokasi': config['location'],
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # Encode dan kirim message
                msg = json.dumps(data)
                producer.produce('sensor-suhu', value=msg)
                producer.flush()
                
                print(f"Data terkirim: {data}")
                time.sleep(1)
                
    except KeyboardInterrupt:
        print("\nProducer dihentikan")
        producer.flush()

if __name__ == "__main__":
    simulate_sensor_data()
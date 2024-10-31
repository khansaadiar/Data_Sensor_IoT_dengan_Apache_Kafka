# Pengumpulan Data Sensor IoT dengan Apache Kafka - Consumer Realtime
# Sylvia Febrianti - 5027221019
# Khansa Adia Rahma - 5027221071

from confluent_kafka import Consumer
import json

def create_consumer():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'realtime-consumer-group',
        'auto.offset.reset': 'earliest'
    }
    return Consumer(conf)

def consume_realtime():
    consumer = create_consumer()
    consumer.subscribe(['sensor-suhu'])
    print("Realtime Consumer Started...")

    try:
        while True:
            msg = consumer.poll(1.0) 
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue
            
            data = json.loads(msg.value().decode('utf-8'))
            print(f"Data diterima: {data}")
    
    except KeyboardInterrupt:
        print("\nConsumer Realtime dihentikan")
    
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_realtime()

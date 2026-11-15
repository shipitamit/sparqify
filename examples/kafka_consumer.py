import json
from collections import defaultdict
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer('orders', bootstrap_servers='broker1:9092', group_id='etl',
                         auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
producer = KafkaProducer(bootstrap_servers='broker1:9092')
by_region = defaultdict(float)

for msg in consumer:
    order = msg.value
    if order['amount'] > 100:
        total = order['amount'] * order['qty']
        label = order['region'].upper()
        out = {'id': order['id'], 'total': total, 'label': label}
        producer.send('big_orders', json.dumps(out).encode())

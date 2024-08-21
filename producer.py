import psycopg2
from kafka import KafkaProducer
import logging
import json
 
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
 
# Database Connection
db_host = "192.168.3.43"
db_name = "Task6"
db_user = "Darshan"
db_password = "Darshan123"
db_port = "5432"
 
# Kafka Producer Configuration
KAFKA_HOST = 'kafka1'
KAFKA_TOPIC = 'task7'
producer = KafkaProducer(
    bootstrap_servers=f'{KAFKA_HOST}:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
 
# PostgreSQL Connection
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
cursor = conn.cursor()
 
# Fetch data from the profit_loss_data table
cursor.execute('SELECT * FROM financial_data')
records = cursor.fetchall()
 
# Get column names
cursor.execute('SELECT column_name FROM information_schema.columns WHERE table_name = %s', ('financial_data',))
column_names = [row[0] for row in cursor.fetchall()]
 
logging.info("Producing data to Kafka...")
 
try:
    for record in records:
        # Create a dictionary with column names as keys
        data = dict(zip(column_names, record))
       
        # Convert data types
        for key, value in data.items():
            if key == 'id':
                continue  # Keep id column as it is
            elif ',' in str(value):
                data[key] = int(str(value).replace(',', ''))
            elif '.' in str(value):
                data[key] = float(value)
            elif any(char.isalpha() for char in str(value)):
                data[key] = str(value)
            else:
                data[key] = int(value)
 
        # Produce the JSON data to Kafka
        producer.send(KAFKA_TOPIC, data)
        logging.info(f"Produced to Kafka: {json.dumps(data).strip()}")
except KeyboardInterrupt:
    logging.error("Process interrupted")
finally:
    cursor.close()
    conn.close()
    producer.close()
    logging.info("Connections closed")

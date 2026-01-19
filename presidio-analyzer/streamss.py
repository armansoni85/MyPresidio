from presidio_analyzer import AnalyzerEngine
from kafka import KafkaConsumer, KafkaProducer
import json
import logging
import redis
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
import time

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Analyzer
analyzer = AnalyzerEngine()

# Redis
redis_client = redis.StrictRedis(host="10.158.9.201", port=6379, db=0, decode_responses=True)

# Kafka Consumer
consumer = KafkaConsumer(
    'presidio-input',
    bootstrap_servers=['10.158.9.200:9092', '10.158.9.201:9092', '10.158.9.202:9092'],
    group_id='test-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    session_timeout_ms=60000,
    max_poll_interval_ms=300000,
    max_partition_fetch_bytes=50000000,
    enable_auto_commit=False,
    auto_offset_reset='earliest'
)

# Kafka Producer for dead-letter topic
producer = KafkaProducer(
    bootstrap_servers=['10.158.9.200:9092', '10.158.9.201:9092', '10.158.9.202:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def chunk_text(text, chunk_size=200000):
    return [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

def update_task_status(task_id, status):
    redis_client.set(f"task_status:{task_id}", status)

def process_chunk(args):
    task_id, chunk, entities, language, start_offset = args
    try:
        chunk_results = analyzer.analyze(text=chunk, entities=entities, language=language)
        adjusted_results = []
        for result in chunk_results:
            d = result.to_dict()
            d['start'] += start_offset
            d['end'] += start_offset
            adjusted_results.append(d)
        return adjusted_results
    except Exception as e:
        logging.error(f"Error processing chunk (task {task_id}): {e}")
        raise

def process_message(message):
    start_time = time.time()

    text = message.get('text', '')
    task_id = message.get('task_id', '')
    language = message.get("language", "en")
    entities = message.get('entities', [])

    if not text:
        update_task_status(task_id, "failed")
        redis_client.set(f"task_result:{task_id}", json.dumps({"error": "No text provided"}))
        return

    update_task_status(task_id, "in-progress")
    chunks = chunk_text(text)
    combined_results = []
    args = [(task_id, chunk, entities, language, sum(len(c) for c in chunks[:i])) for i, chunk in enumerate(chunks)]

    try:
        with ProcessPoolExecutor(max_workers=min(cpu_count(), len(args))) as executor:
            results = executor.map(process_chunk, args)
            for res in results:
                combined_results.extend(res)

        redis_client.set(f"task_result:{task_id}", json.dumps(combined_results))
        update_task_status(task_id, "completed")
        elapsed = time.time() - start_time
        logging.info(f"Task {task_id} completed in {elapsed:.2f} sec, found {len(combined_results)} entities.")

    except Exception as e:
        logging.error(f"Failed to process message (task {task_id}): {e}")
        producer.send("presidio-dlq", message)
        update_task_status(task_id, "failed")
        redis_client.set(f"task_result:{task_id}", json.dumps({"error": str(e)}))

def main():
    logging.info("Kafka consumer started. Polling for messages...")

    executor = ProcessPoolExecutor(max_workers=cpu_count())

    while True:
        records = consumer.poll(timeout_ms=10000, max_records=cpu_count())

        tasks = []
        for tp, msgs in records.items():
            for msg in msgs:
                task = msg.value
                logging.info(f"Queued message from partition {msg.partition}, offset {msg.offset}")
                tasks.append(executor.submit(process_message, task))

        for future in as_completed(tasks):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Processing failed during execution: {e}")

        consumer.commit()

if __name__ == "__main__":
    main()


from presidio_analyzer import AnalyzerEngine
from kafka import KafkaConsumer, KafkaProducer
import json
import logging
import redis
from concurrent.futures import ThreadPoolExecutor
import time  # Added for measuring execution time
from queue import Queue

message_queue = Queue(maxsize=10)

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Initialize Presidio Analyzer
analyzer = AnalyzerEngine()

# Redis Connection Pool Configuration
redis_pool = redis.ConnectionPool(
    host="10.158.9.201",
    port=6379,
    db=0,
    decode_responses=True,
    max_connections=10  # Adjust based on your workload and requirements
)
redis_client = redis.StrictRedis(connection_pool=redis_pool)

# Kafka Consumer (reads from presidio-input)
consumer = KafkaConsumer(
    'presidio-input',
    bootstrap_servers=['10.158.9.200:9092', '10.158.9.201:9092', '10.158.9.202:9092'],
    group_id='test-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    session_timeout_ms=120000,  # Increase timeout for longer processing
    max_poll_interval_ms=300000,  # Allow longer time for chunk processing
    max_partition_fetch_bytes=10485760,  # Fetch up to 10 MB per partition
    auto_offset_reset="latest"  # Process only new messages
)

# Kafka Producer (writes to presidio-output)
producer = KafkaProducer(
    bootstrap_servers=['10.158.9.200:9092', '10.158.9.201:9092', '10.158.9.202:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    max_request_size=10485760  # Allow large messages (10 MB)
)

# Thread pool for parallel processing
executor = ThreadPoolExecutor(max_workers=8)  # Adjust based on vCPUs (8 CPUs here)

def chunk_text(text, chunk_size=250000):
    """
    Splits text into chunks of specified size.
    """
    return [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

def update_task_status(task_id, status):
    """
    Update the status of a task in Redis.
    """
    redis_client.set(f"task_status:{task_id}", status)

def process_chunk(task_id, chunk, entities, language, start_offset):
    """
    Process a single chunk of text using Presidio Analyzer.
    """
    try:
        chunk_results = analyzer.analyze(
            text=chunk,
            entities=entities,
            language=language
        )
        # Adjust offsets for the chunk
        adjusted_results = []
        for result in chunk_results:
            adjusted_result = result.to_dict()
            adjusted_result['start'] += start_offset
            adjusted_result['end'] += start_offset
            adjusted_results.append(adjusted_result)

        return adjusted_results
    except Exception as e:
        logging.error(f"Error processing chunk for task {task_id}: {e}")
        return []

def process_message(message):
    """
    Process a single Kafka message using Presidio Analyzer.
    """
    start_time = time.time()  # Start timing

    text = message.get('text', '')
    task_id = message.get('task_id', '')
    language = message.get("language", "en")
    entities = message.get('entities', [])

    if not text:
        logging.warning("Received message with no text field.")
        update_task_status(task_id, "failed")
        redis_client.set(f"task_result:{task_id}", json.dumps({"error": "No text provided"}))
        return

    # Update task status to "in-progress"
    update_task_status(task_id, "in-progress")

    chunks = chunk_text(text)
    combined_results = []
    start_offset = 0

    futures = []
    for index, chunk in enumerate(chunks):
        logging.info(f"Scheduling processing for chunk {index + 1}/{len(chunks)}: {chunk[:100]}...")
        futures.append(executor.submit(process_chunk, task_id, chunk, entities, language, start_offset))
        start_offset += len(chunk)

    # Aggregate results from all futures
    for future in futures:
        combined_results.extend(future.result())

    # Save results to Redis
    redis_client.set(f"task_result:{task_id}", json.dumps(combined_results))
    update_task_status(task_id, "completed")

    end_time = time.time()  # End timing
    elapsed_time = end_time - start_time  # Calculate execution time

    logging.info(f"Task {task_id} completed in {elapsed_time:.2f} seconds.")  # Log execution time


def worker_loop():
    while True:
        task = message_queue.get()
        try:
            process_message(task)
        finally:
            message_queue.task_done()

def main():
    """
    Main function to consume messages, process them, and publish results.
    """

    logging.info("Starting worker threads")

    logging.info("Kafka consumer started. Waiting for messages from 'presidio-input'.")
    for msg in consumer:
        task = msg.value
        logging.info(f"Received message from 'presidio-input': {task}")

        # Process the text
        executor.submit(process_message, task)

if __name__ == "__main__":
    main()


from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import SpacyNlpEngine
from kafka import KafkaConsumer, KafkaProducer
import json
import logging
import redis
from multiprocessing import Pool, cpu_count
import spacy

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load SpaCy NLP model
nlp = spacy.load("en_core_web_lg")
nlp.max_length = 110000000  # Set max_length to handle up to 100 MB of text
logging.info(f"SpaCy model loaded with max_length: {nlp.max_length}")

# Create the SpacyNlpEngine with lang_code explicitly passed
spacy_nlp_engine = SpacyNlpEngine(models={"en": {"model": nlp, "lang_code": "en"}})

# Confirm models loaded
logging.info(f"Loaded SpacyNlpEngine models: {spacy_nlp_engine.models}")

# Initialize Presidio Analyzer with the custom NLP engine
analyzer = AnalyzerEngine(nlp_engine=spacy_nlp_engine)

# Redis Configuration
redis_pool = redis.ConnectionPool(
    host="10.158.9.201",
    port=6379,
    db=0,
    decode_responses=True,
    max_connections=10  # Adjust based on your workload
)
redis_client = redis.StrictRedis(connection_pool=redis_pool)

# Kafka Consumer Configuration (reads from presidio-input)
consumer = KafkaConsumer(
    'presidio-input',
    bootstrap_servers=['10.158.9.200:9092', '10.158.9.201:9092', '10.158.9.202:9092'],
    group_id='test-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    session_timeout_ms=300000,
    max_poll_interval_ms=600000,
    max_partition_fetch_bytes=200000000,
    enable_auto_commit=False,
    auto_offset_reset="latest"
)

# Kafka Producer Configuration (writes to presidio-output)
producer = KafkaProducer(
    bootstrap_servers=['10.158.9.200:9092', '10.158.9.201:9092', '10.158.9.202:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    max_request_size=200000000
)

def chunk_text(text, chunk_size=5000000):
    """Splits text into chunks of specified size."""
    return [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

def update_task_status(task_id, status):
    """Update the status of a task in Redis."""
    redis_client.set(f"task_status:{task_id}", status)

def process_chunk(args):
    """Process a single chunk of text using Presidio Analyzer."""
    task_id, chunk, entities, language, start_offset = args
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
    """Process a single Kafka message using Presidio Analyzer."""
    start_time = time.time()

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

    with Pool(cpu_count()) as pool:
        args = [(task_id, chunk, entities, language, start_offset + i * len(chunk)) for i, chunk in enumerate(chunks)]
        results = pool.map(process_chunk, args)
        for res in results:
            combined_results.extend(res)

    # Save results to Redis
    redis_client.set(f"task_result:{task_id}", json.dumps(combined_results))
    update_task_status(task_id, "completed")

    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f"Task {task_id} completed in {elapsed_time:.2f} seconds.")

def main():
    """Main function to consume messages, process them, and publish results."""
    logging.info("Kafka consumer started. Waiting for messages from 'presidio-input'.")
    for msg in consumer:
        task = msg.value
        logging.info(f"Received message from 'presidio-input': {task}")
        process_message(task)

if __name__ == "__main__":
    main()


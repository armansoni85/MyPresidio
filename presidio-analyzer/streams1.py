from presidio_analyzer import AnalyzerEngine
from kafka import KafkaConsumer, KafkaProducer
import json
import logging
import redis

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Initialize Presidio Analyzer
analyzer = AnalyzerEngine()

# Redis Configuration
redis_client = redis.StrictRedis(host="10.158.9.201", port=6379, db=0, decode_responses=True)

# Kafka Consumer (reads from presidio-input)
consumer = KafkaConsumer(
    'presidio-input',
    bootstrap_servers=['10.158.9.200:9092', '10.158.9.201:9092'],
    group_id='test-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    session_timeout_ms=60000,
    heartbeat_interval_ms=20000,
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    auto_offset_reset="latest"  # Ensure only new messages are processed
)

# Kafka Producer (writes to presidio-output)
producer = KafkaProducer(
    bootstrap_servers=['10.158.9.200:9092', '10.158.9.201:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    max_request_size=10485760  # Allow large messages (10 MB)
)

def chunk_text(text, chunk_size=5000):
    """
    Splits text into chunks of specified size.
    """
    return [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

def update_task_status(task_id, status):
    """
    Update the status of a task in Redis.
    """
    redis_client.set(f"task_status:{task_id}", status)

def process_message(message):
    """
    Process a single Kafka message using Presidio Analyzer.
    """
    text = message.get('text', '')
    task_id = message.get('task_id', '')
    language = task.get("language", "en")
    entities = message.get('entities', [])

    if not text:
        logging.warning("Received message with no text field.")
        update_task_status(task_id, "failed")
        return {"task_id": task_id, "error": "No text provided"}

    # Update task status to "in-progress"
    update_task_status(task_id, "in-progress")

    # Chunk the text into manageable sizes
    chunks = chunk_text(text)
    combined_results = []  # Collect results for all chunks

    start_offset = 0  # Track the position offset for combining chunk results

    for index, chunk in enumerate(chunks):
        logging.info(f"Analyzing chunk {index + 1}/{len(chunks)}: {chunk[:100]}...")
        try:
            chunk_results = analyzer.analyze(
                text=chunk,
                entities=entities,
                language=message.get('language', 'en')
            )
            # Adjust start and end positions for chunk offset
            for result in chunk_results:
                adjusted_result = result.to_dict()
                adjusted_result['start'] += start_offset
                adjusted_result['end'] += start_offset

                # Handle NoneType in analysis_explanation
                if adjusted_result.get('analysis_explanation') is None:
                    logging.warning(f"No analysis explanation for entity: {adjusted_result.get('entity_type')}")
                combined_results.append(adjusted_result)
        except AttributeError as e:
            logging.error(f"Error analyzing chunk {index + 1}: {e}")
        except Exception as e:
            logging.error(f"Unhandled error analyzing chunk {index + 1}: {e}")

        # Update the start_offset for the next chunk
        start_offset += len(chunk)

    # Save results to Redis
    redis_client.set(f"task_result:{task_id}", json.dumps(combined_results))

    # Update task status to "completed"
    update_task_status(task_id, "completed")

    # Return aggregated results
    return {
        "task_id": task_id,
        "entities": combined_results
    }

def main():
    """
    Main function to consume messages, process them, and publish results.
    """
    logging.info("Kafka consumer started. Waiting for messages from 'presidio-input'.")
    for msg in consumer:
        task = msg.value
        logging.info(f"Received message from 'presidio-input': {task}")

        # Process the text
        result = process_message(task)

        # Send result to Kafka output topic
        if result:
            try:
                producer.send('presidio-output', value=result).get(timeout=10)
                logging.info(f"Sent processed result to 'presidio-output': {result}")
            except Exception as e:
                logging.error(f"Failed to send message to 'presidio-output': {e}")

if __name__ == "__main__":
    main()


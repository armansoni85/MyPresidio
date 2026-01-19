import zlib
import json
import logging
import multiprocessing
import time
import os
import redis
import mysql.connector
from kafka import KafkaConsumer
from presidio_analyzer import AnalyzerEngine
from dotenv import load_dotenv
import socket

load_dotenv()

# ================= LOGGING =================
log_dir = '/var/www/presidio_celery/presidio-analyzer/logs'
os.makedirs(log_dir, exist_ok=True)

# We use INFO to keep logs clean, but silence the NLP internal debug noise
logging.basicConfig(
    filename=os.path.join(log_dir, 'streams.log'),
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logging.getLogger("presidio-analyzer").setLevel(logging.ERROR)
logging.getLogger("spacy").setLevel(logging.ERROR)

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "autocommit": True
}

def update_mysql_task(task_id, status, text_length=None, entities=None, result=None, error_message=None):
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cur = conn.cursor()
        updates, params = ["status=%s", "updated_at=NOW()"], [status]

        if text_length is not None:
            updates.append("text_length=%s")
            params.append(text_length)
        if entities is not None:
            updates.append("entities=%s")
            params.append(json.dumps(entities))
        if result is not None:
            updates.append("result=%s")
            params.append(json.dumps(result))
        if error_message is not None:
            updates.append("error_message=%s")
            params.append(str(error_message))

        params.append(task_id)
        cur.execute(f"UPDATE presidio_tasks SET {', '.join(updates)} WHERE task_id=%s", tuple(params))
        conn.commit()
    except Exception as e:
        logging.error(f"DB update failed for {task_id}: {e}")
    finally:
        if conn: conn.close()

# ================== WORKER PROCESS ==================
def worker_process(task_queue):
    # Initialize engine once per worker to save CPU
    analyzer = AnalyzerEngine()
    # decode_responses=False because we handle zlib binary data
    redis_client = redis.StrictRedis(host="10.158.9.200", port=6379, decode_responses=False)

    while True:
        task = task_queue.get()
        if task is None: break

        task_id = task["task_id"]
        data_key = task["redis_key"]
        start_time = time.time()

        try:
            # 1. READ LOG: Start time and Key
            read_start = time.time()

            # --- FIX 2: Added Retry Logic for Redis ---
            compressed_payload = redis_client.get(data_key)
            #compressed_payload = None
            #for attempt in range(1, 4):
            #    compressed_payload = redis_client.get(data_key)
            #    if compressed_payload:
            #        break
            #    logging.warning(f"Task {task_id}: Redis payload missing (Attempt {attempt}/3). Retrying...")
            #    time.sleep(0.5) # Wait 500ms before retrying

            if not compressed_payload:
                raise ValueError(f"Payload missing in Redis for key: {data_key}")
            # ------------------------------------------

            update_mysql_task(task_id, "in-progress")

            # 2. DECOMPRESS
            full_text = zlib.decompress(compressed_payload).decode('utf-8')
            read_duration = round(time.time() - read_start, 3)

            # 3. DYNAMIC STRATEGY BASED ON CONTENT
            text_len = len(full_text)
            size_mb = text_len / (1024 * 1024)

            # Strict Chunking to fix [E088] and preserve RAM
            if size_mb < 3:
                chunk_size = 100000  # 0.1MB (Below the 1M SpaCy limit)
                strategy = "FAST-2MB"
            elif size_mb < 15:
                chunk_size = 100000  # 0.5MB
                strategy = "BALANCED-10MB"
            else:
                chunk_size = 200000  # 0.2MB (Safest for 50MB files)
                strategy = "STABLE-50MB"

            logging.info(f"Task {task_id} | Read: {read_duration}s | Size: {size_mb:.2f}MB | Mode: {strategy}")

            # 4. PROCESS CHUNKS
            all_results = []
            for i in range(0, text_len, chunk_size):
                chunk = full_text[i : i + chunk_size]

                chunk_findings = analyzer.analyze(
                    text=chunk,
                    entities=task.get("entities", []),
                    language=task.get("language", "en")
                )

                for finding in chunk_findings:
                    f_dict = finding.to_dict()
                    f_dict["start"] += i
                    f_dict["end"] += i
                    all_results.append(f_dict)

            # 5. DB UPDATE & REDIS CLEANUP
            # --- FIX 1: Fixed variable typo 'all_result' -> 'all_results' ---
            update_mysql_task(
                task_id,
                "completed",
                text_length=text_len,
                entities=task.get("entities"),
                result=all_results
            )
            # ----------------------------------------------------------------

            redis_client.delete(data_key) # Immediate memory release on 9.201

            # 6. FINISH LOG: Task ID, Finish Time, File Size
            finish_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            total_duration = round(time.time() - start_time, 3)
            logging.info(f"FINISHED Task: {task_id} | Time: {finish_time} | Total: {total_duration}s | Size: {size_mb:.2f}MB")

        except Exception as e:
            logging.error(f"Worker Error on {task_id}: {e}")
            update_mysql_task(task_id, "failed", error_message=str(e))
        finally:
            # Force local string cleanup
            full_text = None

# ================== MAIN ORCHESTRATOR ==================
def main():
    # 6 workers per VM is the 'Goldilocks' zone for 24GB RAM.
    # It leaves 1-2 CPUs free for system/Redis and ~10GB RAM headroom for OS.
    num_workers = 6
    task_queue = multiprocessing.Queue(maxsize=12)

    processes = []
    for i in range(num_workers):
        p = multiprocessing.Process(target=worker_process, args=(task_queue,))
        p.daemon = True
        p.start()
        processes.append(p)
        logging.info(f"Starting Worker {i+1}...")

    # Kafka Configuration for Large Payloads
    consumer = KafkaConsumer(
        "presidio-input",
        bootstrap_servers=["10.158.9.201:9092", "10.158.9.202:9092"],
        group_id="presidio-workers",
        value_deserializer=lambda v: json.loads(v.decode()),
        auto_offset_reset="earliest",
        # Pull enough for workers but not so many that pointers waste RAM
        max_poll_records=6,
        # Allow 30 minutes for a heavy 50MB batch to finish
        max_poll_interval_ms=1800000,
        session_timeout_ms=45000,
        enable_auto_commit=True
    )

    logging.info("Multi-VM Stream Processor ready (2MB - 100MB support)")

    try:
        for msg in consumer:
            task_queue.put(msg.value)
    except KeyboardInterrupt:
        logging.info("Graceful shutdown initiated...")

if __name__ == "__main__":
    main()

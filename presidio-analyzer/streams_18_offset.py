from presidio_analyzer import AnalyzerEngine
from kafka import KafkaConsumer, KafkaProducer
import json
import logging
import redis
import mysql.connector
import time
import signal
import sys
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count, Semaphore

# ================= CONFIG =================
MAX_CPUS = 8
MAX_WORKERS = 8                     # Exactly match VM CPU
MAX_INFLIGHT_TASKS = 8              # Backpressure control
CHUNK_SIZE = 250000                 # ~250 KB safe chunk
MAX_POLL_RECORDS = 4                # Keep polling fast

# ================= LOGGING =================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ================= REDIS =================
redis_client = redis.StrictRedis(
    host="10.158.9.201",
    port=6379,
    db=0,
    decode_responses=True
)

# ================= MYSQL =================
DB_CONFIG = {
    "host": "10.158.9.161",
    "user": "arman",
    "password": "ArmDev123!",
    "database": "stage1_presidio",
    "autocommit": True
}

def get_db():
    return mysql.connector.connect(**DB_CONFIG)

def db_update_status(task_id, status, result=None, error=None):
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE presidio_tasks
            SET status=%s,
                result=%s,
                error_message=%s,
                updated_at=NOW()
            WHERE task_id=%s
            """,
            (status, json.dumps(result) if result else None, error, task_id)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"[DB] update failed task_id={task_id}: {e}")

# ================= KAFKA =================
consumer = KafkaConsumer(
    "presidio-input",
    bootstrap_servers=[
        "10.158.9.200:9092",
        "10.158.9.201:9092",
        "10.158.9.202:9092"
    ],
    group_id="presidio-workers",
    enable_auto_commit=False,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    session_timeout_ms=60000,
    max_poll_interval_ms=300000,
    max_partition_fetch_bytes=50000000
)

producer = KafkaProducer(
    bootstrap_servers=[
        "10.158.9.200:9092",
        "10.158.9.201:9092",
        "10.158.9.202:9092"
    ],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ================= GLOBALS =================
shutdown_flag = False
task_semaphore = Semaphore(MAX_INFLIGHT_TASKS)

# ================= SIGNALS =================
def shutdown_handler(signum, frame):
    global shutdown_flag
    shutdown_flag = True
    logging.info("Shutdown signal received")

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)

# ================= WORKER INIT =================
_ANALYZER = None

def init_worker():
    global _ANALYZER
    _ANALYZER = AnalyzerEngine()
    logging.info("AnalyzerEngine loaded in worker")

# ================= HELPERS =================
def chunk_text_stream(text, size):
    for i in range(0, len(text), size):
        yield i, text[i:i + size]

# ================= TASK PROCESS =================
def process_message(message):
    global _ANALYZER

    task_id = message.get("task_id")
    text = message.get("text", "")
    entities = message.get("entities", [])
    language = message.get("language", "en")

    start = time.time()

    try:
        redis_client.set(f"task_status:{task_id}", "in-progress")
        db_update_status(task_id, "in-progress")

        results = []

        for offset, chunk in chunk_text_stream(text, CHUNK_SIZE):
            chunk_results = _ANALYZER.analyze(
                text=chunk,
                entities=entities,
                language=language
            )
            for r in chunk_results:
                d = r.to_dict()
                d["start"] += offset
                d["end"] += offset
                results.append(d)

        #redis_client.set(f"task_result:{task_id}", json.dumps(results))
        redis_client.set(f"task_status:{task_id}", "completed")
        db_update_status(task_id, "completed", result=results)

        logging.info(
            f"Task {task_id} done | "
            f"entities={len(results)} | "
            f"time={time.time() - start:.2f}s"
        )

    except Exception as e:
        logging.error(f"Task failed {task_id}: {e}")
        producer.send("presidio-dlq", message)
        redis_client.set(f"task_status:{task_id}", "failed")
        db_update_status(task_id, "failed", error=str(e))

    finally:
        task_semaphore.release()

# ================= MAIN =================
def main():
    logging.info("Presidio stream worker started")

    with ProcessPoolExecutor(
        max_workers=MAX_WORKERS,
        initializer=init_worker
    ) as executor:

        while not shutdown_flag:
            records = consumer.poll(
                timeout_ms=1000,
                max_records=MAX_POLL_RECORDS
            )

            for tp, msgs in records.items():
                for msg in msgs:
                    task_semaphore.acquire()          # Limit only submission
                    executor.submit(process_message, msg.value)

            # ✅ commit immediately after submission
            if records:
                consumer.commit()

    logging.info("Worker shutdown complete")

# ================= ENTRY =================
if __name__ == "__main__":
    main()

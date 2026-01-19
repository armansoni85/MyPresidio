from presidio_analyzer import AnalyzerEngine, BatchAnalyzerEngine
from kafka import KafkaConsumer, KafkaProducer
import json
import logging
import redis
import threading
import time
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_EXCEPTION
import psutil
import gc

# ==================== Configuration ====================
# Memory safety thresholds (adjust based on 24GB RAM)
MAX_MEMORY_PERCENT = 70  # Stop processing if memory usage exceeds 70%
CHUNK_SIZE = 100000  # ~100KB chunks (smaller for better memory control)
MAX_CONCURRENT_MESSAGES = 2  # Process 2 messages concurrently per VM
MAX_CHUNK_WORKERS = 2  # Parallel chunk processing within a message

# ==================== Logging Setup ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s",
    handlers=[
        logging.FileHandler('/var/log/presidio_streams.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== Memory Management ====================
class MemoryGuard:
    """Monitor and control memory usage"""
    
    @staticmethod
    def memory_usage_percent():
        return psutil.virtual_memory().percent
    
    @staticmethod
    def should_pause_processing():
        return MemoryGuard.memory_usage_percent() > MAX_MEMORY_PERCENT
    
    @staticmethod
    def force_garbage_collection():
        collected = gc.collect()
        logger.debug(f"Garbage collection freed {collected} objects")
        return collected

# ==================== Initialize Components ====================
# Presidio Analyzer (shared thread-safe instance)
analyzer = AnalyzerEngine()
batch_analyzer = BatchAnalyzerEngine(analyzer_engine=analyzer)

# Redis Configuration
redis_client = redis.StrictRedis(
    host="10.158.9.201", 
    port=6379, 
    db=0, 
    decode_responses=True,
    socket_connect_timeout=5,
    retry_on_timeout=True
)

# Kafka Consumer
consumer = KafkaConsumer(
    'presidio-input',
    bootstrap_servers=['10.158.9.200:9092', '10.158.9.201:9092', '10.158.9.202:9092'],
    group_id='presidio-consumer-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    max_poll_records=MAX_CONCURRENT_MESSAGES * 2,  # Slightly more than processing capacity
    fetch_max_bytes=52428800,  # 50MB max fetch
    max_partition_fetch_bytes=10485760,  # 10MB per partition
    enable_auto_commit=False,  # Manual offset management
    auto_offset_reset='latest',
    session_timeout_ms=30000,
    max_poll_interval_ms=3600000,
    heartbeat_interval_ms=10000
)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['10.158.9.200:9092', '10.158.9.201:9092', '10.158.9.202:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    compression_type='gzip',
    max_request_size=10485760,
    buffer_memory=268435456
)

# ==================== Processing Functions ====================
def chunk_text(text, chunk_size=CHUNK_SIZE):
    """Split text into manageable chunks with overlap for context preservation"""
    chunks = []
    for i in range(0, len(text), chunk_size):
        chunk = text[i:i + chunk_size]
        # Add small overlap to avoid splitting entities
        if i > 0 and len(chunk) == chunk_size:
            # Look for a natural break (space, punctuation) near the end
            overlap_point = chunk_size - 100  # 100 chars overlap
            for j in range(overlap_point, max(overlap_point - 50, 0), -1):
                if chunk[j] in ' .,;!?\n':
                    chunk = text[i-100:i + j]  # Include 100 chars from previous chunk
                    break
        chunks.append(chunk)
    return chunks

def update_task_status(task_id, status):
    """Update task status in Redis with expiration"""
    try:
        redis_client.setex(
            f"task_status:{task_id}",
            3600,  # Expire after 1 hour
            status
        )
    except Exception as e:
        logger.error(f"Failed to update task status {task_id}: {e}")

def process_chunk_with_memory_check(args):
    """Process a single chunk with memory monitoring"""
    chunk, entities, language, start_offset = args
    
    # Check memory before processing
    if MemoryGuard.should_pause_processing():
        logger.warning(f"Memory threshold exceeded, pausing chunk processing")
        time.sleep(5)  # Brief pause
        MemoryGuard.force_garbage_collection()
    
    try:
        chunk_results = analyzer.analyze(
            text=chunk,
            entities=entities,
            language=language
        )
        
        # Adjust offsets
        adjusted_results = []
        for result in chunk_results:
            adjusted_result = result.to_dict()
            adjusted_result['start'] += start_offset
            adjusted_result['end'] += start_offset
            adjusted_results.append(adjusted_result)
        
        return adjusted_results
    except Exception as e:
        logger.error(f"Error processing chunk: {e}")
        return []

def process_message_optimized(message):
    """Process a single message with controlled parallelism and memory management"""
    task_id = message.get('task_id')
    text = message.get('text', '')
    language = message.get('language', 'en')
    entities = message.get('entities', [])
    
    if not text or not task_id:
        logger.error(f"Invalid message: {message}")
        return
    
    logger.info(f"Processing task {task_id}, text length: {len(text)}")
    update_task_status(task_id, "in-progress")
    
    # Early memory check
    if MemoryGuard.should_pause_processing():
        logger.warning(f"Memory high at start of task {task_id}, delaying...")
        time.sleep(10)
    
    start_time = time.time()
    combined_results = []
    
    try:
        # Stream processing: process chunks as they're generated
        chunks = []
        start_offset = 0
        
        # Generate and process chunks in batches
        for i in range(0, len(text), CHUNK_SIZE * 10):  # Process 10 chunks at a time
            chunk_batch = []
            offsets = []
            
            # Prepare batch of chunks
            for j in range(0, min(CHUNK_SIZE * 10, len(text) - i), CHUNK_SIZE):
                chunk = text[i + j:i + j + CHUNK_SIZE]
                if chunk:
                    chunk_batch.append((chunk, entities, language, i + j))
                    offsets.append(i + j)
            
            # Process batch with limited parallelism
            with ThreadPoolExecutor(max_workers=MAX_CHUNK_WORKERS, 
                                   thread_name_prefix=f"ChunkWorker-{task_id}") as chunk_executor:
                futures = [chunk_executor.submit(process_chunk_with_memory_check, args) 
                          for args in chunk_batch]
                
                # Collect results as they complete
                for future in futures:
                    try:
                        chunk_results = future.result(timeout=30)
                        combined_results.extend(chunk_results)
                    except Exception as e:
                        logger.error(f"Chunk processing failed: {e}")
            
            # Force garbage collection after each batch
            MemoryGuard.force_garbage_collection()
            
            # Check memory between batches
            if MemoryGuard.should_pause_processing():
                logger.warning(f"Memory threshold exceeded, pausing batch processing")
                time.sleep(5)
        
        # Store results in Redis with compression consideration
        if combined_results:
            # Consider storing only IDs or references for very large results
            if len(json.dumps(combined_results)) > 10000000:  # >10MB
                logger.warning(f"Large result set for task {task_id}, consider alternative storage")
                # Option: Store in multiple Redis keys or external storage
                redis_client.setex(
                    f"task_result:{task_id}",
                    3600,
                    json.dumps({"count": len(combined_results), "note": "Results too large for Redis"})
                )
            else:
                redis_client.setex(
                    f"task_result:{task_id}",
                    3600,
                    json.dumps(combined_results)
                )
        else:
            redis_client.setex(
                f"task_result:{task_id}",
                3600,
                json.dumps([])
            )
        
        update_task_status(task_id, "completed")
        elapsed_time = time.time() - start_time
        logger.info(f"Task {task_id} completed in {elapsed_time:.2f}s, {len(combined_results)} entities found")
        
    except Exception as e:
        logger.error(f"Failed to process task {task_id}: {e}")
        update_task_status(task_id, "failed")
        redis_client.setex(
            f"task_result:{task_id}",
            3600,
            json.dumps({"error": str(e)})
        )
    
    # Final memory cleanup
    MemoryGuard.force_garbage_collection()

# ==================== Main Processing Loop ====================
class StreamProcessor:
    """Manages concurrent message processing with backpressure"""
    
    def __init__(self):
        self.processing_semaphore = threading.Semaphore(MAX_CONCURRENT_MESSAGES)
        self.shutdown_flag = False
        self.active_tasks = {}
        
    def process_with_backpressure(self, message):
        """Process a message with concurrency control"""
        with self.processing_semaphore:
            if self.shutdown_flag:
                return
            process_message_optimized(message)
    
    def run(self):
        """Main processing loop"""
        logger.info(f"Starting stream processor with {MAX_CONCURRENT_MESSAGES} concurrent messages")
        
        while not self.shutdown_flag:
            try:
                # Check memory before polling
                if MemoryGuard.should_pause_processing():
                    mem_usage = MemoryGuard.memory_usage_percent()
                    logger.warning(f"Memory usage {mem_usage}% > {MAX_MEMORY_PERCENT}%, pausing consumption")
                    time.sleep(10)
                    continue
                
                # Poll for messages
                raw_messages = consumer.poll(timeout_ms=5000, max_records=MAX_CONCURRENT_MESSAGES)
                
                if not raw_messages:
                    continue
                
                # Process each partition's messages
                for tp, messages in raw_messages.items():
                    for msg in messages:
                        if self.shutdown_flag:
                            break
                            
                        try:
                            # Submit for processing
                            message = msg.value
                            task_id = message.get('task_id', 'unknown')
                            
                            logger.info(f"Received task {task_id} from partition {tp.partition}")
                            
                            # Process in background thread
                            import threading
                            thread = threading.Thread(
                                target=self.process_with_backpressure,
                                args=(message,),
                                name=f"Processor-{task_id}"
                            )
                            thread.daemon = True
                            thread.start()
                            self.active_tasks[task_id] = thread
                            
                        except Exception as e:
                            logger.error(f"Failed to handle message: {e}")
                    
                    # Manually commit offsets for this partition
                    if not self.shutdown_flag:
                        try:
                            consumer.commit()
                        except Exception as e:
                            logger.error(f"Failed to commit offsets: {e}")
                
                # Clean up finished threads
                self._cleanup_threads()
                
            except KeyboardInterrupt:
                logger.info("Shutdown signal received")
                self.shutdown_flag = True
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(1)
        
        self.shutdown()
    
    def _cleanup_threads(self):
        """Remove completed threads from active tasks"""
        to_remove = []
        for task_id, thread in list(self.active_tasks.items()):
            if not thread.is_alive():
                to_remove.append(task_id)
        
        for task_id in to_remove:
            del self.active_tasks[task_id]
    
    def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down stream processor...")
        self.shutdown_flag = True
        
        # Wait for active tasks
        for task_id, thread in self.active_tasks.items():
            thread.join(timeout=30)
            if thread.is_alive():
                logger.warning(f"Task {task_id} did not complete in timeout")
        
        consumer.close()
        producer.close()
        logger.info("Stream processor shutdown complete")

# ==================== Entry Point ====================
if __name__ == "__main__":
    # Install psutil if not available: pip install psutil
    try:
        import psutil
    except ImportError:
        logger.warning("psutil not installed, memory monitoring disabled")
        logger.warning("Install with: pip install psutil")
    
    processor = StreamProcessor()
    
    try:
        processor.run()
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        processor.shutdown()

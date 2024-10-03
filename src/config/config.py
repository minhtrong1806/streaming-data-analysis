KAFKA_BROKER_URL="localhost:9092"


# Producer Configuration
PRODUCER_CONFIG = {
    'bootstrap_servers': KAFKA_BROKER_URL,
    'acks': 'all',  # Yêu cầu xác nhận từ tất cả các replica
    'retries': 3,  # Số lần thử lại khi gửi thất bại
    'linger_ms': 10,  # Thời gian chờ trước khi gửi batch (ms)
    'batch_size': 16384  # Kích thước batch (bytes)
}

# Consumer Configuration
CONSUMER_CONFIG = {
    'bootstrap_servers': KAFKA_BROKER_URL,
    'group_id': 'your_consumer_group',
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': False,
}
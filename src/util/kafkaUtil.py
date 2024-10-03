import logging
from kafka import KafkaAdminClient
from config.config import KAFKA_BROKER_URL

class KafkaTopicManager:
    def __init__(self):
        """Khởi tạo Kafka Admin Client."""
        self.kafka_broker = KAFKA_BROKER_URL
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.kafka_broker)
        self.setup_logging()
        self.logger = logging.getLogger(__name__)

    def setup_logging(self):
        """Thiết lập cấu hình logging."""
        logging.basicConfig(
            level=logging.INFO,  
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'  
        )
        
    def create_topic(self, topic_name):
        """Tạo topic Kafka nếu chưa tồn tại."""
        try:
            self.admin_client.create_topics([{
                'topic': topic_name,
                'num_partitions': 1,
                'replication_factor': 1
            }])
            self.logger.info(f"Đã tạo topic: {topic_name}")
        except Exception as e:
            self.logger.warning(f"Không thể tạo topic {topic_name}: {e}")
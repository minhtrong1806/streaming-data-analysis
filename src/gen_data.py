import json
import os
import time
import pandas as pd
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
from util.kafkaUtil import KafkaTopicManager
from config.config import KAFKA_BROKER_URL, PRODUCER_CONFIG

class TaxiDataGenerator:
    def __init__(self, parquet_file_path):
        """Khởi tạo các biến và cấu hình cần thiết."""
        self.parquet_file_path = parquet_file_path
        self.df = self.read_parquet_data()

    def read_parquet_data(self):
        """Đọc dữ liệu từ file Parquet và chuyển đổi sang DataFrame của Pandas."""
        return pd.read_parquet(self.parquet_file_path)

    def filter_data(self):
        """Lọc dữ liệu chỉ lấy bản ghi từ tháng 07/2024."""
        self.df['tpep_pickup_datetime'] = pd.to_datetime(self.df['tpep_pickup_datetime'])
        filtered_df = self.df[(self.df['tpep_pickup_datetime'] >= '2024-07-01') & 
                               (self.df['tpep_pickup_datetime'] < '2024-08-01')]
        return filtered_df

    def sort_data(self, df):
        """Sắp xếp dữ liệu theo thời gian đón."""
        return df.sort_values(by='tpep_pickup_datetime')

    def get_completed_trips(self, df_sorted, simulated_now):
        """Lấy các bản ghi hoàn thành trong khoảng thời gian 15 phút trước simulated_now."""
        simulated_now = pd.to_datetime(simulated_now)
        return df_sorted[(df_sorted['tpep_dropoff_datetime'] <= simulated_now) & 
                         (df_sorted['tpep_dropoff_datetime'] > simulated_now - timedelta(minutes=15))]


class KafkaDataProducer:
    def __init__(self):
        """Khởi tạo Kafka Producer."""
        self.kafka_broker = KAFKA_BROKER_URL
        self.producer = self.initialize_kafka_producer()
        self.setup_logging()
        self.logger = logging.getLogger(__name__)

    def setup_logging(self):
        """Thiết lập cấu hình logging."""
        logging.basicConfig(
            level=logging.INFO,  
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'  
        )

    def initialize_kafka_producer(self):
        """Khởi tạo Kafka Producer."""
        return KafkaProducer(**PRODUCER_CONFIG, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def send_data(self, topic, batch):
        """Gửi dữ liệu đến Kafka topic."""
        for record in batch:
            record_to_send = {
                'VendorID': record['VendorID'],
                'tpep_pickup_datetime': record['tpep_pickup_datetime'].isoformat(),
                'tpep_dropoff_datetime': record['tpep_dropoff_datetime'].isoformat(),
                'passenger_count': record['passenger_count'],
                'trip_distance': record['trip_distance'],
                'RatecodeID': record['RatecodeID'],
                'store_and_fwd_flag': record['store_and_fwd_flag'],
                'PULocationID': record['PULocationID'],
                'DOLocationID': record['DOLocationID'],
                'payment_type': record['payment_type'],
                'fare_amount': record['fare_amount'],
                'extra': record['extra'],
                'mta_tax': record['mta_tax'],
                'tip_amount': record['tip_amount'],
                'tolls_amount': record['tolls_amount'],
                'improvement_surcharge': record['improvement_surcharge'],
                'total_amount': record['total_amount'],
                'congestion_surcharge': record['congestion_surcharge'],
                'Airport_fee': record['Airport_fee']
            }

            # Gửi đến Kafka topic
            self.producer.send(topic=topic, value=record_to_send)
            self.logger.info(f"{topic}: {record_to_send}")


def main():
    # Thiết lập logging cho main function
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    parquet_file_path = os.path.join(base_dir, "../data/yellow_tripdata_2024-07.parquet")
    data_processor = TaxiDataGenerator(parquet_file_path)
    data_producer = KafkaDataProducer()
    topic_manager = KafkaTopicManager()
    topic_manager.create_topic('trip_data')

    # Đọc file Parquet chỉ từ tháng 07/2024
    df = data_processor.read_parquet_data()

    # Lọc các bản ghi có pickup datetime thuộc tháng 07/2024
    df_filtered = data_processor.filter_data()

    # Sắp xếp dữ liệu theo thời gian đón
    df_sorted = data_processor.sort_data(df_filtered)

    # Mô phỏng thời gian hiện tại
    simulated_now = datetime(2024, 7, 31, 23, 59, 59)  # Thay đổi thời gian theo nhu cầu

    # Gửi dữ liệu mỗi 5 giây
    while True:
        # Lấy các bản ghi đã hoàn thành trong 15 phút trước simulated_now
        completed_trips = data_processor.get_completed_trips(df_sorted, simulated_now)
        
        if not completed_trips.empty:
            data_producer.send_data('trip_data', completed_trips.to_dict(orient='records'))
        else:
            logger.info("Không có record nào!")

        simulated_now += timedelta(minutes=15)
        time.sleep(5)


if __name__ == "__main__":
    main()

# Streaming Data Analysis

## Yêu cầu

- **Python**: 3.9.x trở lên
- **Docker**: (đảm bảo đã cài đặt Docker)

## Quick Start

**1. Cài đặt các thư viện cần thiết:**
```bash
   pip install -r requirements.txt
```

**2. Chạy Infratructure:**
```bash
    make run
```
**3. Gen data**
```bash
    python .\src\gen_data.py
```

**3. Analysis**
```bash
    python .\src\analysis.py
```

### Port

| Ứng dụng              | Cổng  |
|-----------------------|-------|
| **Kafka Control Center**   | 9021  |
| **Influxdb**   | 8086  |
| **Grafana**   | 3000  |

### Data: [TLC trip](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
# StreamFlow

- **Real-time processing:** Optimized for processing data as soon as it's created.
- **Easy configuration:** Use YAML/JSON files to set up data flows.
- **Extensibility:** Easily integrate additional processing modules or connect to new databases.
- **Automation:** Built-in scripts support rapid deployment.

## Operating system
- Linux: Ubuntu, Debian.
- I will expand and make further adjustments for Windows and macOS at a later time.

## Technologies Used

- **Main Language:** Python, C.
- **Frameworks/Libraries:** Numpy, Selenium, BeutifulSoup, Requests, Pandas, Kafka, Docker, Spark, Flink.
- **Databases:** PostgreSQL.

## Setting

1. Clone repository
```bash
git clone https://github.com/bazzi24/streamflow.git
cd streamflow
```

2. Install dependent libraries
- Python >= 3.9.x
- uv >= 0.5.x
- Docker >= 24.0.0 & Docker Compose >= v2.26.1

If you use uv
```bash
uv venv
source .venv/bin/activate
uv sync
```
If you use pip
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

3. Build Docker images
```bash
docker compose up -d
```

## Lauch service Kafka
To access Kafka UI, enter localhost:8080 in your browser. 
You can change port for Kafka UI in docker-compse.yml:
```bash
kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: "local"
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: "kafka:9092"
    depends_on:
      - kafka
```

## Edit .env
Remove the .example extension and change the variables. You can register for the SSI API from https://www.ssi.com.vn/
Then change the variables consumerID and consumerSecret in .env

## Run
Run Kafka Producer
```bash
./orchestration.sh
```

Run Kafka Consumer & push to PSQL
```bash
./orchestration_consumer.sh
```

## Contributing
Streamflow will thrive with open-source collaboration. In that spirit, I welcome diverse contributions from the community. If you would like to participate, please contact me first.





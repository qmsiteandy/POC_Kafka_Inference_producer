# Kafka Docker Compose Setup

This Docker Compose configuration sets up a complete Kafka environment with:
- **Zookeeper**: Coordination service for Kafka
- **Kafka Broker**: The main Kafka server
- **Kafka UI**: Web interface for managing Kafka topics and messages

## Services

### Zookeeper
- **Port**: 2181
- **Image**: confluentinc/cp-zookeeper:7.4.0
- **Purpose**: Manages Kafka cluster metadata

### Kafka Broker
- **Port**: 9092 (external), 29092 (internal)
- **Image**: confluentinc/cp-kafka:7.4.0
- **JMX Port**: 9101

### Kafka UI
- **Port**: 8080
- **Image**: provectuslabs/kafka-ui:latest
- **Purpose**: Web interface for Kafka management

## Usage

### Start the services
```bash
docker-compose up -d
```

### Check service status
```bash
docker-compose ps
```

### View logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f kafka
```

### Stop the services
```bash
docker-compose down
```

### Stop and remove volumes (clean reset)
```bash
docker-compose down -v
```

## Accessing Services

- **Kafka Broker**: `localhost:9092`
- **Kafka UI**: http://localhost:8080
- **Zookeeper**: `localhost:2181`

## Testing Kafka

### Create a topic
```bash
docker exec -it kafka kafka-topics --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### List topics
```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Produce messages
```bash
docker exec -it kafka kafka-console-producer --topic test-topic --bootstrap-server localhost:9092
```

### Consume messages
```bash
docker exec -it kafka kafka-console-consumer --topic test-topic --from-beginning --bootstrap-server localhost:9092
```

## Configuration Notes

- **Replication Factor**: Set to 1 (suitable for development, increase for production)
- **Data Persistence**: Uses Docker volumes for data persistence
- **Network**: Uses a custom network `kafka-network`
- **JMX**: Enabled on port 9101 for monitoring

## Volumes

- `zookeeper-data`: Zookeeper data storage
- `zookeeper-logs`: Zookeeper transaction logs
- `kafka-data`: Kafka data storage

## Environment Variables

Key configuration is handled through environment variables. Check the `docker-compose.yml` file for detailed configuration options.

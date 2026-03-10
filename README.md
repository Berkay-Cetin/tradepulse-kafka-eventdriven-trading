# TradePulse 📈
### Real-Time Event-Driven Trading Platform

A production-grade, cloud-native trading platform built with **.NET 8** and **Apache Kafka**, demonstrating real-world microservices architecture patterns including **CQRS**, **Event Sourcing**, and **Kubernetes** orchestration.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         TRADEPULSE                                  │
│                                                                     │
│  ┌──────────────────────┐                                           │
│  │   MarketSimulator    │  Geometric Brownian Motion price engine   │
│  │   (.NET 8 Worker)    │─────────────────────────────────┐        │
│  └──────────────────────┘   market.price-feed (Kafka)     │        │
│                                                            ▼        │
│  ┌──────────────────────┐                    ┌──────────────────┐  │
│  │    OrderService      │  trade.orders      │  PricingService  │  │
│  │  (.NET 8 Web API)    │───────────────────►│  (.NET 8 Worker) │  │
│  │  CQRS + MediatR      │◄── REST/Swagger ───│  Kafka Consumer  │  │
│  └──────────────────────┘                    └────────┬─────────┘  │
│           │                                           │             │
│           │                      trade.executions (Kafka)          │
│           │                  ┌────────────────────────┤            │
│           │                  │                        │            │
│           ▼                  ▼                        ▼            │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐ │
│  │   PostgreSQL     │  │ EventStoreService│  │  Redis Cache     │ │
│  │   Write DB       │  │  Event Sourcing  │  │  Read Model      │ │
│  │   CQRS Write     │  │  Append-only log │  │  Price Feed      │ │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘

Kubernetes (Minikube)
├── 4x Deployments
├── HorizontalPodAutoscaler (PricingService: 2-6, OrderService: 2-4)
├── ConfigMap + Secrets
└── Helm Chart
```

---

## Kafka Topic Design

| Topic | Partitions | Producer | Consumers | Retention |
|---|---|---|---|---|
| `market.price-feed` | 6 | MarketSimulator | PricingService, ProjectionService | 1 hour |
| `trade.orders` | 3 | OrderService | PricingService | 7 days |
| `trade.executions` | 3 | PricingService | EventStoreService, ReportingService | 7 days |
| `trade.dead-letter` | 1 | All services | Manual inspection | 30 days |
| `portfolio.snapshots` | 3 | ProjectionService | ReportingService | 24 hours |

> **Partition Key Strategy:** `market.price-feed` uses `symbol` as key — same stock always routes to same partition, guaranteeing ordering per symbol.

---

## Services

### 🏭 MarketSimulator
- Simulates real market price movements using **Geometric Brownian Motion (GBM)** — the same model used in quantitative finance
- Publishes price events to Kafka every 500ms
- Configurable volatility and drift parameters
- Kafka Producer with `Acks.All` + `EnableIdempotence` for exactly-once semantics

### 💹 PricingService
- Kafka Consumer with **manual offset commit** — processes message before acknowledging
- Updates **Redis Read Model** with latest prices
- Detects significant price movements (≥0.5%) and triggers trade executions
- Routes failed messages to **Dead Letter Queue (DLQ)**
- REST API: `GET /api/prices/{symbol}`

### 📋 OrderService
- Full **CQRS** implementation with **MediatR**
  - `PlaceOrderCommand` → writes to PostgreSQL → publishes Kafka event
  - `GetOrderQuery` → reads from PostgreSQL with `AsNoTracking()`
- **Optimistic Concurrency** via version token
- **Outbox Pattern** table for guaranteed Kafka delivery
- REST API with Swagger: `POST /api/orders`, `GET /api/orders/{id}`

### 🗄️ EventStoreService
- Implements **Event Sourcing** — every domain event is persisted as an immutable append-only record
- Consumes from multiple Kafka topics simultaneously (`trade.orders` + `trade.executions`)
- **Idempotent writes** — duplicate events are safely skipped via `EventId` check
- Full event replay capability for state reconstruction

---

## Tech Stack

| Category | Technology |
|---|---|
| **Runtime** | .NET 8, C# |
| **Messaging** | Apache Kafka (Confluent Platform 7.6) |
| **Database** | PostgreSQL 16 |
| **Cache** | Redis 7 |
| **ORM** | Entity Framework Core 8 + Npgsql |
| **Mediator** | MediatR 14 |
| **Logging** | Serilog |
| **Containers** | Docker, Docker Compose |
| **Orchestration** | Kubernetes (Minikube), Helm |
| **Autoscaling** | HorizontalPodAutoscaler |
| **Monitoring** | Kafka UI, Redis Commander |

---

## Patterns & Architecture

- ✅ **Event-Driven Microservices** — services communicate exclusively via Kafka events
- ✅ **CQRS** — strict read/write separation with dedicated models
- ✅ **Event Sourcing** — immutable append-only event log, full audit trail
- ✅ **Dead Letter Queue** — failed messages are captured for inspection and replay
- ✅ **Outbox Pattern** — guaranteed at-least-once Kafka delivery
- ✅ **Optimistic Concurrency** — version-based conflict detection
- ✅ **Consumer Group Load Balancing** — multiple PricingService replicas share partition load
- ✅ **Read Model / Projection** — Redis cache built from Kafka event stream

---

## Getting Started

### Prerequisites
- Docker & Docker Compose
- .NET 8 SDK
- Kubernetes (Minikube) + Helm *(for K8s deployment)*

### 1. Start Infrastructure

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/TradePulse-Kafka-EventDriven-Trading.git
cd TradePulse-Kafka-EventDriven-Trading

# Start Kafka, PostgreSQL, Redis
docker compose up -d

# Verify Kafka topics
docker exec tradepulse-kafka kafka-topics \
  --bootstrap-server localhost:9092 --list
```

### 2. Run Services Locally

Open 4 terminals:

```bash
# Terminal 1
cd src/MarketSimulator.Service && dotnet run

# Terminal 2
cd src/PricingService && dotnet run

# Terminal 3
cd src/OrderService && dotnet run

# Terminal 4
cd src/EventStoreService && dotnet run
```

### 3. Test the Platform

```bash
# Place an order (OrderService Swagger → http://localhost:PORT/swagger)
curl -X POST http://localhost:PORT/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "symbol": "AAPL",
    "orderType": "BUY",
    "quantity": 10,
    "price": 185.50
  }'

# Get latest price from Redis cache
curl http://localhost:PORT/api/prices/AAPL

# Check event store
docker exec -it tradepulse-postgres psql -U tradepulse -d tradepulse \
  -c 'SELECT "EventType", "AggregateType", "KafkaTopic", "OccurredAt" FROM event_store ORDER BY "Id" DESC LIMIT 10;'
```

### 4. Kubernetes Deployment (Helm)

```bash
# Start Minikube
minikube start --driver=docker --cpus=4 --memory=6144

# Configure values
cp helm/tradepulse/values.example.yaml helm/tradepulse/values.yaml
# Edit values.yaml — set your host IP (minikube ssh "ip route | grep default | awk '{print $3}'")

# Build images into Minikube
eval $(minikube docker-env)
docker build -t tradepulse/market-simulator:latest src/MarketSimulator.Service/
docker build -t tradepulse/pricing-service:latest src/PricingService/
docker build -t tradepulse/order-service:latest src/OrderService/
docker build -t tradepulse/event-store:latest src/EventStoreService/

# Deploy
minikube addons enable metrics-server
helm install tradepulse helm/tradepulse/

# Monitor
kubectl get pods -w
kubectl get hpa
```

---

## Monitoring

| Tool | URL | Description |
|---|---|---|
| **Kafka UI** | http://localhost:8080 | Topic browser, consumer group lag, message inspector |
| **Redis Commander** | http://localhost:8081 | Redis key browser, real-time price cache |
| **Swagger (OrderService)** | http://localhost:PORT/swagger | REST API playground |

---

## Project Structure

```
TradePulse/
├── src/
│   ├── MarketSimulator.Service/    # Kafka Producer — price feed
│   ├── PricingService/             # Kafka Consumer — price processing + Redis
│   ├── OrderService/               # CQRS Web API — order management
│   └── EventStoreService/          # Event Sourcing — append-only log
├── docker/
│   └── postgres-init.sql           # DB schema
├── helm/
│   └── tradepulse/                 # Helm Chart
│       ├── Chart.yaml
│       ├── values.example.yaml
│       └── templates/
│           ├── configmap.yaml
│           ├── market-simulator.yaml
│           ├── pricing-service.yaml
│           ├── order-service.yaml
│           ├── event-store.yaml
│           └── hpa.yaml
├── scripts/
│   ├── start.sh
│   ├── stop.sh
│   └── kafka-cli-guide.sh          # Kafka CLI learning guide
├── docker-compose.yml
└── README.md
```

---

## Key Outcomes

This project covers enterprise-grade concepts:

- **Kafka internals**: Topics, Partitions, Offsets, Consumer Groups, Rebalancing
- **Exactly-once delivery**: `EnableIdempotence` + `Acks.All` + manual commit
- **Event-driven design**: Loose coupling via events, no direct service-to-service calls
- **CQRS in practice**: Separate command/query models, MediatR pipeline
- **Event Sourcing**: Immutable log, state reconstruction from events
- **Kubernetes production patterns**: HPA, ConfigMap, rolling updates, health checks

---

<p align="center">
  Built with .NET 8 · Apache Kafka · PostgreSQL · Redis · Kubernetes
</p>
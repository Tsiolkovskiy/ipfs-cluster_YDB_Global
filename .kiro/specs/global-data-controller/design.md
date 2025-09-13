# Документ проектирования Global Data Controller

## Обзор

Global Data Controller (GDC) представляет собой централизованную систему управления, которая оркестрирует IPFS-кластеры в географически распределённых зонах. Система построена на микросервисной архитектуре с использованием событийно-ориентированного подхода и обеспечивает автоматизированное управление размещением данных, репликацией и балансировкой нагрузки.

### Ключевые принципы проектирования

1. **Разделение ответственности**: Policy Engine определяет "что нужно", Scheduler решает "где и как", Orchestrator гарантирует "доведение до конца"
2. **Идемпотентность**: все операции безопасны для повторного выполнения
3. **Наблюдаемость**: встроенная телеметрия и трассировка всех операций
4. **Безопасность**: сквозной mTLS с SPIFFE/SPIRE и авторизация через OPA/Rego
5. **Отказоустойчивость**: graceful degradation при сбоях компонентов или зон

## Архитектура

### Высокоуровневая архитектура

```
┌─────────────────────────────────────────────────────────────────┐
│                    Global Data Controller                        │
├─────────────────────────────────────────────────────────────────┤
│  API Gateway │ AuthN/AuthZ │ Policy Engine │ Scheduler          │
│  Orchestrator│ Shard Mgr   │ Event Bus     │ Cluster Adapter    │
│  Meta DB     │ Telemetry   │ Admin UI      │                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┼─────────┐
                    │                   │
            ┌───────▼────────┐  ┌───────▼────────┐
            │  Zone MSK      │  │  Zone NN       │
            │ ┌────────────┐ │  │ ┌────────────┐ │
            │ │Envoy GW    │ │  │ │Envoy GW    │ │
            │ │IPFS Cluster│ │  │ │IPFS Cluster│ │
            │ │Kubo + ZFS  │ │  │ │Kubo + ZFS  │ │
            │ │YDB State   │ │  │ │YDB State   │ │
            │ └────────────┘ │  │ └────────────┘ │
            └────────────────┘  └────────────────┘
```

### Компонентная архитектура GDC

#### 1. API Gateway
- **Назначение**: Единая точка входа для всех внешних запросов
- **Технологии**: gRPC/HTTP с поддержкой REST и GraphQL
- **Функции**:
  - Валидация запросов и версионирование API
  - Rate limiting и throttling
  - Маршрутизация к соответствующим сервисам
  - Агрегация ответов от нескольких сервисов

#### 2. Authentication & Authorization (AuthN/AuthZ)
- **Назначение**: Безопасность и контроль доступа
- **Технологии**: SPIFFE/SPIRE для идентификации, OPA/Rego для политик
- **Функции**:
  - Выдача и проверка SVID (SPIFFE Verifiable Identity Documents)
  - Применение политик доступа на уровне запросов и ресурсов
  - Аудит всех операций безопасности

#### 3. Policy Engine
- **Назначение**: Управление политиками репликации и размещения
- **Технологии**: OPA/Rego с git-подобным версионированием
- **Компоненты**:
  - **Policy Repository**: хранение и версионирование политик
  - **Policy Evaluator**: вычисление применимых политик для операций
- **Политики**:
  - Replication Factor (RF): количество реплик
  - Erasure Coding (EC): параметры кодирования с исправлением ошибок
  - Placement: ограничения размещения (зоны, типы узлов)
  - Tiering: правила многоуровневого хранения
  - Retention: политики хранения и удаления
  - Quota: лимиты на пользователей/проекты

#### 4. Scheduler
- **Назначение**: Интеллектуальное планирование размещения данных
- **Технологии**: Go с алгоритмами многокритериальной оптимизации
- **Входные данные**:
  - Топология зон и узлов
  - Метрики здоровья и производительности
  - Модель стоимости операций
  - Политики и ограничения
- **Алгоритмы**:
  - Geo-aware placement (учёт географии и задержек)
  - Cost-aware optimization (минимизация затрат)
  - Load balancing (равномерное распределение нагрузки)
  - Constraint satisfaction (соблюдение всех ограничений)

#### 5. Shard Manager
- **Назначение**: Управление шардированием и владением данными
- **Функции**:
  - Разбиение глобального пространства данных на шарды
  - Отслеживание владения шардами между зонами
  - Планирование и выполнение миграции шардов
  - Graceful draining узлов и зон

#### 6. Orchestrator
- **Назначение**: Управление долгосрочными операциями
- **Технологии**: Workflow engine с поддержкой саг и компенсаций
- **Функции**:
  - Разбиение сложных операций на этапы
  - Отслеживание прогресса выполнения
  - Retry logic с exponential backoff
  - Rollback и компенсирующие транзакции
- **Типы операций**:
  - Массовое закрепление (bulk pin)
  - Репликация между зонами
  - Ребалансировка кластеров
  - Миграция данных

#### 7. Event Bus
- **Назначение**: Асинхронная коммуникация между компонентами
- **Технологии**: NATS JetStream или Apache Kafka
- **События**:
  - `PinRequested`: запрос на закрепление контента
  - `PlanReady`: план размещения готов
  - `ExecProgress`: прогресс выполнения операций
  - `HealthChanged`: изменение состояния узлов/зон
  - `PolicyUpdated`: обновление политик
  - `AlertTriggered`: системные оповещения

#### 8. Cluster Control Adapter
- **Назначение**: Интеграция с IPFS Cluster
- **Технологии**: gRPC клиенты с mTLS
- **Функции**:
  - Отправка планов размещения в кластеры
  - Мониторинг статуса выполнения операций
  - Идемпотентные операции с safe retry
  - Адаптация к различным версиям IPFS Cluster API

#### 9. Global Meta Database
- **Назначение**: Хранение глобальных метаданных
- **Технологии**: Распределённая БД (PostgreSQL с Citus или CockroachDB)
- **Схема данных**:
  ```sql
  -- Зоны и топология
  CREATE TABLE zones (
    id UUID PRIMARY KEY,
    name VARCHAR NOT NULL,
    region VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
  );
  
  -- Кластеры IPFS
  CREATE TABLE clusters (
    id UUID PRIMARY KEY,
    zone_id UUID REFERENCES zones(id),
    endpoint VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    capabilities JSONB,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
  );
  
  -- Политики
  CREATE TABLE policies (
    id UUID PRIMARY KEY,
    name VARCHAR NOT NULL,
    version INTEGER NOT NULL,
    content JSONB NOT NULL,
    created_at TIMESTAMP,
    created_by VARCHAR
  );
  
  -- Шарды и владение
  CREATE TABLE shard_ownership (
    shard_id VARCHAR PRIMARY KEY,
    zone_id UUID REFERENCES zones(id),
    status VARCHAR NOT NULL,
    assigned_at TIMESTAMP,
    updated_at TIMESTAMP
  );
  ```

#### 10. Telemetry Store
- **Назначение**: Хранение метрик и логов для принятия решений
- **Технологии**: InfluxDB или Prometheus + Grafana
- **Метрики**:
  - Производительность: latency, throughput, IOPS
  - Ресурсы: CPU, память, дисковое пространство
  - Сеть: bandwidth, packet loss, RTT между зонами
  - Стоимость: storage cost, egress cost, compute cost
  - SLI/SLO: availability, durability, consistency

#### 11. Admin UI
- **Назначение**: Веб-интерфейс для администрирования
- **Технологии**: React/Vue.js с WebSocket для real-time обновлений
- **Функции**:
  - Управление политиками и их симуляция
  - Мониторинг состояния зон и кластеров
  - Просмотр планов размещения и их выполнения
  - Дашборды с метриками и алертами
  - Управление пользователями и правами доступа

## Компоненты и интерфейсы

### Внутренние API

#### Policy Engine API
```go
type PolicyEngine interface {
    // Управление политиками
    CreatePolicy(ctx context.Context, policy *Policy) error
    UpdatePolicy(ctx context.Context, id string, policy *Policy) error
    DeletePolicy(ctx context.Context, id string) error
    GetPolicy(ctx context.Context, id string) (*Policy, error)
    ListPolicies(ctx context.Context, filter *PolicyFilter) ([]*Policy, error)
    
    // Оценка политик
    EvaluatePolicies(ctx context.Context, request *PlacementRequest) (*PolicyResult, error)
    ValidatePolicy(ctx context.Context, policy *Policy) error
}

type Policy struct {
    ID          string            `json:"id"`
    Name        string            `json:"name"`
    Version     int               `json:"version"`
    Rules       map[string]string `json:"rules"`  // Rego rules
    Metadata    map[string]string `json:"metadata"`
    CreatedAt   time.Time         `json:"created_at"`
    CreatedBy   string            `json:"created_by"`
}
```

#### Scheduler API
```go
type Scheduler interface {
    // Планирование размещения
    ComputePlacement(ctx context.Context, request *PlacementRequest) (*PlacementPlan, error)
    OptimizePlacement(ctx context.Context, constraints *OptimizationConstraints) (*PlacementPlan, error)
    
    // Симуляция
    SimulatePlacement(ctx context.Context, request *PlacementRequest) (*SimulationResult, error)
    
    // Ребалансировка
    ComputeRebalance(ctx context.Context, target *RebalanceTarget) (*RebalancePlan, error)
}

type PlacementRequest struct {
    CID         string            `json:"cid"`
    Size        int64             `json:"size"`
    Policies    []string          `json:"policies"`
    Constraints map[string]string `json:"constraints"`
    Priority    int               `json:"priority"`
}

type PlacementPlan struct {
    ID          string              `json:"id"`
    Request     *PlacementRequest   `json:"request"`
    Assignments []*NodeAssignment   `json:"assignments"`
    Cost        *CostEstimate       `json:"cost"`
    CreatedAt   time.Time           `json:"created_at"`
}
```

#### Orchestrator API
```go
type Orchestrator interface {
    // Управление workflow
    StartWorkflow(ctx context.Context, workflow *WorkflowDefinition) (*WorkflowExecution, error)
    GetWorkflowStatus(ctx context.Context, id string) (*WorkflowStatus, error)
    CancelWorkflow(ctx context.Context, id string) error
    
    // Мониторинг
    ListActiveWorkflows(ctx context.Context) ([]*WorkflowExecution, error)
    GetWorkflowHistory(ctx context.Context, filter *HistoryFilter) ([]*WorkflowExecution, error)
}

type WorkflowDefinition struct {
    Type        string                 `json:"type"`
    Steps       []*WorkflowStep        `json:"steps"`
    Retries     int                    `json:"retries"`
    Timeout     time.Duration          `json:"timeout"`
    Metadata    map[string]interface{} `json:"metadata"`
}
```

### Внешние интеграции

#### IPFS Cluster Integration
```go
type ClusterControlAdapter interface {
    // Управление пинами
    SubmitPinPlan(ctx context.Context, clusterID string, plan *PinPlan) error
    GetPinStatus(ctx context.Context, clusterID string, cid string) (*PinStatus, error)
    
    // Мониторинг кластера
    GetClusterStatus(ctx context.Context, clusterID string) (*ClusterStatus, error)
    GetNodeMetrics(ctx context.Context, clusterID string) ([]*NodeMetrics, error)
    
    // Управление узлами
    AddNode(ctx context.Context, clusterID string, nodeConfig *NodeConfig) error
    RemoveNode(ctx context.Context, clusterID string, nodeID string) error
}
```

#### YDB Integration
```go
type MetadataStore interface {
    // Зоны и кластеры
    RegisterZone(ctx context.Context, zone *Zone) error
    RegisterCluster(ctx context.Context, cluster *Cluster) error
    GetZoneTopology(ctx context.Context) (*Topology, error)
    
    // Политики
    StorePolicyVersion(ctx context.Context, policy *Policy) error
    GetActivePolicies(ctx context.Context) ([]*Policy, error)
    
    // Шарды
    UpdateShardOwnership(ctx context.Context, assignments []*ShardAssignment) error
    GetShardDistribution(ctx context.Context) (*ShardDistribution, error)
}
```

## Модели данных

### Основные сущности

#### Zone (Зона)
```go
type Zone struct {
    ID          string            `json:"id"`
    Name        string            `json:"name"`
    Region      string            `json:"region"`
    Status      ZoneStatus        `json:"status"`
    Capabilities map[string]string `json:"capabilities"`
    Coordinates *GeoCoordinates   `json:"coordinates,omitempty"`
    CreatedAt   time.Time         `json:"created_at"`
    UpdatedAt   time.Time         `json:"updated_at"`
}

type ZoneStatus string
const (
    ZoneStatusActive     ZoneStatus = "active"
    ZoneStatusDegraded   ZoneStatus = "degraded"
    ZoneStatusMaintenance ZoneStatus = "maintenance"
    ZoneStatusOffline    ZoneStatus = "offline"
)
```

#### Cluster (Кластер IPFS)
```go
type Cluster struct {
    ID          string            `json:"id"`
    ZoneID      string            `json:"zone_id"`
    Name        string            `json:"name"`
    Endpoint    string            `json:"endpoint"`
    Status      ClusterStatus     `json:"status"`
    Version     string            `json:"version"`
    Nodes       []*Node           `json:"nodes"`
    Capabilities map[string]string `json:"capabilities"`
    CreatedAt   time.Time         `json:"created_at"`
    UpdatedAt   time.Time         `json:"updated_at"`
}

type Node struct {
    ID          string            `json:"id"`
    ClusterID   string            `json:"cluster_id"`
    Address     string            `json:"address"`
    Status      NodeStatus        `json:"status"`
    Resources   *NodeResources    `json:"resources"`
    Metrics     *NodeMetrics      `json:"metrics,omitempty"`
}
```

#### Content (Контент)
```go
type Content struct {
    CID         string            `json:"cid"`
    Size        int64             `json:"size"`
    Type        string            `json:"type"`
    Replicas    []*Replica        `json:"replicas"`
    Policies    []string          `json:"policies"`
    Status      ContentStatus     `json:"status"`
    CreatedAt   time.Time         `json:"created_at"`
    UpdatedAt   time.Time         `json:"updated_at"`
}

type Replica struct {
    NodeID      string            `json:"node_id"`
    ClusterID   string            `json:"cluster_id"`
    ZoneID      string            `json:"zone_id"`
    Status      ReplicaStatus     `json:"status"`
    CreatedAt   time.Time         `json:"created_at"`
    VerifiedAt  *time.Time        `json:"verified_at,omitempty"`
}
```

### IPLD Schema для глобального графа

```ipldsch
# Глобальный граф данных
type GlobalDataGraph struct {
    version String
    zones {String:Zone}
    content {String:Content}
    policies {String:Policy}
    topology Topology
    updated_at String  # ISO 8601 timestamp
}

type Zone struct {
    id String
    name String
    region String
    status String
    clusters {String:Cluster}
    coordinates optional GeoCoordinates
}

type Content struct {
    cid String
    size Int
    replicas [Replica]
    policies [String]
    status String
    metadata {String:Any}
}

type Replica struct {
    node_id String
    cluster_id String  
    zone_id String
    status String
    verified_at optional String
}
```

## Обработка ошибок

### Стратегии обработки ошибок

#### 1. Graceful Degradation
- При недоступности зоны: перенаправление трафика в здоровые зоны
- При сбое компонента: переключение на резервные экземпляры
- При превышении лимитов: throttling с информативными сообщениями

#### 2. Retry Strategies
```go
type RetryConfig struct {
    MaxAttempts     int           `json:"max_attempts"`
    InitialDelay    time.Duration `json:"initial_delay"`
    MaxDelay        time.Duration `json:"max_delay"`
    BackoffFactor   float64       `json:"backoff_factor"`
    Jitter          bool          `json:"jitter"`
}

// Exponential backoff с jitter
func (r *RetryConfig) NextDelay(attempt int) time.Duration {
    delay := r.InitialDelay * time.Duration(math.Pow(r.BackoffFactor, float64(attempt)))
    if delay > r.MaxDelay {
        delay = r.MaxDelay
    }
    if r.Jitter {
        delay = time.Duration(float64(delay) * (0.5 + rand.Float64()*0.5))
    }
    return delay
}
```

#### 3. Circuit Breaker Pattern
```go
type CircuitBreaker struct {
    maxFailures     int
    resetTimeout    time.Duration
    currentFailures int
    lastFailureTime time.Time
    state          CircuitState
}

type CircuitState int
const (
    CircuitClosed CircuitState = iota
    CircuitOpen
    CircuitHalfOpen
)
```

### Коды ошибок и их обработка

```go
type ErrorCode string
const (
    // Системные ошибки
    ErrInternalServer     ErrorCode = "INTERNAL_SERVER_ERROR"
    ErrServiceUnavailable ErrorCode = "SERVICE_UNAVAILABLE"
    ErrTimeout           ErrorCode = "TIMEOUT"
    
    // Ошибки валидации
    ErrInvalidRequest    ErrorCode = "INVALID_REQUEST"
    ErrInvalidPolicy     ErrorCode = "INVALID_POLICY"
    ErrConstraintViolation ErrorCode = "CONSTRAINT_VIOLATION"
    
    // Ошибки ресурсов
    ErrResourceNotFound  ErrorCode = "RESOURCE_NOT_FOUND"
    ErrResourceConflict  ErrorCode = "RESOURCE_CONFLICT"
    ErrQuotaExceeded     ErrorCode = "QUOTA_EXCEEDED"
    
    // Ошибки размещения
    ErrPlacementFailed   ErrorCode = "PLACEMENT_FAILED"
    ErrInsufficientNodes ErrorCode = "INSUFFICIENT_NODES"
    ErrZoneUnavailable   ErrorCode = "ZONE_UNAVAILABLE"
)

type Error struct {
    Code    ErrorCode `json:"code"`
    Message string    `json:"message"`
    Details map[string]interface{} `json:"details,omitempty"`
    Cause   error     `json:"-"`
}
```

## Стратегия тестирования

### 1. Unit Tests
- Тестирование отдельных компонентов в изоляции
- Mock-объекты для внешних зависимостей
- Покрытие критических алгоритмов планирования

### 2. Integration Tests
- Тестирование взаимодействия между компонентами
- Тестирование интеграции с IPFS Cluster и YDB
- Проверка событийных потоков через Event Bus

### 3. End-to-End Tests
- Полные сценарии от API до размещения данных
- Тестирование отказоустойчивости и восстановления
- Performance и load testing

### 4. Chaos Engineering
- Имитация сбоев зон и узлов
- Тестирование network partitions
- Проверка graceful degradation

### Тестовые сценарии

#### Сценарий 1: Базовое размещение контента
```go
func TestBasicContentPlacement(t *testing.T) {
    // 1. Создать тестовый кластер с 2 зонами
    // 2. Определить политику RF=2
    // 3. Отправить запрос на pin контента
    // 4. Проверить создание плана размещения
    // 5. Проверить выполнение плана в кластерах
    // 6. Проверить обновление глобального графа
}
```

#### Сценарий 2: Отказ зоны
```go
func TestZoneFailover(t *testing.T) {
    // 1. Создать контент с RF=3 в 3 зонах
    // 2. Имитировать отказ одной зоны
    // 3. Проверить перенаправление трафика
    // 4. Проверить автоматическую репликацию
    // 5. Проверить восстановление после возврата зоны
}
```

#### Сценарий 3: Ребалансировка
```go
func TestRebalancing(t *testing.T) {
    // 1. Создать неравномерно загруженный кластер
    // 2. Запустить ребалансировку
    // 3. Проверить перемещение данных
    // 4. Проверить сохранение доступности
    // 5. Проверить достижение целевого распределения
}
```
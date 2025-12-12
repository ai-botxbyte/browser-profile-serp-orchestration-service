# Demo A - Consumer Worker Job Architecture

## Mermaid Class Diagram

```mermaid
classDiagram
    %% Base Classes
    class BaseAppConsumer {
        <<abstract>>
        +queue_name: str
        +config: Any
        +connection: Optional[Connection]
        +channel: Optional[Channel]
        +queue: Optional[Queue]
        +__init__(queue_name, config)
        +connect()
        +disconnect()
        +start_consuming()*
    }

    class BaseAppWorker {
        <<abstract>>
        +queue_name: str
        +consumer: BaseAppConsumer
        +__init__(queue_name, consumer)
        +connect()
        +disconnect()
        +start_consuming()
        +process_message(message: AbstractIncomingMessage)
        +_execute_jobs(message: dict)*
    }

    class BaseAppJob {
        <<abstract>>
        +job_name: str
        +start_time: Optional[datetime]
        +__init__()
        +process_message(message: dict)*
        +execute(message: dict)
    }

    %% Demo A Consumer
    class DemoAConsumer {
        +__init__()
        +start_consuming()
    }

    %% Demo A Worker
    class DemoAWorker {
        +job_handlers: dict[str, BaseAppJob]
        +__init__()
        +_execute_jobs(message: dict)
    }

    %% Demo A Jobs
    class DemoA1Job {
        +__init__()
        +process_message(message: dict)
        -_validate_social_accounts(validation_data: dict)
        -_process_social_validation(validation_data: dict)
    }

    class DemoA2Job {
        +__init__()
        +process_message(message: dict)
        -_analyze_content(tagging_data: dict)
        -_generate_tags(analysis: dict)
        -_apply_tags(tagging_data: dict, tags: list)
    }

    %% Supporting Classes
    class DemoAOrchestrationService {
        +db: AsyncSession
        +validate_creation_data(data: dict)
        +validate_social_accounts(data: dict)
        -_validate_social_accounts(social_accounts: list)
    }

    class ConsumerDemoJobException {
        +queue_name: str
        +job_name: str
        +job_error: str
    }

    class JobDemoServiceException {
        +job_name: str
        +service_name: str
        +service_error: str
    }

    %% Inheritance Relationships
    BaseAppConsumer <|-- DemoAConsumer
    BaseAppWorker <|-- DemoAWorker
    BaseAppJob <|-- DemoA1Job
    BaseAppJob <|-- DemoA2Job

    %% Composition Relationships
    DemoAWorker *-- DemoAConsumer : uses for connection
    DemoAWorker *-- DemoA1Job : manages
    DemoAWorker *-- DemoA2Job : manages

    %% Dependency Relationships
    DemoAWorker ..> ConsumerDemoJobException : throws
    DemoA1Job ..> JobDemoServiceException : throws
    DemoA2Job ..> JobDemoServiceException : throws
```

## Flow Diagram

```mermaid
flowchart TD
    Start([Start Demo A Worker]) --> Init[Initialize DemoAWorker]
    Init --> CreateConsumer[Create DemoAConsumer]
    Init --> CreateJobs[Create Job Handlers<br/>- DemoA1Job job1<br/>- DemoA2Job job2]
    
    CreateConsumer --> Connect[Connect to RabbitMQ<br/>via consumer.connect]
    Connect --> DeclareQueue[Declare demo_A_queue<br/>and dead_letter queue]
    DeclareQueue --> StartConsuming[Start Consuming from demo_A_queue]
    
    StartConsuming --> ReceiveMsg[Receive Message from RabbitMQ]
    ReceiveMsg --> WorkerProcess[DemoAWorker<br/>process_message]
    
    WorkerProcess --> ParseJSON[Parse JSON message body]
    ParseJSON --> CheckJobType{Check job_type<br/>from message}
    
    CheckJobType -->|job1| ExecuteA1[Execute DemoA1Job.execute]
    CheckJobType -->|job2| ExecuteA2[Execute DemoA2Job.execute]
    CheckJobType -->|Unknown| JobError[Throw ConsumerDemoJobException]
    
    ExecuteA1 --> A1Process[DemoA1Job.process_message]
    A1Process --> A1Step1[Step 1: _validate_social_accounts]
    A1Step1 --> A1Step2[Step 2: _process_social_validation]
    A1Step2 --> A1Complete[Job A1 Complete]
    
    ExecuteA2 --> A2Process[DemoA2Job.process_message]
    A2Process --> A2Step1[Step 1: _analyze_content]
    A2Step1 --> A2Step2[Step 2: _generate_tags]
    A2Step2 --> A2Step3[Step 3: _apply_tags]
    A2Step3 --> A2Complete[Job A2 Complete]
    
    A1Complete --> AckMsg[Acknowledge Message<br/>via message.process]
    A2Complete --> AckMsg
    JobError --> AckMsg
    
    AckMsg --> ReceiveMsg
    
    style ExecuteA1 fill:#fff4e6
    style ExecuteA2 fill:#fff4e6
    style WorkerProcess fill:#e1f5ff
```

## Sequence Diagram

```mermaid
sequenceDiagram
    participant RMQ as RabbitMQ<br/>demo_A_queue
    participant Worker as DemoAWorker
    participant Consumer as DemoAConsumer
    participant Job as DemoA1Job/DemoA2Job

    Note over Worker,Consumer: Initialization Phase
    Worker->>Consumer: Create DemoAConsumer()
    Consumer->>RMQ: connect() - Establish connection
    Consumer->>RMQ: Declare queue and dead_letter queue
    Worker->>Worker: Initialize job_handlers<br/>{'job1': DemoA1Job, 'job2': DemoA2Job}
    
    Note over Worker,RMQ: Message Consumption Phase
    RMQ->>Worker: Message Received (AbstractIncomingMessage)
    Worker->>Worker: process_message(message)
    Worker->>Worker: Parse JSON from message.body
    Worker->>Worker: Extract job_type from message
    
    alt job_type = 'job1'
        Worker->>Job: Execute DemoA1Job.execute(message)
        Job->>Job: process_message(message)
        Job->>Job: _validate_social_accounts(data)
        Job->>Job: _process_social_validation(data)
        Job-->>Worker: Job Completed Successfully
    else job_type = 'job2'
        Worker->>Job: Execute DemoA2Job.execute(message)
        Job->>Job: process_message(message)
        Job->>Job: _analyze_content(data)
        Job->>Job: _generate_tags(analysis)
        Job->>Job: _apply_tags(data, tags)
        Job-->>Worker: Job Completed Successfully
    else Unknown job_type
        Worker->>Worker: Raise ConsumerDemoJobException
    end
    
    alt Success
        Worker->>RMQ: Acknowledge Message (via message.process)
    else Error
        Worker->>RMQ: Reject Message (via exception in message.process)
        Note over RMQ: Message sent to dead_letter queue
    end
```

## Component Overview

### 1. **Base Classes**

#### BaseAppConsumer
- Abstract base class for all consumers
- Handles RabbitMQ connection management using `aio_pika`
- Provides queue declaration (main queue + dead letter queue)
- Manages connection lifecycle (connect/disconnect)
- Sets QoS with prefetch_count=1
- Declares queues with priority support (x-max-priority: 10)

#### BaseAppWorker
- Abstract base class for all workers
- Uses consumer for RabbitMQ connection management
- Processes messages from queue
- Parses JSON message bodies
- Routes to job handlers via abstract `_execute_jobs()` method
- Handles message acknowledgment via `async with message.process()`

#### BaseAppJob
- Abstract base class for all jobs
- Provides `execute()` wrapper method with error handling
- Tracks job execution start time
- Logs job execution lifecycle
- Re-raises errors for dead letter queue handling
- Requires subclasses to implement `process_message()` method

### 2. **Demo A Components**

#### DemoAConsumer
- **Purpose**: Manages RabbitMQ connection and queue setup for `demo_A_queue`
- **Functions**:
  - `__init__()`: Initialize consumer with queue name "demo_A_queue" and config
  - `start_consuming()`: Start consuming messages (keeps worker running)
- **Note**: Consumer only handles connection management. Message processing is done by DemoAWorker.

#### DemoAWorker
- **Purpose**: Orchestrates message processing and job execution
- **Functions**:
  - `__init__()`: Initialize with DemoAConsumer and job handlers dictionary
  - `process_message(message)`: Parse JSON message and execute jobs
  - `_execute_jobs(message)`: Route to appropriate job handler based on `job_type`
- **Job Handlers**:
  - `'job1'`: Routes to `DemoA1Job` instance
  - `'job2'`: Routes to `DemoA2Job` instance

#### DemoA1Job (Social Validation)
- **Purpose**: Handle `job_type='job1'` - Social validation workflow
- **Functions**:
  - `execute(message)`: Wrapper method with error handling and metrics
  - `process_message(message)`: Main processing logic
  - `_validate_social_accounts(validation_data)`: Validate social accounts (testing implementation)
  - `_process_social_validation(validation_data)`: Process validation results (testing implementation)
- **Error Handling**: Wraps errors in `JobDemoServiceException`

#### DemoA2Job (Auto Tagging)
- **Purpose**: Handle `job_type='job2'` - Auto tagging workflow
- **Functions**:
  - `execute(message)`: Wrapper method with error handling and metrics
  - `process_message(message)`: Main processing logic
  - `_analyze_content(tagging_data)`: Analyze content for tags (testing implementation)
  - `_generate_tags(analysis)`: Generate tags from analysis (testing implementation)
  - `_apply_tags(tagging_data, tags)`: Apply generated tags (testing implementation)
- **Error Handling**: Wraps errors in `JobDemoServiceException`

#### DemoAOrchestrationService
- **Purpose**: Service layer for Demo A orchestration operations
- **Functions**:
  - `validate_creation_data(data)`: Validates creation data including social accounts, user_id, workspace_id, name, description, age, progress
  - `validate_social_accounts(data)`: Validates social accounts data structure
  - `_validate_social_accounts(social_accounts)`: Private method for detailed social account validation (platform, username, url, followers, verified)
- **Note**: Currently jobs use test implementations, but service is available for future integration

### 3. **Data Flow**

1. **Initialization**: 
   - `DemoAWorker` is created, which creates a `DemoAConsumer` instance
   - Consumer connects to RabbitMQ and declares `demo_A_queue` and `demo_A_queue_dead_letter`
   - Worker initializes job handlers: `{'job1': DemoA1Job(), 'job2': DemoA2Job()}`

2. **Message Reception**: 
   - Worker receives `AbstractIncomingMessage` from RabbitMQ queue
   - Message is processed via `process_message()` method

3. **Message Parsing**: 
   - Worker parses JSON from `message.body.decode()`
   - Extracts `job_type` from message dictionary

4. **Job Routing**: 
   - Worker looks up job handler in `job_handlers` dictionary using `job_type`
   - If job_type not found, raises `ConsumerDemoJobException`

5. **Job Execution**: 
   - Worker calls `job_handler.execute(message)` 
   - Job's `execute()` method calls `process_message()` with error handling
   - Job performs its specific workflow steps

6. **Message Acknowledgment**: 
   - On success: Message is automatically acknowledged via `async with message.process()`
   - On error: Exception is raised, message is rejected and sent to dead letter queue

### 4. **Error Handling**

- **JSON Decode Errors**: Caught in `process_message()`, logged and re-raised (message goes to dead letter queue)
- **Job Execution Errors**: 
  - Caught in job's `execute()` method
  - Wrapped in `JobDemoServiceException` by individual jobs
  - Re-raised to worker level
- **Unknown Job Type**: Raises `ConsumerDemoJobException` in worker's `_execute_jobs()`
- **All Errors**: Re-raised for debugging, causing message to be rejected and sent to `demo_A_queue_dead_letter`

### 5. **Queue Configuration**

- **Main Queue**: `demo_A_queue`
  - Durable: `True`
  - Auto-delete: `False`
  - Priority support: `x-max-priority: 10`
- **Dead Letter Queue**: `demo_A_queue_dead_letter`
  - Durable: `True`
  - Auto-delete: `False`
  - Priority support: `x-max-priority: 10`
- **QoS**: Prefetch count set to 1 (process one message at a time)

### 6. **Running the Worker**

The worker is run as a standalone service via `run_demo_A_worker.py`:
- Creates `DemoAWorker` instance
- Connects to RabbitMQ
- Starts consuming messages
- Runs indefinitely until interrupted

# Demo A - Consumer Worker Job Architecture

## Mermaid Class Diagram

```mermaid
classDiagram
    %% Base Classes
    class BaseAppConsumer {
        <<abstract>>
        +queue_name: str
        +config: Any
        +connection: Connection
        +channel: Channel
        +queue: Queue
        +consumer_tag: str
        +__init__(queue_name, config)
        +connect()
        +disconnect()
        +start_consuming()*
    }

    class BaseAppWorker {
        <<abstract>>
        +queue_name: str
        +consumer: BaseAppConsumer
        +connection: Connection
        +channel: Channel
        +__init__(queue_name, consumer)
        +connect()
        +disconnect()
        +start_consuming()
        +process_message(message)
        +_execute_jobs(message)*
        +_send_job_status(trace_id, job_name, job_completed, error_message, job_status)
    }

    class BaseAppJob {
        <<abstract>>
        +job_name: str
        +start_time: datetime
        +__init__()
        +process_message(message)*
        +execute(message)
    }

    %% Demo A Consumer
    class DemoAConsumer {
        +__init__()
        +start_consuming()
        +process_message(message)
        +validate_message(message_data)
    }

    %% Demo A Worker
    class DemoAWorker {
        +job_handlers: Dict
        +__init__()
        +_execute_jobs(message)
    }

    %% Demo A Jobs
    class DemoA1Job {
        +__init__()
        +process_message(message)
        -_validate_social_accounts(validation_data, trace_id)
        -_process_social_validation(validation_data, trace_id)
    }

    class DemoA2Job {
        +__init__()
        +process_message(message)
        -_analyze_content(tagging_data, trace_id)
        -_generate_tags(analysis, trace_id)
        -_apply_tags(tagging_data, tags, trace_id)
    }

    %% Supporting Classes
    class DemoOrchestrationMessageSchema {
        +trace_id: str
        +job_type: str
        +data: dict
    }

    class DemoAOrchestrationService {
        +db: Any
        +validate_social_accounts(validation_data)
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
    DemoAWorker *-- DemoAConsumer : uses for validation
    DemoAWorker *-- DemoA1Job : manages
    DemoAWorker *-- DemoA2Job : manages

    %% Dependency Relationships
    DemoAConsumer ..> DemoOrchestrationMessageSchema : validates
    DemoAWorker ..> DemoOrchestrationMessageSchema : processes
    DemoA1Job ..> DemoOrchestrationMessageSchema : consumes
    DemoA2Job ..> DemoOrchestrationMessageSchema : consumes
    DemoA1Job ..> DemoAOrchestrationService : uses
    DemoA1Job ..> JobDemoServiceException : throws
    DemoA2Job ..> JobDemoServiceException : throws
    DemoAWorker ..> ConsumerDemoJobException : throws
```

## Flow Diagram

```mermaid
flowchart TD
    Start([Start Demo A Worker]) --> Init[Initialize DemoAWorker]
    Init --> CreateConsumer[Create DemoAConsumer]
    Init --> CreateJobs[Create Job Handlers<br/>- DemoA1Job job1<br/>- DemoA2Job job2]
    
    CreateConsumer --> Connect[Connect to RabbitMQ]
    Connect --> StartConsuming[Start Consuming from demo_A_queue]
    
    StartConsuming --> ReceiveMsg[Receive Message]
    ReceiveMsg --> ValidateMsg{DemoAConsumer<br/>validate_message}
    
    ValidateMsg -->|Invalid| LogError[Log Validation Error]
    ValidateMsg -->|Valid| WorkerProcess[DemoAWorker<br/>process_message]
    
    WorkerProcess --> CheckJobType{Check job_type}
    
    CheckJobType -->|job1| ExecuteA1[Execute DemoA1Job]
    CheckJobType -->|job2| ExecuteA2[Execute DemoA2Job]
    CheckJobType -->|Unknown| JobError[Throw ConsumerDemoJobException]
    
    ExecuteA1 --> A1Step1[Step 1: _validate_social_accounts]
    A1Step1 --> A1Step2[Step 2: _process_social_validation]
    A1Step2 --> A1Complete[Job A1 Complete]
    
    ExecuteA2 --> A2Step1[Step 1: _analyze_content]
    A2Step1 --> A2Step2[Step 2: _generate_tags]
    A2Step2 --> A2Step3[Step 3: _apply_tags]
    A2Step3 --> A2Complete[Job A2 Complete]
    
    A1Complete --> SendStatus[_send_job_status<br/>to data_orchestration_queue]
    A2Complete --> SendStatus
    JobError --> SendStatus
    
    SendStatus --> AckMsg[Acknowledge Message]
    LogError --> AckMsg
    
    AckMsg --> ReceiveMsg
    
    style ValidateMsg fill:#e1f5ff
    style ExecuteA1 fill:#fff4e6
    style ExecuteA2 fill:#fff4e6
    style SendStatus fill:#e8f5e9
```

## Sequence Diagram

```mermaid
sequenceDiagram
    participant RMQ as RabbitMQ<br/>demo_A_queue
    participant Worker as DemoAWorker
    participant Consumer as DemoAConsumer
    participant Schema as DemoOrchestrationMessageSchema
    participant Job as DemoA1Job/DemoA2Job
    participant Service as DemoAOrchestrationService
    participant DataQueue as data_orchestration_queue

    RMQ->>Worker: Message Received
    Worker->>Consumer: validate_message(message_data)
    Consumer->>Schema: Validate using Pydantic
    Schema-->>Consumer: Validated Message
    Consumer-->>Worker: DemoOrchestrationMessageSchema
    
    Worker->>Worker: Get job_handler by job_type
    
    alt job_type = 'job1'
        Worker->>Job: Execute DemoA1Job
        Job->>Service: validate_social_accounts()
        Service-->>Job: Validation Result
        Job->>Job: _process_social_validation()
        Job-->>Worker: Job Completed
    else job_type = 'job2'
        Worker->>Job: Execute DemoA2Job
        Job->>Job: _analyze_content()
        Job->>Job: _generate_tags()
        Job->>Job: _apply_tags()
        Job-->>Worker: Job Completed
    else Unknown job_type
        Worker->>Worker: Throw ConsumerDemoJobException
    end
    
    Worker->>DataQueue: _send_job_status(trace_id, job_name, status)
    Worker->>RMQ: Acknowledge Message
```

## Component Overview

### 1. **Base Classes**

#### BaseAppConsumer
- Abstract base class for all consumers
- Handles RabbitMQ connection management
- Provides queue declaration and connection lifecycle

#### BaseAppWorker
- Abstract base class for all workers
- Orchestrates consumer validation and job execution
- Manages message processing workflow

#### BaseAppJob
- Abstract base class for all jobs
- Provides execution framework with error handling
- Tracks job execution metrics

### 2. **Demo A Components**

#### DemoAConsumer
- **Purpose**: Validates messages from `demo_A_queue`
- **Functions**:
  - `__init__()`: Initialize consumer with queue name
  - `start_consuming()`: Start consuming messages
  - `process_message(message)`: Process incoming messages
  - `validate_message(message_data)`: Validate using schema

#### DemoAWorker
- **Purpose**: Orchestrates message validation and job execution
- **Functions**:
  - `__init__()`: Initialize with consumer and job handlers
  - `_execute_jobs(message)`: Route to appropriate job based on job_type

#### DemoA1Job (Social Validation)
- **Purpose**: Handle job_type='job1' - Social validation workflow
- **Functions**:
  - `process_message(message)`: Main processing logic
  - `_validate_social_accounts()`: Validate social accounts
  - `_process_social_validation()`: Process validation results

#### DemoA2Job (Auto Tagging)
- **Purpose**: Handle job_type='job2' - Auto tagging workflow
- **Functions**:
  - `process_message(message)`: Main processing logic
  - `_analyze_content()`: Analyze content for tags
  - `_generate_tags()`: Generate tags from analysis
  - `_apply_tags()`: Apply generated tags

### 3. **Data Flow**

1. **Message Reception**: Worker receives message from `demo_A_queue`
2. **Validation**: DemoAConsumer validates message using DemoOrchestrationMessageSchema
3. **Routing**: DemoAWorker routes to job handler based on `job_type`
4. **Execution**: Selected job (DemoA1Job or DemoA2Job) executes
5. **Status Reporting**: Worker sends job status to `data_orchestration_queue`
6. **Acknowledgment**: Message is acknowledged in RabbitMQ

### 4. **Error Handling**

- **Validation Errors**: Caught by DemoAConsumer, message logged and skipped
- **Job Errors**: Wrapped in `JobDemoServiceException` by individual jobs
- **Worker Errors**: Wrapped in `ConsumerDemoJobException` by worker
- **Final Status**: All errors reported to `data_orchestration_queue`

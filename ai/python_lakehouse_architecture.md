# Python-First Lakehouse Architecture
## High-Level Design for Small Data Teams

---

## Table of Contents

- [Overview](#overview)
- [Architecture Diagram](#architecture-diagram)
- [Component Breakdown](#component-breakdown)
  - [1. Storage Layer: SeaweedFS](#1-storage-layer-seaweedfs)
  - [2. Table Format: Apache Iceberg](#2-table-format-apache-iceberg)
  - [3. Catalog Layer: Apache Polaris](#3-catalog-layer-apache-polaris)
  - [4. Query Engine: DuckDB](#4-query-engine-duckdb)
  - [5. Ingestion Layer: dlt + Airflow](#5-ingestion-layer-dlt--airflow)
  - [6. Transformation Layer: dbt + dbt-duckdb](#6-transformation-layer-dbt--dbt-duckdb)
  - [7. Visualization Layer: Streamlit + Plotly Dash](#7-visualization-layer-streamlit--plotly-dash)
  - [8. AI/RAG Integration Layer](#8-airag-integration-layer)
  - [9. Orchestration Layer: Apache Airflow](#9-orchestration-layer-apache-airflow)
- [Simplified Governance Strategy](#simplified-governance-strategy)
  - [Layer 1: Catalog (Polaris RBAC)](#layer-1-catalog-polaris-rbac)
  - [Layer 2: Query (DuckDB Users)](#layer-2-query-duckdb-users)
  - [Layer 3: Application (Python Code)](#layer-3-application-python-code)
  - [Layer 4: Data Quality (dbt Tests)](#layer-4-data-quality-dbt-tests)
  - [Layer 5: Lineage (dbt + Airflow)](#layer-5-lineage-dbt--airflow)
  - [What We're NOT Using](#what-were-not-using)
  - [OpenMetadata (Optional Discovery Tool)](#openmetadata-optional-discovery-tool)
- [Data Flow](#data-flow)
  - [Ingestion Flow](#ingestion-flow)
  - [Transformation Flow](#transformation-flow)
  - [Query Flow](#query-flow)
  - [RAG Flow](#rag-flow)
- [Development Workflow](#development-workflow)
  - [Environment Isolation (Polaris Namespaces)](#environment-isolation-polaris-namespaces)
  - [Local Development](#local-development)
  - [Staging Deployment](#staging-deployment)
  - [Production Deployment](#production-deployment)
  - [Schema Changes](#schema-changes)
- [Deployment Patterns](#deployment-patterns)
  - [Docker Compose (Development & Small Production)](#docker-compose-development--small-production)
  - [Kubernetes (Larger Scale)](#kubernetes-larger-scale)
- [Technology Summary](#technology-summary)
  - [Pure Python Components (90% of stack)](#pure-python-components-90-of-stack)
  - [Non-Python (with justification)](#non-python-with-justification)
  - [Why This Works](#why-this-works)
- [Scaling Considerations](#scaling-considerations)
  - [When This Stack Works](#when-this-stack-works)
  - [When to Evolve](#when-to-evolve)
  - [Migration Path](#migration-path)
- [Why Python-First Matters](#why-python-first-matters)
  - [Operational Benefits](#operational-benefits)
  - [Developer Productivity](#developer-productivity)
  - [Cost Efficiency](#cost-efficiency)
  - [Trade-offs Accepted](#trade-offs-accepted)
- [Best Practices](#best-practices)
  - [Data Modeling](#data-modeling)
  - [Performance](#performance)
  - [Security](#security)
  - [Monitoring](#monitoring)
  - [Cost Management](#cost-management)
- [Conclusion](#conclusion)
- [MVP Implementation Guide: Docker on Windows](#mvp-implementation-guide-docker-on-windows)
  - [MVP Goals](#mvp-goals)
  - [Phase 1: Windows Docker Setup](#phase-1-windows-docker-setup)
  - [Phase 2: Docker Compose Configuration](#phase-2-docker-compose-configuration)
  - [Phase 3: Sample Data and Initial Setup](#phase-3-sample-data-and-initial-setup)
  - [Phase 4: Data Pipeline Implementation](#phase-4-data-pipeline-implementation)
  - [Phase 5: Visualization Layer](#phase-5-visualization-layer)
  - [Phase 6: Smoke Tests](#phase-6-smoke-tests)
  - [Phase 7: Troubleshooting Guide](#phase-7-troubleshooting-guide)
  - [Phase 8: Next Steps After MVP](#phase-8-next-steps-after-mvp)
  - [Phase 9: Performance Optimization](#phase-9-performance-optimization)
  - [Phase 10: MVP Success Checklist](#phase-10-mvp-success-checklist)
- [Conclusion: MVP to Production Path](#conclusion-mvp-to-production-path)

---

## Overview

A radically simplified lakehouse architecture optimized for small teams (2-5 people) with Python-first tooling, batch processing, and minimal operational complexity.

**Design Philosophy:**
- **Python-first**: 90% Python, minimize JVM dependencies
- **Small scale optimized**: <2TB data, 5-10 concurrent users
- **Operational simplicity**: Fewer tools, less complexity
- **Batch-oriented**: No streaming overhead
- **Built-in governance**: Use native tool features, no dedicated governance layer

**Target Audience:**
- Small data teams (2-5 engineers/analysts)
- Strong Python skills, limited Java/DevOps expertise
- 100GB-2TB data volume
- Batch analytics workloads
- Budget-conscious deployments

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    Data Sources                                  │
│              APIs │ Databases │ Files │ SaaS                     │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                 Ingestion Layer (Python)                         │
│                   dlt + Apache Airflow                           │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              Transformation Layer (Python)                       │
│                  dbt Core + dbt-duckdb                           │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                 Table Format (Python)                            │
│            Apache Iceberg (via PyIceberg)                        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                  Storage Layer (S3 API)                          │
│                      SeaweedFS                                   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              Catalog Layer (REST API)                            │
│                  Apache Polaris                                  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              Query & Analytics (Python)                          │
│                       DuckDB                                     │
└─────────────────────────────────────────────────────────────────┘
                              ↓
        ┌───────────────────────┴──────────────────────┐
        ↓                                               ↓
┌──────────────────────┐                  ┌────────────────────────┐
│  Visualization       │                  │  AI/RAG Integration    │
│  Streamlit + Dash    │                  │  LangChain + ChromaDB  │
└──────────────────────┘                  └────────────────────────┘
```

---

## Component Breakdown

### 1. Storage Layer: SeaweedFS

**What It Is**: Distributed object storage with S3-compatible API

**Why SeaweedFS**:
- Open source (Apache 2.0), no licensing concerns
- S3 API compatibility (works with all Iceberg tools)
- Efficient storage for both large files and metadata
- Simple deployment (fewer moving parts than Ceph/MinIO)
- Good performance for analytical workloads

**Language**: Go (but exposed via S3 REST API)

**Role in Stack**:
- Stores Iceberg data files (Parquet)
- Stores Iceberg metadata files
- Persistent storage for ChromaDB vectors
- Can be replaced with AWS S3, MinIO, or any S3-compatible storage

**Governance Approach**:
- Bucket-level access control via S3 policies
- No additional tooling needed

---

### 2. Table Format: Apache Iceberg

**What It Is**: Open table format for large analytical datasets

**Why Iceberg**:
- ACID transactions on data lakes
- Schema evolution without rewrites
- Time travel and rollback capabilities
- Hidden partitioning (no partition predicates in queries)
- Multi-engine support (even if we only use DuckDB now)
- Future-proof if you need to scale to distributed engines later

**Language**: Originally Java, but **PyIceberg** provides pure Python implementation

**PyIceberg Benefits**:
- Native Python library
- No JVM required for basic operations
- Works with DuckDB directly
- Can read/write Iceberg tables from Python
- Active development by Apache Iceberg community

**Role in Stack**:
- Defines table schemas and metadata structure
- Manages snapshots and versioning
- Enables safe concurrent reads
- Provides foundation for time travel queries

**Governance Approach**:
- Schema enforcement (data types, required columns)
- Immutable snapshots (audit trail of all changes)
- No additional tooling needed

---

### 3. Catalog Layer: Apache Polaris

**What It Is**: REST catalog for Apache Iceberg tables

**Why Polaris** (only JVM component):
- Official Iceberg catalog from Snowflake (donated to Apache)
- Production-ready, purpose-built for Iceberg
- REST API (language-agnostic access)
- Built-in RBAC (role-based access control)
- Multi-namespace support (dev/staging/prod isolation)
- Small footprint (single Java service)

**Language**: Java/Quarkus (but accessed via REST API)

**Why We Accept One Java Service**:
- No mature Python-native alternative exists
- It's a stateless API service (low operational burden)
- PyIceberg and DuckDB talk to it via REST
- Small resource footprint
- Critical for production Iceberg deployments

**Role in Stack**:
- Tracks all Iceberg tables and their locations
- Manages table metadata pointers
- Provides RBAC for table access
- Enables atomic catalog operations
- Supports multiple namespaces for environment isolation

**Governance Approach** (Built-in):
- **RBAC**: Users, roles, and permissions at catalog level
- **Namespace isolation**: dev/staging/prod separation
- **Audit logging**: All catalog operations logged
- **Table-level permissions**: Grant/revoke on specific tables
- No additional governance tools needed

---

### 4. Query Engine: DuckDB

**What It Is**: In-process analytical database with Iceberg support

**Why DuckDB Instead of Trino**:
- ✅ **Python-native**: Embedded in Python processes, no JVM
- ✅ **Zero infrastructure**: No cluster to manage, no coordinator nodes
- ✅ **Perfect for small scale**: Handles 100GB-2TB efficiently on single node
- ✅ **Fast**: Vectorized execution, columnar processing
- ✅ **Iceberg support**: Native Iceberg table reads via DuckDB extension
- ✅ **Parquet-native**: Direct access to data files
- ✅ **Low resource usage**: Runs on laptops or small VMs
- ✅ **Simple debugging**: Everything in one process
- ✅ **Cost-effective**: Single machine vs. distributed cluster

**When DuckDB Works** (Your Scale):
- Data under 2TB
- Single-machine memory sufficient (32-128GB)
- Concurrent users under 10
- Batch analytics (not high-concurrency OLTP)
- Team comfortable with resource limits

**When You'd Need Distributed Query Engine**:
- Data exceeds 3-5TB
- Need distributed query processing
- Concurrent users exceed 20
- Complex query federation across systems
- (At that scale, consider adding Trino later)

**Language**: C++ core with Python bindings (feels native to Python)

**Role in Stack**:
- Execute dbt transformations (via dbt-duckdb)
- Power interactive analytics queries
- Run Dash/Streamlit dashboard queries
- Execute RAG-generated SQL queries
- Local development and testing

**Integration Pattern**:
- dbt connects via dbt-duckdb adapter
- Dash/Streamlit connect via DuckDB Python API
- Reads Iceberg tables directly from SeaweedFS
- Uses Polaris catalog via REST API (through PyIceberg)

**Governance Approach** (Built-in):
- **Database-level users**: CREATE USER, password management
- **Schema-level permissions**: GRANT SELECT/INSERT/UPDATE
- **Read-only users**: For dashboards and RAG queries
- **Query logging**: Built-in logging for audit
- No additional governance tools needed

---

### 5. Ingestion Layer: dlt + Airflow

**What It Is**: Python-native data ingestion framework orchestrated by Airflow

**Why dlt Only (No NiFi)**:
- ✅ **Pure Python**: No Java dependency
- ✅ **Covers 90% of use cases**: APIs, databases, files, SaaS
- ✅ **Schema inference**: Automatically detects schemas
- ✅ **Incremental loading**: Built-in state management
- ✅ **Iceberg integration**: Can write directly to Iceberg tables
- ✅ **Simple deployment**: Just Python packages
- ✅ **Developer-friendly**: Code-first, easy to test

**NiFi Removed Because**:
- Heavy JVM application
- Visual UI is overkill for batch pipelines
- dlt handles file processing with Python
- Extra operational complexity
- Not needed for small teams

**dlt Capabilities**:
- **REST APIs**: Built-in connectors + custom sources
- **Databases**: PostgreSQL, MySQL, SQL Server replication
- **Files**: CSV, JSON, Parquet processing with Python
- **SaaS Platforms**: 50+ verified sources (Salesforce, HubSpot, etc.)
- **Custom logic**: Full Python for complex transformations

**Apache Airflow Role**:
- **Orchestration**: Schedule dlt pipelines
- **Dependency management**: Define task order
- **Monitoring**: Pipeline status and alerts
- **Retry logic**: Automatic retries on failures
- **Backfills**: Historical data loading

**Language**: Both pure Python

**Integration Pattern**:
```
Airflow DAG triggers dlt pipeline
  ↓
dlt extracts data from source
  ↓
dlt writes to Iceberg tables (via DuckDB or PyIceberg)
  ↓
Tables registered in Polaris catalog
  ↓
dbt transformations run (also in Airflow)
```

**Governance Approach**:
- **Source credentials**: Stored in Airflow secrets
- **dlt validation**: Data contracts at ingestion
- **Airflow RBAC**: Control who can trigger pipelines
- No additional governance tools needed

---

### 6. Transformation Layer: dbt + dbt-duckdb

**What It Is**: SQL-based transformation framework for analytics

**Why dbt**:
- Industry standard for analytics engineering
- Version-controlled SQL transformations
- Built-in testing and documentation
- Modular models (staging → marts)
- Works with DuckDB via dbt-duckdb adapter

**dbt-duckdb Adapter**:
- Official adapter maintained by DuckDB team
- Native Iceberg support
- Incremental models
- Snapshots for slowly changing dimensions
- Seeds for static data

**Language**: Python runtime, SQL transformations

**Role in Stack**:
- Define staging layer (clean raw data)
- Build marts layer (business logic)
- Run data quality tests
- Generate documentation
- Orchestrated by Airflow

**Medallion Architecture Pattern**:
- **Raw**: Ingested data from dlt (minimal transformation)
- **Staging**: Cleaned, standardized, deduplicated
- **Marts**: Business-ready fact and dimension tables

**Governance Approach**:
- **dbt tests**: Data quality validation (not null, unique, relationships)
- **Documentation**: Auto-generated data dictionary
- **Lineage**: dbt tracks column-level lineage
- **Git history**: All changes version-controlled
- No additional governance tools needed

---

### 7. Visualization Layer: Streamlit + Plotly Dash

**Two Tools, Different Purposes**:

#### **Streamlit** (Rapid Prototyping & Internal Tools)

**What It Is**: Python library for building data apps quickly

**When to Use Streamlit**:
- ✅ Rapid prototyping (build in hours)
- ✅ Internal data exploration tools
- ✅ ML model demos and experiments
- ✅ Ad-hoc analysis interfaces
- ✅ Quick wins for stakeholders
- ✅ Developer tools and admin panels

**Strengths**:
- Extremely fast development (pure Python)
- No HTML/CSS/JavaScript needed
- Auto-reloading on code changes
- Built-in widgets (sliders, dropdowns, file uploads)
- Easy deployment

**Limitations**:
- Less customizable than Dash
- Not ideal for complex multi-page apps
- Performance limits with many users
- Less control over styling

**Language**: Pure Python

**Integration**: Connect directly to DuckDB via Python API

---

#### **Plotly Dash** (Production Dashboards)

**What It Is**: Framework for production-grade analytical web applications

**When to Use Dash**:
- ✅ Production dashboards (customer-facing)
- ✅ Embedded analytics in applications
- ✅ Complex interactive visualizations
- ✅ Multi-page applications
- ✅ Custom styling requirements
- ✅ High-performance needs

**Strengths**:
- Full customization (React components under hood)
- Production-ready performance
- Complex callbacks and interactivity
- Sophisticated layouts
- Enterprise deployment

**Trade-off**:
- Slower development than Streamlit
- More code required
- Steeper learning curve

**Language**: Pure Python (wraps React components)

**Integration**: Connect directly to DuckDB via Python API

---

**Recommended Workflow**:
1. **Prototype in Streamlit** (fast iteration)
2. **Validate with stakeholders** (get feedback)
3. **Rebuild in Dash for production** (if needed)
4. **Deploy both** (Streamlit for internal, Dash for external)

**Governance Approach**:
- **DuckDB read-only users**: Dashboards use limited permissions
- **Application-level auth**: Streamlit/Dash built-in auth
- **Query timeout limits**: Prevent runaway queries
- No additional governance tools needed

---

### 8. AI/RAG Integration Layer

**What It Is**: Natural language query interface to lakehouse data

**Components** (All Python):
- **LangChain**: RAG orchestration framework
- **ChromaDB**: Vector database for metadata embeddings
- **Sentence Transformers**: Generate embeddings locally

**Architecture Pattern**:
```
User asks: "What were sales last month?"
  ↓
LangChain retrieves relevant table metadata from ChromaDB
  ↓
LLM generates SQL query with context
  ↓
DuckDB executes query on Iceberg tables
  ↓
LangChain formats response
  ↓
Answer returned to user
```

**What Gets Indexed in ChromaDB**:
- Table schemas (from Polaris catalog)
- Column descriptions and data types
- Business glossary terms
- Example queries and their SQL
- Common metrics definitions (ARR, MRR, CAC, etc.)

**LLM Options**:
- **Cloud-based**: OpenAI GPT-4, Anthropic Claude (API costs)
- **Local**: Llama 2, Mistral, CodeLlama (zero API costs, runs on CPU)
- **Local GPU**: Llama 3 70B, Mixtral 8×7B via Ollama (RTX 4090 24 GB VRAM can run 70B models at Q4 quantization with no API costs — strongly preferred for this machine)
- **Specialized**: SQLCoder (fine-tuned for text-to-SQL)

**Use Cases**:
- Slack/Teams chatbot for data questions
- Voice assistants ("Alexa, what were yesterday's sales?")
- Automated report generation
- Data discovery ("What tables contain customer info?")
- Anomaly detection queries

**Integration Pattern**:
- FastAPI service exposes REST endpoint
- Airflow DAG updates ChromaDB daily (when schemas change)
- Dash/Streamlit apps can call RAG API
- External tools query via HTTP

**Language**: Pure Python (LangChain, ChromaDB, FastAPI)

**Governance Approach**:
- **Read-only DuckDB user**: RAG queries can't modify data
- **Query validation**: Sanitize generated SQL before execution
- **Rate limiting**: Prevent API abuse
- **Audit logging**: Log all AI-generated queries
- **Respect Polaris permissions**: Only query tables user can access
- No additional governance tools needed

---

### 9. Orchestration Layer: Apache Airflow

**What It Is**: Workflow orchestration platform

**Why Airflow**:
- Industry standard for data pipelines
- Pure Python DAG definitions
- Rich ecosystem of integrations
- Built-in monitoring and alerting
- Cosmos plugin for dbt integration

**Language**: Pure Python

**Role in Stack**:
- Schedule dlt ingestion pipelines
- Orchestrate dbt transformations (via Cosmos)
- Update RAG metadata (ChromaDB indexing)
- Data quality checks
- Alert on failures

**Airflow Cosmos**:
- Automatically generate Airflow tasks from dbt models
- Run dbt tests as Airflow tasks
- Visualize dbt lineage in Airflow UI
- Incremental model runs

**Governance Approach**:
- **Airflow RBAC**: Control DAG access by user
- **Secret management**: Secure credential storage
- **Audit logs**: All DAG runs logged
- No additional governance tools needed

---

## Simplified Governance Strategy

**Philosophy**: Use built-in features, avoid dedicated governance tools

### Layer 1: Catalog (Polaris RBAC)
- Create roles (analyst, engineer, admin)
- Grant table-level permissions
- Namespace isolation (dev can't touch prod)
- Audit all catalog operations

### Layer 2: Query (DuckDB Users)
- CREATE USER with passwords
- GRANT SELECT for read-only (dashboards, RAG)
- GRANT ALL for transformations (dbt)
- Query logging enabled

### Layer 3: Application (Python Code)
- Airflow RBAC for pipeline access
- Streamlit/Dash authentication
- FastAPI JWT tokens for RAG API
- Environment variables for secrets

### Layer 4: Data Quality (dbt Tests)
- NOT NULL checks
- UNIQUE constraints
- Referential integrity
- Custom business logic tests
- Freshness checks

### Layer 5: Lineage (dbt + Airflow)
- dbt automatically tracks lineage
- Airflow shows dependencies
- Git history for change tracking
- No separate tool needed

### What We're NOT Using:
- ❌ Apache Ranger (too complex for small scale)
- ❌ Apache Atlas (heavyweight metadata management)
- ❌ Dedicated data catalog tools
- ❌ Separate access control frameworks

### OpenMetadata (Optional Discovery Tool)
- **Can add later** if team needs self-service discovery
- Provides glossary, tagging, ownership
- Not required for basic governance
- Adds some Java dependency but optional

---

## Data Flow

### Ingestion Flow
```
Source System
  ↓
dlt Pipeline (Python) extracts data
  ↓
Writes to Iceberg table (via DuckDB or PyIceberg)
  ↓
Registers in Polaris catalog
  ↓
Available for transformation
```

### Transformation Flow
```
Raw Iceberg tables
  ↓
dbt models read from staging schema
  ↓
SQL transformations execute in DuckDB
  ↓
Write to marts schema (Iceberg tables)
  ↓
Registered in Polaris catalog
  ↓
Available for analytics
```

### Query Flow
```
User opens Streamlit/Dash app
  ↓
App queries DuckDB
  ↓
DuckDB reads Iceberg tables from SeaweedFS
  ↓
DuckDB uses Polaris catalog for metadata
  ↓
Results displayed in app
```

### RAG Flow
```
User asks question in natural language
  ↓
LangChain retrieves metadata from ChromaDB
  ↓
LLM generates SQL with context
  ↓
DuckDB executes query
  ↓
LangChain formats answer
  ↓
Response returned
```

---

## Development Workflow

### Environment Isolation (Polaris Namespaces)

**Git Branches Map to Polaris Namespaces**:
- `main` branch → `prod_analytics` namespace
- `develop` branch → `staging_analytics` namespace
- `feature/*` branches → `dev_analytics` namespace

**No Separate Catalogs Needed**: Single Polaris instance, multiple namespaces

### Local Development
1. Developer works in `dev_analytics` namespace
2. Uses DuckDB locally (embedded mode)
3. Tests dbt models on subset of data
4. Commits code to Git feature branch

### Staging Deployment
1. Merge to `develop` branch
2. Airflow runs dlt + dbt in `staging_analytics` namespace
3. Validate with realistic data volumes
4. Test dashboards against staging

### Production Deployment
1. Merge to `main` branch (with approval)
2. Airflow runs dlt + dbt in `prod_analytics` namespace
3. Production tables updated
4. Dashboards switch to prod namespace

### Schema Changes
1. Test in `dev_analytics` namespace
2. Use Iceberg schema evolution (add/drop columns)
3. Validate in `staging_analytics`
4. Deploy to `prod_analytics` via CI/CD
5. Roll back via Iceberg snapshots if needed

---

## Deployment Patterns

### Docker Compose (Development & Small Production)

**Services**:
- SeaweedFS (object storage)
- Polaris (Iceberg catalog)
- PostgreSQL (Polaris backend + Airflow metadata)
- Airflow (webserver, scheduler, workers)
- DuckDB (embedded in applications, no service needed)
- Streamlit apps (Python processes)
- Dash apps (Python processes)
- RAG API (FastAPI service)
- ChromaDB (embedded or standalone)

**Single Machine Requirements**:
- 32-64 GB RAM (for DuckDB in-memory processing) — *this machine: 64 GB DDR5 @ 6000 MT/s*
- 500 GB–2 TB SSD (for local data caching) — *this machine: 2× 1 TB Samsung 990 PRO PCIe 4.0 NVMe*
- 4-8 CPU cores — *this machine: i9-13900K 24-core (8P + 16E)*
- GPU optional for local LLMs — *this machine: RTX 4090 24 GB VRAM (can run 70B parameter models locally)*

### Kubernetes (Larger Scale)

**Deployments**:
- SeaweedFS Operator
- Polaris Catalog (stateless, horizontal scaling)
- Airflow Helm Chart
- Streamlit apps (multiple replicas)
- Dash apps (multiple replicas)
- RAG API (multiple replicas with ChromaDB PVC)

**DuckDB Consideration**: Still runs embedded in apps (not clustered)

---

## Technology Summary

### Pure Python Components (90% of stack)
- dlt (ingestion)
- dbt (transformation)
- DuckDB (queries)
- PyIceberg (Iceberg operations)
- Airflow (orchestration)
- Streamlit (rapid viz)
- Plotly Dash (production viz)
- LangChain (RAG orchestration)
- ChromaDB (vector DB)
- Sentence Transformers (embeddings)
- FastAPI (RAG service)

### Non-Python (with justification)
- **Polaris** (Java/Quarkus): Only mature Iceberg catalog, REST API
- **SeaweedFS** (Go): S3-compatible storage, language-agnostic API
- **DuckDB core** (C++): Embedded database, feels native to Python

### Why This Works
- Single language for development (Python)
- Minimal JVM operational burden (1 service)
- Simpler debugging and monitoring
- Unified toolchain (pytest, black, ruff, mypy)
- Easier hiring (Python skills only)
- Lower infrastructure costs (fewer services)

---

## Scaling Considerations

### When This Stack Works
- ✅ Data under 2TB
- ✅ Batch workloads (hourly/daily updates)
- ✅ Concurrent users under 10
- ✅ Team of 2-5 people
- ✅ Budget-conscious deployments

### When to Evolve
- **Data exceeds 3TB**: Add Trino for distributed queries
- **High concurrency needed**: Add query caching or Trino
- **Real-time requirements**: Add streaming layer (Kafka + Flink)
- **Complex governance**: Add Apache Ranger or OPA
- **Large team**: Add dedicated data catalog (OpenMetadata)

### Migration Path
This stack is designed to scale up gradually:
- DuckDB → Trino (change dbt adapter, keep Iceberg tables)
- dlt only → dlt + streaming tools (add Kafka/Flink)
- Built-in governance → Ranger (add centralized policies)
- All layers remain compatible (Iceberg abstraction)

---

## Why Python-First Matters

### Operational Benefits
- **Single runtime**: Python everywhere (except Polaris REST API)
- **Unified debugging**: All code in one language
- **Consistent tooling**: pytest, black, mypy, ruff across all components
- **Easier onboarding**: New team members learn Python only
- **Simpler CI/CD**: Python build pipelines only

### Developer Productivity
- **Faster iteration**: No context-switching between languages
- **Reusable code**: Share utilities across ingestion, transformation, viz
- **Better IDE support**: One language, better autocomplete
- **Easier troubleshooting**: Stack traces in Python only

### Cost Efficiency
- **Fewer servers**: DuckDB embedded, no query cluster
- **Lower memory**: No JVM overhead (except Polaris)
- **Smaller VMs**: Python apps have lower baseline resource needs
- **Simplified licensing**: All open source, no enterprise editions

### Trade-offs Accepted
- **DuckDB limits**: Single-node only, but sufficient for scale
- **No real-time**: Batch only, but 90% of analytics is batch
- **DIY governance**: No Ranger, but built-in tools sufficient
- **One JVM service**: Polaris necessary, minimal operational burden

---

## Best Practices

### Data Modeling
- Start with wide staging tables (raw data)
- Build focused mart tables (business logic)
- Use Iceberg partitioning for date/time columns
- Leverage schema evolution for flexibility
- Document all models in dbt

### Performance
- Use DuckDB's columnar storage efficiently
- Partition large tables by date
- Create aggregate tables for common queries
- Use dbt incremental models to avoid full refreshes
- Monitor query patterns, optimize hot paths

### Security
- Use Polaris RBAC for table access
- Create read-only DuckDB users for dashboards
- Store secrets in Airflow connections
- Enable audit logging in Polaris and DuckDB
- Use namespace isolation for environments

### Monitoring
- Airflow for pipeline health
- dbt test results for data quality
- DuckDB query logs for performance
- Streamlit/Dash app metrics
- RAG API success rates

### Cost Management
- Use local LLMs for RAG (avoid API costs)
- Compress Parquet files with Snappy/ZSTD
- Set up Iceberg snapshot expiration
- Monitor storage growth in SeaweedFS
- Use DuckDB's efficient memory management

---

## Conclusion

This Python-first lakehouse architecture provides:

**Simplicity**:
- Minimal components (10 vs. 20+ in traditional stacks)
- Single primary language (Python)
- Built-in governance (no dedicated tools)
- Embedded query engine (no cluster management)

**Appropriate for Small Scale**:
- DuckDB handles 100GB-2TB efficiently
- dlt covers all ingestion needs
- Polaris provides production-ready catalog
- Streamlit enables rapid prototyping
- Dash delivers production dashboards

**Future-Proof**:
- Iceberg tables work with any query engine
- Can add Trino later without data migration
- Standard tools (dbt, Airflow) scale to enterprise
- RAG layer enables AI-powered analytics

**Cost-Effective**:
- Fewer servers (DuckDB embedded)
- Lower operational burden (fewer tools)
- Open source (no licensing costs)
- Local LLMs (no API fees)

**Developer-Friendly**:
- Python skills only
- Fast iteration cycles
- Version-controlled everything
- Easy debugging and testing

This stack proves you can build a production lakehouse with modern capabilities (ACID transactions, time travel, AI queries) without the complexity and cost of traditional big data architectures.

Start simple, scale when needed, and keep Python at the core.

---

## MVP Implementation Guide: Docker on Windows

This section provides explicit, step-by-step instructions — including all commands, config files, and working code — to build a running MVP lakehouse on Windows using Docker.

### MVP Goals

**What You'll Build**:
- All core services running in Docker (SeaweedFS, Polaris, PostgreSQL, Jupyter)
- Sample sales dataset (1,000 rows)
- dlt ingestion pipeline (CSV → DuckDB)
- dbt transformations (raw → staging → marts)
- Streamlit dashboard with charts and filters
- Smoke tests verifying every layer

---

### Development Machine Specifications

| Component | Specification |
|-----------|--------------|
| **GPU** | MSI GeForce RTX 4090 Gaming X Trio 24G — 24 GB GDDR6X, 384-bit, 2595 MHz boost (Ada Lovelace) |
| **Motherboard** | ASUS PRIME Z790-P WIFI — Intel Z790 chipset |
| **CPU** | Intel Core i9-13900K — 24 cores (8 P-cores + 16 E-cores), 36 MB L3 cache |
| **Storage** | 2× Samsung 990 PRO 1 TB PCIe 4.0 M.2 NVMe (2 TB total) |
| **RAM** | G.SKILL Trident Z5 RGB 64 GB DDR5 (2×32 GB) @ 6000 MT/s CL32 (XMP 3.0) |
| **OS** | Windows 11 Pro |

**Implications for this stack**:
- DuckDB can use up to **48 GB RAM** comfortably (leaving 16 GB for OS + Docker overhead)
- **24 CPU threads** available for DuckDB parallel query execution
- **2 TB NVMe** storage supports datasets well beyond the MVP sample data
- **RTX 4090 (24 GB VRAM)** enables fully local LLM inference (Llama 3 70B, Mistral, etc.) for the RAG layer — no API costs

---

**Prerequisites**:
- Windows 11 Pro (installed)
- 64 GB RAM — well above minimum; allocate up to 48 GB to Docker
- 2 TB NVMe SSD — allocate at least 200 GB for Docker and lakehouse data
- Admin rights to install software

---

### Phase 1: Windows Docker Setup

#### Step 1.1: Install WSL2

Open **PowerShell as Administrator** and run:

```powershell
wsl --install
```

This installs WSL2 and **Ubuntu 24.04** by default on current Windows 11 builds (older builds may install 22.04 — run `wsl --list --verbose` after reboot to confirm). **Restart your computer** when prompted.

After restart, Ubuntu opens automatically. Create a Unix username and password when asked.

Verify WSL2 is active:

```powershell
wsl --list --verbose
```

Expected output:
```
  NAME      STATE           VERSION
* Ubuntu    Running         2
```

**If WSL was already installed but shows version 1:**
```powershell
wsl --set-default-version 2
wsl --set-version Ubuntu 2
```

**Troubleshooting:**
- `wsl --install` fails → Enable virtualization in BIOS (Intel VT-x or AMD-V)
- Ubuntu won't start → Run `winver`; you need Windows build 19041 or higher

---

#### Step 1.2: Configure WSL2 Resources (.wslconfig)

Before installing Docker, set WSL2's memory, CPU, and networking limits so the VM is properly sized for this machine from the start.

**Copy the pre-configured file** from the repo into your Windows user profile:

```powershell
# In PowerShell — replace <YourUsername> with your actual Windows username
Copy-Item "D:\source\lakehouse\lakehouse\wsl\.wslconfig" `
          "C:\Users\<YourUsername>\.wslconfig"
```

Or create `C:\Users\<YourUsername>\.wslconfig` manually with this content:

```ini
# .wslconfig — WSL2 global configuration
# Hardware: i9-13900K (24 cores) | 64 GB DDR5 @ 6000 MT/s | RTX 4090 24 GB | 2x 1 TB NVMe

[wsl2]

# Leave ~8 GB for Windows + background apps
# Docker Desktop will further cap its distro to 48 GB via Settings > Resources
memory=56GB

# Leave 4 logical cores for Windows (i9-13900K has 24 threads)
processors=20

# Swap file — useful when DuckDB spills large sorts to disk
# D: preferred: C: already has the Windows page file + WSL2/Docker .vhdx files
# competing for I/O. D: is idle during normal OS operation.
swap=16GB
swapFile=D:\\Temp\\wsl-swap.vhdx

# Allow accessing WSL2 services from Windows via localhost
localhostForwarding=true

[experimental]

# Release WSL2 RAM back to Windows after heavy DuckDB workloads
autoMemoryReclaim=gradual

# Shrink the WSL2 .vhdx automatically when files are deleted (saves NVMe space)
sparseVhd=true

# NOTE: networkingMode=mirrored is intentionally omitted.
# It breaks Docker Desktop WSL integration (Wsl/Service/0x8007274c error).
# Default NAT mode works correctly. Reach Ollama from containers via:
#   http://host.docker.internal:11434
```

**Apply the config — full restart sequence:**

`.wslconfig` is only read when the WSL2 virtual machine first boots. Any running WSL2 or Docker Desktop processes must be stopped first.

**1. Create the swap directory on D:** (do this before shutdown, so it exists on next boot):

```powershell
New-Item -ItemType Directory -Force -Path D:\Temp
```

> **Why D: not C:?** C: already carries the Windows page file, the Ubuntu WSL2 `.vhdx`, and Docker Desktop's `.vhdx`. Putting the swap file on D: keeps large sequential DuckDB spill writes off the busy OS drive. Both Samsung 990 Pro drives are identical, so there's no speed trade-off — only less I/O contention.

**Optional but recommended — move Docker Desktop's disk image to D: as well:**

In Docker Desktop → Settings → Resources → Advanced → **Disk image location**, set it to `D:\Docker` then click **Apply & Restart**. This moves Docker's `.vhdx` (which can grow to 100 GB+) off C:, keeping the OS drive free.

**2. Quit Docker Desktop** if it is running:

Right-click the Docker whale icon in the system tray → **Quit Docker Desktop**. Wait for the icon to disappear.

**3. Shut down all WSL2 instances:**

```powershell
wsl --shutdown
```

Verify everything is stopped:

```powershell
wsl --list --verbose
```

Expected — all distros show `Stopped`:

```
  NAME              STATE           VERSION
* Ubuntu            Stopped         2
  docker-desktop    Stopped         2
```

**4. Wait 8 seconds** for the WSL2 kernel process (`vmmem`) to fully exit. You can confirm in Task Manager — `vmmem` should disappear from the process list.

**5. Start WSL2 again:**

```powershell
wsl
```

This opens an Ubuntu shell. The new `.wslconfig` limits are applied at this boot. The shell prompt will appear when the distro is ready (usually 5–10 seconds).

**6. Verify the settings took effect** (run inside the Ubuntu shell):

```bash
free -h
# Expected: total memory ~55 GB (may show slightly less due to kernel overhead)
#   Mem:    55G    ...

nproc
# Expected: 20
```

If `free -h` still shows the old value (e.g., 32 GB), the old `.wslconfig` is still in place. Double-check the file is saved at `C:\Users\<YourUsername>\.wslconfig` (not `.wslconfig.txt` or inside a subfolder).

**7. Restart Docker Desktop:**

Launch Docker Desktop from the Start Menu. It will start its own WSL2 distro (`docker-desktop`) using the new limits.

Confirm Docker is running:

```powershell
docker run hello-world
```

> **Tip:** If `wsl --list --verbose` shows `docker-desktop` distro never reaches `Running` after restart, open Docker Desktop → Settings → General → uncheck then re-check **"Use the WSL 2 based engine"**, then Apply & Restart.
>
> **Known issue:** If you see `Wsl/Service/0x8007274c` or "WSL integration unexpectedly stopped", `networkingMode=mirrored` is the cause. Remove it from `.wslconfig`, run `wsl --shutdown`, then restart Docker Desktop.

---

#### Step 1.3: Install Docker Desktop

1. Download Docker Desktop from `https://www.docker.com/products/docker-desktop/`
2. Run the installer. Ensure **"Use WSL 2 based engine"** is checked during setup.
3. Restart when prompted.
4. Launch Docker Desktop and accept the license agreement.

**Set resource limits** — Docker Desktop → Settings → Resources:

| Setting | Minimum | Recommended | This Machine |
|---------|---------|-------------|--------------|
| Memory  | 8 GB    | 12 GB       | 48 GB        |
| CPUs    | 4       | 6           | 20           |
| Disk    | 50 GB   | 80 GB       | 200 GB       |

> **Note (i9-13900K):** Assign 20 of the 24 logical cores to Docker (leave 4 for Windows/OS tasks). With 64 GB RAM, allocating 48 GB gives Docker plenty of headroom for DuckDB's in-memory processing while keeping the host stable.

**Verify Docker is working:**

```powershell
docker run hello-world
```

Expected: `Hello from Docker!` in the output.

---

#### Step 1.4: Verify NVIDIA Driver (Windows)

The RTX 4090 GPU becomes available inside WSL2 and Docker automatically once the Windows host driver is installed — no Linux GPU driver needed inside WSL2.

Check your driver version in PowerShell:

```powershell
nvidia-smi
```

Expected output includes your driver version and GPU name:
```
+-----------------------------------------------------------------------------+
| NVIDIA-SMI 561.xx    Driver Version: 561.xx    CUDA Version: 12.x          |
|-------------------------------+----------------------+----------------------+
| GPU 0: NVIDIA GeForce RTX 4090 ...
```

**Minimum driver version required:** 527.41 (supports CUDA 12.x in WSL2).

If not installed or out of date, download the latest from the [NVIDIA driver page](https://www.nvidia.com/Download/index.aspx) and install on Windows, then reboot.

> **Important:** Do NOT install a Linux NVIDIA GPU driver inside WSL2. The Windows driver is passed through automatically.

---

#### Step 1.5: Install CUDA Toolkit in WSL2

The CUDA Toolkit provides compiler and runtime libraries needed for PyTorch GPU acceleration and sentence-transformers. Install it inside WSL2, **not** on Windows.

Open the WSL2 Ubuntu terminal and run:

```bash
# Check your Ubuntu version first — the repo URL differs between 22.04 and 24.04
lsb_release -rs
```

**For Ubuntu 24.04** (default from `wsl --install` on recent Windows builds):

```bash
wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2404/x86_64/cuda-keyring_1.1-1_all.deb
sudo dpkg -i cuda-keyring_1.1-1_all.deb
```

**For Ubuntu 22.04:**

```bash
wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/cuda-keyring_1.0-1_all.deb
sudo dpkg -i cuda-keyring_1.0-1_all.deb
```

> **Expected warning after `dpkg -i`:** If you see *"A deprecated public CUDA GPG key appears to be installed"*, an old key from a previous CUDA install is present. Remove it before continuing — this is normal on systems that have had any prior CUDA or NVIDIA packages:
> ```bash
> sudo apt-key del 7fa2af80
> ```

Continue with the install:

```bash
sudo apt-get update

# Install the toolkit ONLY — do NOT use the 'cuda' or 'cuda-drivers' meta-package
# Those would attempt to install a Linux GPU driver, breaking WSL2 passthrough
# Do NOT use: sudo apt install nvidia-cuda-toolkit  (that's an older Ubuntu repo version)
sudo apt-get install -y cuda-toolkit-12-6
```

**Add CUDA to your PATH** (the installer does not do this automatically):

```bash
echo 'export PATH=/usr/local/cuda/bin:$PATH' >> ~/.bashrc
echo 'export LD_LIBRARY_PATH=/usr/local/cuda/lib64:$LD_LIBRARY_PATH' >> ~/.bashrc
source ~/.bashrc
```

Verify the toolkit is visible:

```bash
nvcc --version
# Expected: Cuda compilation tools, release 12.6, ...

nvidia-smi
# Expected: RTX 4090 listed with Driver Version from Windows
```

**Troubleshooting — `nvcc: command not found` after install:**
```bash
# Confirm the binary exists
ls /usr/local/cuda/bin/nvcc

# If it exists but nvcc still not found, the PATH line above wasn't applied yet
source ~/.bashrc
nvcc --version
```

---

#### Step 1.6: Install NVIDIA Container Toolkit (GPU access for Docker)

This allows Docker containers to use the RTX 4090. Run inside WSL2:

```bash
# Modern keyring approach (replaces deprecated apt-key, works on Ubuntu 24.04)
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | \
  sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg

curl -s -L https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list | \
  sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
  sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

sudo apt-get update && sudo apt-get install -y nvidia-container-toolkit

# Write the NVIDIA runtime config to /etc/docker/daemon.json
sudo nvidia-ctk runtime configure --runtime=docker
```

> **Do NOT run `sudo systemctl restart docker` in WSL2.** Docker Desktop is not a systemd service — it runs on Windows. Restarting it via systemctl will always fail with `Unit docker.service not found`. Restart Docker Desktop from Windows instead (next step).

**Restart Docker Desktop from Windows:**

Right-click the Docker whale icon in the system tray → **Restart Docker Desktop**. Wait for the icon to become steady (not animated).

**Test GPU access inside a container:**

```bash
docker run --rm --gpus all nvidia/cuda:12.6.0-runtime-ubuntu22.04 nvidia-smi
```

> **Note:** The `ubuntu22.04` in the image tag refers to the container's OS, not the host. This image runs correctly on Ubuntu 24.04 WSL2 hosts — Docker containers are isolated from the host OS version.

Expected: RTX 4090 shown inside the container output. If this passes, all Docker-based AI workloads will see the GPU.

**Troubleshooting:**
- `Failed to initialize NVML` → Ensure Docker Desktop is ≥ v4.31.1 (update via Docker Desktop Settings → Software Updates)
- `Unsupported distribution` warning during install → Harmless on Ubuntu 24.04; the toolkit installs correctly from the ubuntu2204 fallback repo
- GPU not listed → Confirm `nvidia-smi` works in WSL2 outside Docker first (Step 1.4)

---

#### Step 1.7: Create Project Directory Structure

Open PowerShell and run:

```powershell
New-Item -ItemType Directory -Force -Path C:\lakehouse-mvp
cd C:\lakehouse-mvp
mkdir docker, data, dlt, dbt, streamlit, storage, chroma_db
```

Expected structure:
```
C:\lakehouse-mvp\
├── docker\        ← Docker configs, Dockerfile, requirements.txt
├── data\          ← Sample CSV files
├── dlt\           ← dlt pipeline scripts
├── dbt\           ← dbt project
├── streamlit\     ← Streamlit dashboard
├── storage\       ← SeaweedFS volume data (auto-populated)
└── chroma_db\     ← ChromaDB vectors (for later RAG)
```

---

### Phase 2: Docker Compose Configuration

All files in this phase go in `C:\lakehouse-mvp\docker\`.

#### Step 2.1: Create docker-compose.yml

Create `C:\lakehouse-mvp\docker\docker-compose.yml`:

```yaml
version: '3.8'

services:

  # ── PostgreSQL ──────────────────────────────────────────────
  # Backend database for the Polaris Iceberg catalog
  postgres:
    image: postgres:15
    container_name: lakehouse-postgres
    environment:
      POSTGRES_DB: polaris
      POSTGRES_USER: polaris
      POSTGRES_PASSWORD: polaris_secret
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U polaris"]
      interval: 5s
      timeout: 5s
      retries: 10

  # ── SeaweedFS ───────────────────────────────────────────────
  # Distributed object storage with S3-compatible API
  seaweedfs-master:
    image: chrislusf/seaweedfs:latest
    container_name: lakehouse-weed-master
    ports:
      - "9333:9333"
    command: "master -ip=seaweedfs-master -ip.bind=0.0.0.0"
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:9333/cluster/status"]
      interval: 5s
      timeout: 5s
      retries: 10

  seaweedfs-volume:
    image: chrislusf/seaweedfs:latest
    container_name: lakehouse-weed-volume
    ports:
      - "8080:8080"
    command: "volume -mserver=seaweedfs-master:9333 -ip.bind=0.0.0.0 -dir=/data -dataCenter=dc1"
    depends_on:
      seaweedfs-master:
        condition: service_healthy
    volumes:
      - ../storage:/data

  seaweedfs-filer:
    image: chrislusf/seaweedfs:latest
    container_name: lakehouse-weed-filer
    ports:
      - "8898:8898"
    command: "filer -master=seaweedfs-master:9333 -port=8898"
    depends_on:
      seaweedfs-master:
        condition: service_healthy

  seaweedfs-s3:
    image: chrislusf/seaweedfs:latest
    container_name: lakehouse-weed-s3
    ports:
      - "8333:8333"
    command: "s3 -filer=seaweedfs-filer:8898 -port=8333 -config=/etc/seaweedfs/s3.json"
    depends_on:
      - seaweedfs-filer
    volumes:
      - ./s3.json:/etc/seaweedfs/s3.json

  # ── Apache Polaris ──────────────────────────────────────────
  # REST catalog for Apache Iceberg tables
  # Note: verify the image tag at https://github.com/apache/polaris
  polaris:
    image: apache/polaris:latest
    container_name: lakehouse-polaris
    ports:
      - "8181:8181"
    environment:
      QUARKUS_DATASOURCE_DB_KIND: postgresql
      QUARKUS_DATASOURCE_USERNAME: polaris
      QUARKUS_DATASOURCE_PASSWORD: polaris_secret
      QUARKUS_DATASOURCE_JDBC_URL: jdbc:postgresql://postgres:5432/polaris
      POLARIS_PERSISTENCE_TYPE: relational-jdbc
    depends_on:
      postgres:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8181/api/management/v1/health"]
      interval: 10s
      timeout: 5s
      retries: 20

  # ── Python Workspace ────────────────────────────────────────
  # Jupyter Lab + all Python tools (dlt, dbt, Streamlit, etc.)
  workspace:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: lakehouse-workspace
    ports:
      - "8889:8889"    # Jupyter Lab
      - "8501:8501"    # Streamlit
    volumes:
      - ../data:/workspace/data
      - ../dlt:/workspace/dlt
      - ../dbt:/workspace/dbt
      - ../streamlit:/workspace/streamlit
      - ../chroma_db:/workspace/chroma_db
      - workspace_db:/workspace/db    # DuckDB persistent storage
    environment:
      S3_ENDPOINT: http://seaweedfs-s3:8333
      S3_ACCESS_KEY: lakehouse_key
      S3_SECRET_KEY: lakehouse_secret
      POLARIS_URI: http://polaris:8181/api/catalog
    command: >
      jupyter lab
      --ip=0.0.0.0
      --port=8889
      --no-browser
      --allow-root
      --NotebookApp.token=''
      --NotebookApp.password=''
      --notebook-dir=/workspace
    depends_on:
      polaris:
        condition: service_healthy

volumes:
  postgres_data:
  workspace_db:

networks:
  default:
    name: lakehouse-network
```

---

#### Step 2.2: Create SeaweedFS S3 Auth Config

Create `C:\lakehouse-mvp\docker\s3.json`:

```json
{
  "identities": [
    {
      "name": "lakehouse-admin",
      "credentials": [
        {
          "accessKey": "lakehouse_key",
          "secretKey": "lakehouse_secret"
        }
      ],
      "actions": ["Read", "Write", "List", "Tagging", "Admin"]
    }
  ]
}
```

---

#### Step 2.3: Create Dockerfile

Create `C:\lakehouse-mvp\docker\Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /workspace

RUN apt-get update && apt-get install -y \
    curl \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8889 8501
```

---

#### Step 2.4: Create requirements.txt

Create `C:\lakehouse-mvp\docker\requirements.txt`:

```
# Jupyter
jupyterlab==4.2.0
ipykernel==6.29.0

# Data ingestion
dlt[duckdb]==0.5.0

# Query engine
duckdb==1.0.0

# Table format
pyiceberg[s3fs]==0.7.0

# Transformation
dbt-core==1.8.0
dbt-duckdb==1.8.0

# Visualization
streamlit==1.36.0
plotly==5.22.0

# Data processing
pandas==2.2.0
pyarrow==16.0.0

# S3 client
boto3==1.34.0
s3fs==2024.6.0

# Utilities
requests==2.32.0
python-dotenv==1.0.0
```

---

### Phase 3: Sample Data and Initial Setup

#### Step 3.1: Start All Docker Services

Open PowerShell in `C:\lakehouse-mvp\docker\` and run:

```powershell
cd C:\lakehouse-mvp\docker

# Build the workspace image and start all services
docker compose up -d --build

# Watch startup logs (services take 1-3 minutes to become healthy)
docker compose logs -f
# Press Ctrl+C to stop watching

# Verify all services are running
docker compose ps
```

Expected output from `docker compose ps` (all should show `running`):
```
NAME                    STATUS     PORTS
lakehouse-postgres      running    0.0.0.0:5432->5432/tcp
lakehouse-weed-master   running    0.0.0.0:9333->9333/tcp
lakehouse-weed-volume   running    0.0.0.0:8080->8080/tcp
lakehouse-weed-filer    running    0.0.0.0:8898->8898/tcp
lakehouse-weed-s3       running    0.0.0.0:8333->8333/tcp
lakehouse-polaris       running    0.0.0.0:8181->8181/tcp
lakehouse-workspace     running    0.0.0.0:8889->8889/tcp, 0.0.0.0:8501->8501/tcp
```

Open **Jupyter Lab** at `http://localhost:8889` in your browser.

---

#### Step 3.2: Create the S3 Bucket

In Jupyter Lab, open a new notebook and run:

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://seaweedfs-s3:8333",
    aws_access_key_id="lakehouse_key",
    aws_secret_access_key="lakehouse_secret",
)

s3.create_bucket(Bucket="lakehouse")

buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
print("Buckets:", buckets)
# Expected: Buckets: ['lakehouse']
```

---

#### Step 3.3: Generate Sample Data

In Jupyter, run:

```python
import csv
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)

PRODUCTS = ["Laptop", "Monitor", "Keyboard", "Mouse", "Headset", "Webcam", "Desk Chair"]
REGIONS  = ["North", "South", "East", "West", "Central"]
START    = date(2024, 1, 1)

rows = []
for i in range(1, 1001):
    order_date = START + timedelta(days=random.randint(0, 89))
    rows.append({
        "order_id":    f"ORD-{i:04d}",
        "customer_id": f"CUST-{random.randint(1, 200):04d}",
        "order_date":  order_date.isoformat(),
        "product":     random.choice(PRODUCTS),
        "quantity":    random.randint(1, 5),
        "amount":      round(random.uniform(20.0, 2000.0), 2),
        "region":      random.choice(REGIONS),
    })

out = Path("/workspace/data/sales.csv")
with open(out, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"Created {out} with {len(rows)} rows")
```

---

#### Step 3.4: Initialize Polaris Namespaces

In Jupyter, run:

```python
from pyiceberg.catalog.rest import RestCatalog

catalog = RestCatalog(
    name="polaris",
    uri="http://polaris:8181/api/catalog",
    warehouse="lakehouse",
    **{
        "s3.endpoint": "http://seaweedfs-s3:8333",
        "s3.access-key-id": "lakehouse_key",
        "s3.secret-access-key": "lakehouse_secret",
        "s3.path-style-access": "true",
    }
)

for ns in ["raw", "staging", "marts"]:
    try:
        catalog.create_namespace(ns)
        print(f"Created namespace: {ns}")
    except Exception:
        print(f"Namespace already exists: {ns}")

print("All namespaces:", catalog.list_namespaces())
```

---

### Phase 4: Data Pipeline Implementation

#### Step 4.1: dlt Ingestion Pipeline

Create `C:\lakehouse-mvp\dlt\load_sales.py`:

```python
"""
dlt pipeline: loads sales.csv into DuckDB (raw.sales table).
Run from Jupyter: from load_sales import run; run()
"""
import csv
from pathlib import Path
import dlt


@dlt.resource(name="sales", write_disposition="replace")
def sales_data(csv_path: str = "/workspace/data/sales.csv"):
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["quantity"] = int(row["quantity"])
            row["amount"]   = float(row["amount"])
            yield row


def run():
    pipeline = dlt.pipeline(
        pipeline_name="sales_pipeline",
        destination=dlt.destinations.duckdb("/workspace/db/lakehouse.duckdb"),
        dataset_name="raw",
    )
    info = pipeline.run(sales_data())
    print(info)
    return pipeline


if __name__ == "__main__":
    run()
```

**Run it in Jupyter:**

```python
import sys
sys.path.insert(0, "/workspace/dlt")

from load_sales import run
pipeline = run()

# Verify
import duckdb
conn = duckdb.connect("/workspace/db/lakehouse.duckdb")
count = conn.execute("SELECT COUNT(*) FROM raw.sales").fetchone()[0]
print(f"Loaded {count} rows into raw.sales")
print(conn.execute("SELECT * FROM raw.sales LIMIT 3").fetchdf())
conn.close()
```

Expected output:
```
Loaded 1000 rows into raw.sales
  order_id customer_id  order_date   product  quantity    amount  region
0  ORD-0001   CUST-0072  2024-01-15    Laptop         2  1423.50   North
1  ORD-0002   CUST-0118  2024-02-03   Monitor         1   849.99    East
2  ORD-0003   CUST-0055  2024-01-27  Keyboard         3    74.97   South
```

---

#### Step 4.2: dbt Transformation Project

**Initialize the dbt project** in a Jupyter terminal (Terminal → New Terminal):

```bash
cd /workspace/dbt
dbt init lakehouse_mvp --skip-profile-setup
```

**Create `C:\lakehouse-mvp\dbt\lakehouse_mvp\profiles.yml`:**

```yaml
lakehouse_mvp:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: /workspace/db/lakehouse.duckdb
      schema: staging
      threads: 4
```

**Replace the entire contents of `C:\lakehouse-mvp\dbt\lakehouse_mvp\dbt_project.yml`:**

```yaml
name: lakehouse_mvp
version: '1.0.0'
config-version: 2

profile: lakehouse_mvp

model-paths: ["models"]
test-paths:  ["tests"]

models:
  lakehouse_mvp:
    staging:
      +materialized: table
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
```

**Create the models directory structure:**

```bash
mkdir -p /workspace/dbt/lakehouse_mvp/models/staging
mkdir -p /workspace/dbt/lakehouse_mvp/models/marts
```

**Create `models/staging/stg_sales.sql`:**

```sql
with source as (
    select * from raw.sales
),

cleaned as (
    select
        order_id,
        customer_id,
        cast(order_date as date)                              as order_date,
        product,
        quantity,
        amount,
        region,
        date_part('year',    cast(order_date as date))::int  as order_year,
        date_part('month',   cast(order_date as date))::int  as order_month,
        date_part('quarter', cast(order_date as date))::int  as order_quarter,
        quantity * amount                                     as line_total
    from source
    where quantity > 0
      and amount   > 0
)

select * from cleaned
```

**Create `models/marts/daily_revenue.sql`:**

```sql
with stg as (
    select * from {{ ref('stg_sales') }}
)

select
    order_date,
    region,
    product,
    count(distinct order_id)    as order_count,
    count(distinct customer_id) as customer_count,
    sum(quantity)               as total_units,
    round(sum(line_total), 2)   as revenue,
    round(avg(amount), 2)       as avg_order_value
from stg
group by order_date, region, product
order by order_date, region, product
```

**Create `models/staging/schema.yml`** (data quality tests):

```yaml
version: 2

models:
  - name: stg_sales
    description: "Cleaned and standardized sales data"
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: customer_id
        tests:
          - not_null
      - name: order_date
        tests:
          - not_null
      - name: quantity
        tests:
          - not_null
      - name: amount
        tests:
          - not_null
```

**Run dbt** from the Jupyter terminal:

```bash
cd /workspace/dbt/lakehouse_mvp

# Verify connection
dbt debug

# Build models
dbt run

# Run data quality tests
dbt test
```

Expected output:
```
Running with dbt=1.8.x
Found 2 models, 5 tests

1 of 2 START sql table model staging.stg_sales ......... [RUN]
1 of 2 OK created sql table model staging.stg_sales .... [OK in 0.5s]
2 of 2 START sql table model marts.daily_revenue ....... [RUN]
2 of 2 OK created sql table model marts.daily_revenue .. [OK in 0.4s]

Finished running 2 table models.
5 of 5 PASS tests. All tests passed.
```

---

### Phase 5: Visualization Layer

#### Step 5.1: Streamlit Dashboard

Create `C:\lakehouse-mvp\streamlit\dashboard.py`:

```python
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Lakehouse Sales Dashboard", layout="wide")
st.title("Sales Dashboard")

DB_PATH = "/workspace/db/lakehouse.duckdb"


@st.cache_data(ttl=60)
def load_data() -> pd.DataFrame:
    conn = duckdb.connect(DB_PATH, read_only=True)
    df = conn.execute("SELECT * FROM marts.daily_revenue").df()
    conn.close()
    df["order_date"] = pd.to_datetime(df["order_date"])
    return df


df = load_data()

# ── Sidebar Filters ───────────────────────────────────────────
st.sidebar.header("Filters")

regions = ["All"] + sorted(df["region"].unique().tolist())
selected_region = st.sidebar.selectbox("Region", regions)

date_min = df["order_date"].min().date()
date_max = df["order_date"].max().date()
date_range = st.sidebar.date_input("Date Range", [date_min, date_max])

# Apply filters
filtered = df.copy()
if selected_region != "All":
    filtered = filtered[filtered["region"] == selected_region]
if len(date_range) == 2:
    start = pd.Timestamp(date_range[0])
    end   = pd.Timestamp(date_range[1])
    filtered = filtered[(filtered["order_date"] >= start) & (filtered["order_date"] <= end)]

# ── KPI Metrics ───────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Revenue",    f"${filtered['revenue'].sum():,.2f}")
c2.metric("Total Orders",     f"{filtered['order_count'].sum():,}")
c3.metric("Unique Customers", f"{filtered['customer_count'].sum():,}")
c4.metric("Avg Order Value",  f"${filtered['avg_order_value'].mean():,.2f}")

st.divider()

# ── Charts ────────────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    daily = filtered.groupby("order_date")["revenue"].sum().reset_index()
    st.plotly_chart(
        px.line(daily, x="order_date", y="revenue", title="Daily Revenue Trend"),
        use_container_width=True,
    )

with col_right:
    by_region = filtered.groupby("region")["revenue"].sum().reset_index()
    st.plotly_chart(
        px.bar(by_region, x="region", y="revenue", title="Revenue by Region", color="region"),
        use_container_width=True,
    )

by_product = (
    filtered.groupby("product")["revenue"].sum()
    .sort_values(ascending=False)
    .reset_index()
)
st.plotly_chart(
    px.bar(by_product, x="product", y="revenue", title="Revenue by Product", color="product"),
    use_container_width=True,
)

st.subheader("Detailed Data")
st.dataframe(filtered.sort_values("order_date", ascending=False), use_container_width=True)
```

**Run Streamlit** from the Jupyter terminal:

```bash
streamlit run /workspace/streamlit/dashboard.py \
  --server.port 8501 \
  --server.address 0.0.0.0
```

Open `http://localhost:8501` in your browser.

---

### Phase 6: Smoke Tests

Run each test in a Jupyter notebook to verify every layer works.

#### Test 1: SeaweedFS Storage

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://seaweedfs-s3:8333",
    aws_access_key_id="lakehouse_key",
    aws_secret_access_key="lakehouse_secret",
)

s3.put_object(Bucket="lakehouse", Key="smoke_test.txt", Body=b"ping")
body = s3.get_object(Bucket="lakehouse", Key="smoke_test.txt")["Body"].read()
assert body == b"ping", f"FAIL: got {body}"
s3.delete_object(Bucket="lakehouse", Key="smoke_test.txt")

print("PASS: SeaweedFS S3 read/write working")
```

#### Test 2: Polaris Catalog

```python
import requests

r = requests.get("http://polaris:8181/api/management/v1/health")
assert r.status_code == 200, f"FAIL: HTTP {r.status_code}"
print("PASS: Polaris catalog healthy")
```

#### Test 3: DuckDB Raw Data

```python
import duckdb

conn = duckdb.connect("/workspace/db/lakehouse.duckdb", read_only=True)
count = conn.execute("SELECT COUNT(*) FROM raw.sales").fetchone()[0]
assert count == 1000, f"FAIL: expected 1000, got {count}"
print(f"PASS: raw.sales has {count} rows")
conn.close()
```

#### Test 4: dbt Transformations

```python
import duckdb

conn = duckdb.connect("/workspace/db/lakehouse.duckdb", read_only=True)
stg   = conn.execute("SELECT COUNT(*) FROM staging.stg_sales").fetchone()[0]
marts = conn.execute("SELECT COUNT(*) FROM marts.daily_revenue").fetchone()[0]
assert stg   > 0, "FAIL: staging.stg_sales is empty"
assert marts > 0, "FAIL: marts.daily_revenue is empty"
print(f"PASS: staging={stg} rows, marts={marts} rows")
conn.close()
```

#### Test 5: Revenue Aggregation

```python
import duckdb

conn = duckdb.connect("/workspace/db/lakehouse.duckdb", read_only=True)
result = conn.execute("""
    SELECT region, round(sum(revenue), 2) as total_revenue
    FROM marts.daily_revenue
    GROUP BY region
    ORDER BY total_revenue DESC
""").fetchdf()
print(result)
assert len(result) == 5, f"FAIL: expected 5 regions, got {len(result)}"
print("PASS: Revenue aggregation by region correct")
conn.close()
```

#### Test 6: End-to-End Incremental Update

```python
import csv

# Append a new row to the source CSV
new_row = {
    "order_id":    "ORD-9999",
    "customer_id": "CUST-0001",
    "order_date":  "2024-03-31",
    "product":     "Laptop",
    "quantity":    "1",
    "amount":      "1500.00",
    "region":      "North",
}
with open("/workspace/data/sales.csv", "a", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=new_row.keys())
    writer.writerow(new_row)

print("New row appended. Now re-run the dlt pipeline, then dbt run, then refresh Streamlit.")
```

After running the above cell:
1. Re-run the dlt pipeline cell (raw.sales should have 1,001 rows)
2. Run `dbt run` in the terminal
3. Refresh `http://localhost:8501` — the new row should appear

---

### Phase 7: Troubleshooting Guide

#### Issue 1: A container keeps restarting

```powershell
# See the error for a specific container
docker logs lakehouse-polaris --tail 50
docker logs lakehouse-weed-master --tail 30

# Check for port conflicts
netstat -ano | findstr "8181"
netstat -ano | findstr "9333"

# Rebuild from scratch (keeps volumes)
docker compose down
docker compose up -d --build

# Nuclear reset (deletes all data, start fresh)
docker compose down -v
docker compose up -d --build
```

#### Issue 2: Polaris returns 500 errors

```powershell
# Check if PostgreSQL is healthy first
docker inspect lakehouse-postgres | findstr "Health"

# Test Polaris health endpoint
curl http://localhost:8181/api/management/v1/health

# Restart just Polaris after verifying postgres is up
docker compose restart polaris
```

Common causes:
- PostgreSQL not ready yet — wait 30 seconds and retry
- Wrong credentials — `POSTGRES_PASSWORD` must match in both services
- Port 8181 already in use on host — check with `netstat`

#### Issue 3: DuckDB tables not found

```python
# List all tables in the database to debug
import duckdb
conn = duckdb.connect("/workspace/db/lakehouse.duckdb")
print(conn.execute("SHOW ALL TABLES").fetchdf())
conn.close()
```

- `raw.sales` missing → re-run the dlt pipeline
- `staging.*` or `marts.*` missing → run `dbt run` in the Jupyter terminal

#### Issue 4: dbt models fail to build

```bash
# In Jupyter terminal
cd /workspace/dbt/lakehouse_mvp

# Check connection first
dbt debug

# See the compiled SQL that failed
cat target/compiled/lakehouse_mvp/models/staging/stg_sales.sql

# Run only one model with verbose output
dbt run --select stg_sales --debug
```

#### Issue 5: Streamlit shows empty charts

```python
# Test the exact query Streamlit runs
import duckdb
conn = duckdb.connect("/workspace/db/lakehouse.duckdb", read_only=True)
print(conn.execute("SELECT COUNT(*) FROM marts.daily_revenue").fetchone())
# If (0,) or an error: re-run dbt
```

If Streamlit port conflicts, change the port:
```bash
streamlit run /workspace/streamlit/dashboard.py --server.port 8502 --server.address 0.0.0.0
```

---

### Phase 8: Next Steps After MVP

#### Enhancement 1: Add Airflow Orchestration

Add to `docker-compose.yml`:

```yaml
airflow:
  image: apache/airflow:2.9.0
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://polaris:polaris_secret@postgres/airflow
  ports:
    - "8080:8080"
  volumes:
    - ../dlt:/opt/airflow/dlt
    - ../dbt:/opt/airflow/dbt
  command: airflow standalone
  depends_on:
    postgres:
      condition: service_healthy
```

Create a DAG file that calls `load_sales.run()` then shells out to `dbt run`, scheduled with `schedule_interval="0 6 * * *"` (daily at 6 AM).

#### Enhancement 2: Implement RAG Layer

Add to `requirements.txt`:
```
langchain==0.2.0
chromadb==0.5.0
sentence-transformers==3.0.0
```

Index DuckDB table schemas into ChromaDB, then use an LLM to convert natural language to SQL and execute it via DuckDB.

#### Enhancement 3: Add Production Dash Dashboard

Add to `requirements.txt`:
```
dash==2.17.0
dash-bootstrap-components==1.6.0
```

Build a multi-page Dash app with more customization than Streamlit, suited for external/customer-facing dashboards.

#### Enhancement 4: Implement CI/CD

Create `.github/workflows/dbt_test.yml` to run `dbt test` on every pull request, catching data quality regressions before they reach production.

#### Enhancement 5: Multi-Environment Setup

Create `docker-compose.staging.yml` with different port mappings, volume names, and `POLARIS_NAMESPACE=staging_analytics`, so staging and production run independently on the same machine.

#### Enhancement 6: Local LLM via Ollama (RTX 4090 — no API costs)

Install Ollama natively on Windows so it uses the RTX 4090 directly, then call it from Docker containers over the host network.

**Install Ollama on Windows** (PowerShell):

```powershell
# Download and install from ollama.com
winget install Ollama.Ollama
```

**Start Ollama and expose it to Docker containers:**

```powershell
# Bind to all interfaces so Docker containers can reach it
$env:OLLAMA_HOST = "0.0.0.0:11434"
ollama serve
```

**Pull models** (open a second PowerShell tab):

```powershell
# Llama 3 70B at Q4 quantization — fits in 24 GB VRAM
ollama pull llama3:70b

# SQLCoder (fine-tuned text-to-SQL) — fits in 8 GB VRAM
ollama pull defog/sqlcoder-70b-alpha

# Verify GPU is being used
ollama run llama3:70b "Say hello"
# Check GPU utilization: nvidia-smi  (should show GPU memory in use)
```

**Call Ollama from Python inside Docker containers:**

```python
import requests

response = requests.post(
    "http://host.docker.internal:11434/api/generate",
    json={
        "model": "llama3:70b",
        "prompt": "Generate SQL to find total sales by region from the sales table",
        "stream": False,
    }
)
print(response.json()["response"])
```

**Or use the `ollama` Python library:**

```python
# Add to requirements.txt: ollama==0.2.0
import ollama

response = ollama.chat(
    model="llama3:70b",
    messages=[{"role": "user", "content": "Write SQL for total sales by region"}]
)
print(response["message"]["content"])
```

**Update `requirements.txt`** to add GPU-accelerated sentence-transformers:

```
# GPU-accelerated PyTorch (CUDA 12.x) — replaces plain torch
--extra-index-url https://download.pytorch.org/whl/cu121
torch
sentence-transformers==3.0.0
ollama==0.2.0
```

> **RTX 4090 performance:** Llama 3 70B at Q4 runs at ~50–80 tokens/second on the RTX 4090 — fast enough for interactive SQL generation and RAG queries with no per-call API costs.

---

### Phase 9: Performance Optimization

#### Optimization 1: DuckDB Configuration

```python
import duckdb

conn = duckdb.connect("/workspace/db/lakehouse.duckdb")
# Tuned for i9-13900K + 64 GB DDR5 machine (Docker allocated 48 GB)
conn.execute("SET memory_limit='40GB'")   # leave ~8 GB for other containers
conn.execute("SET threads=20")            # match Docker CPU allocation
conn.execute("SET temp_directory='/workspace/tmp'")
# Changes persist for the session; add to a config file for permanence
```

#### Optimization 2: Iceberg Table Maintenance

```python
from pyiceberg.catalog.rest import RestCatalog
from datetime import datetime, timedelta

catalog = RestCatalog(
    name="polaris",
    uri="http://polaris:8181/api/catalog",
    warehouse="lakehouse",
    **{
        "s3.endpoint": "http://seaweedfs-s3:8333",
        "s3.access-key-id": "lakehouse_key",
        "s3.secret-access-key": "lakehouse_secret",
        "s3.path-style-access": "true",
    }
)

table = catalog.load_table("raw.sales")

# Expire snapshots older than 7 days to reclaim storage
table.expire_snapshots(
    older_than=datetime.now() - timedelta(days=7)
).commit()
```

#### Optimization 3: dbt Incremental Models

Switch marts to incremental in `dbt_project.yml`:

```yaml
    marts:
      +materialized: incremental
      +incremental_strategy: delete+insert
```

Add an incremental filter to `daily_revenue.sql`:

```sql
{% if is_incremental() %}
  where order_date >= (select max(order_date) from {{ this }})
{% endif %}
```

This makes dbt only reprocess new dates instead of rebuilding the entire table on every run.

---

### Phase 10: MVP Success Checklist

**GPU Setup**:
- [ ] `nvidia-smi` in PowerShell shows RTX 4090 with driver ≥ 527.41
- [ ] `nvcc --version` in WSL2 shows CUDA 12.6
- [ ] `nvidia-smi` in WSL2 shows RTX 4090 (GPU passthrough working)
- [ ] `docker run --rm --gpus all nvidia/cuda:12.6.0-runtime-ubuntu22.04 nvidia-smi` shows RTX 4090
- [ ] Ollama running on Windows with `OLLAMA_HOST=0.0.0.0:11434`
- [ ] `ollama pull llama3:70b` completed and model responds

**Infrastructure**:
- [ ] All 7 containers show `running` in `docker compose ps`
- [ ] `http://localhost:8889` opens Jupyter Lab
- [ ] `http://localhost:9333` shows SeaweedFS master status
- [ ] `http://localhost:8181/api/management/v1/health` returns 200

**Data Layer**:
- [ ] S3 bucket `lakehouse` exists in SeaweedFS
- [ ] Polaris namespaces `raw`, `staging`, `marts` created
- [ ] `sales.csv` generated with 1,000 rows
- [ ] `raw.sales` in DuckDB has 1,000 rows

**Pipeline**:
- [ ] dlt pipeline runs without errors
- [ ] `dbt debug` shows all green
- [ ] `dbt run` builds both `stg_sales` and `daily_revenue`
- [ ] `dbt test` — all 5 tests pass

**Visualization**:
- [ ] `http://localhost:8501` loads dashboard
- [ ] All 4 KPI metrics show non-zero values
- [ ] Revenue trend, region, and product charts render
- [ ] Date range and region filters update charts

**End-to-End**:
- [ ] Added a new row to `sales.csv`
- [ ] Re-ran dlt pipeline — row count increased to 1,001
- [ ] Re-ran `dbt run` — marts updated
- [ ] Streamlit dashboard reflects the new data

---

## Conclusion: MVP to Production Path

This MVP provides a foundation for a production lakehouse:

**What You've Built**:
- Fully functional lakehouse in Docker
- Complete data pipeline (ingest → transform → visualize)
- Modern table format (Iceberg) with ACID guarantees
- Catalog-based metadata management (Apache Polaris)
- Interactive dashboard for analytics

**Production Readiness Path**:
1. **Weeks 1-2**: Run MVP, load real data, learn the system
2. **Weeks 3-4**: Add real data sources (APIs, databases)
3. **Weeks 5-6**: Expand dbt transformations and tests
4. **Weeks 7-8**: Deploy Airflow for automated scheduling
5. **Weeks 9-10**: Add monitoring and alerting
6. **Weeks 11-12**: Move to cloud or dedicated servers

**When to Graduate from MVP**:
- Data exceeds 500 GB
- Need high availability
- Multiple teams using the system
- Require automated scaling
- Compliance requirements increase

**Migration Strategy**:
- Keep Iceberg tables (portable to any engine)
- Keep dbt models (engine-agnostic SQL)
- Add distributed query engine (Trino) if needed
- Scale storage horizontally (more SeaweedFS nodes)
- Maintain Python-first approach

The MVP proves the architecture works. Now scale confidently knowing your foundation is solid.

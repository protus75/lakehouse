# Python-First Lakehouse Architecture
## High-Level Design for Small Data Teams

---

## Table of Contents

- [Python-First Lakehouse Architecture](#python-first-lakehouse-architecture)
  - [High-Level Design for Small Data Teams](#high-level-design-for-small-data-teams)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Architecture Diagram](#architecture-diagram)
  - [Component Breakdown](#component-breakdown)
    - [1. Storage Layer: SeaweedFS](#1-storage-layer-seaweedfs)
    - [2. Table Format: Apache Iceberg](#2-table-format-apache-iceberg)
    - [3. Catalog Layer: PyIceberg SQL on PostgreSQL](#3-catalog-layer-pyiceberg-sql-on-postgresql)
    - [4. Query Engine: DuckDB](#4-query-engine-duckdb)
    - [5. Ingestion Layer: dlt + Dagster](#5-ingestion-layer-dlt--dagster)
    - [6. Transformation Layer: dbt + dbt-duckdb](#6-transformation-layer-dbt--dbt-duckdb)
    - [7. Visualization Layer: Plotly Dash + Streamlit](#7-visualization-layer-plotly-dash--streamlit)
      - [**Plotly Dash** (Production Browser)](#plotly-dash-production-browser)
      - [**Streamlit** (Rapid Prototyping \& Internal Tools)](#streamlit-rapid-prototyping--internal-tools)
    - [8. AI/RAG Integration Layer](#8-airag-integration-layer)
    - [9. Orchestration Layer: Dagster](#9-orchestration-layer-dagster)
  - [Simplified Governance Strategy](#simplified-governance-strategy)
    - [Layer 1: Catalog (PyIceberg SQL)](#layer-1-catalog-pyiceberg-sql)
    - [Layer 2: Query (DuckDB Users)](#layer-2-query-duckdb-users)
    - [Layer 3: Application (Python Code)](#layer-3-application-python-code)
    - [Layer 4: Data Quality (dbt Tests)](#layer-4-data-quality-dbt-tests)
    - [Layer 5: Lineage (dbt + Dagster)](#layer-5-lineage-dbt--dagster)
    - [What We're NOT Using:](#what-were-not-using)
    - [OpenMetadata (Optional Discovery Tool)](#openmetadata-optional-discovery-tool)
  - [Data Flow](#data-flow)
    - [Ingestion Flow](#ingestion-flow)
    - [Transformation Flow](#transformation-flow)
    - [Query Flow](#query-flow)
    - [RAG Flow](#rag-flow)
  - [Development Workflow](#development-workflow)
    - [Environment Isolation (Iceberg Namespaces)](#environment-isolation-iceberg-namespaces)
    - [Local Development](#local-development)
    - [Staging Deployment](#staging-deployment)
    - [Production Deployment](#production-deployment)
    - [Schema Changes](#schema-changes)
  - [Deployment Patterns](#deployment-patterns)
    - [Docker Compose (Development \& Small Production)](#docker-compose-development--small-production)
    - [Kubernetes (Larger Scale)](#kubernetes-larger-scale)
  - [Technology Summary](#technology-summary)
    - [Pure Python Components (95% of stack)](#pure-python-components-95-of-stack)
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
    - [Development Machine Specifications](#development-machine-specifications)
    - [Phase 1: Windows Docker Setup](#phase-1-windows-docker-setup)
      - [Step 1.1: Install WSL2](#step-11-install-wsl2)
      - [Step 1.2: Configure WSL2 Resources (.wslconfig)](#step-12-configure-wsl2-resources-wslconfig)
      - [Step 1.3: Install Docker Desktop](#step-13-install-docker-desktop)
      - [Step 1.4: Verify NVIDIA Driver (Windows)](#step-14-verify-nvidia-driver-windows)
      - [Step 1.5: Install CUDA Toolkit in WSL2](#step-15-install-cuda-toolkit-in-wsl2)
      - [Step 1.6: Install NVIDIA Container Toolkit (GPU access for Docker)](#step-16-install-nvidia-container-toolkit-gpu-access-for-docker)
      - [Step 1.7: Install Ollama (RTX 4090 Local LLM)](#step-17-install-ollama-rtx-4090-local-llm--no-api-costs)
      - [Step 1.8: Create Project Directory Structure](#step-18-create-project-directory-structure)
      - [Step 1.9: Install Cloudflare Tunnel (Internet Access to Local Services)](#step-19-install-cloudflare-tunnel-internet-access-to-local-services)
    - [Phase 2: Docker Compose Configuration](#phase-2-docker-compose-configuration)
      - [Step 2.1: Create docker-compose.yml](#step-21-create-docker-composeyml)
      - [Step 2.2: Create SeaweedFS S3 Auth Config](#step-22-create-seaweedfs-s3-auth-config)
      - [Step 2.3: Create Dockerfile](#step-23-create-dockerfile)
      - [Step 2.4: Create requirements.txt](#step-24-create-requirementstxt)
    - [Phase 3: Sample Data and Initial Setup](#phase-3-sample-data-and-initial-setup)
      - [Step 3.1: Start All Docker Services](#step-31-start-all-docker-services)
      - [Step 3.2: Create the S3 Bucket](#step-32-create-the-s3-bucket)
      - [Step 3.3: Generate Sample Data](#step-33-generate-sample-data)
      - [Step 3.4: Initialize Polaris Namespaces](#step-34-initialize-polaris-namespaces)
    - [Phase 4: Data Pipeline Implementation](#phase-4-data-pipeline-implementation)
      - [Step 4.1: dlt Ingestion Pipeline](#step-41-dlt-ingestion-pipeline)
      - [Step 4.2: dbt Transformation Project](#step-42-dbt-transformation-project)
    - [Phase 5: Visualization Layer](#phase-5-visualization-layer)
      - [Step 5.1: Streamlit Dashboard](#step-51-streamlit-dashboard)
    - [Phase 6: Smoke Tests](#phase-6-smoke-tests)
      - [Test 1: SeaweedFS Storage](#test-1-seaweedfs-storage)
      - [Test 2: Polaris Catalog](#test-2-polaris-catalog)
      - [Test 3: DuckDB Raw Data](#test-3-duckdb-raw-data)
      - [Test 4: dbt Transformations](#test-4-dbt-transformations)
      - [Test 5: Revenue Aggregation](#test-5-revenue-aggregation)
      - [Test 6: End-to-End Incremental Update](#test-6-end-to-end-incremental-update)
      - [Test 7: Cloudflare Tunnel](#test-7-cloudflare-tunnel)
    - [Phase 7: Troubleshooting Guide](#phase-7-troubleshooting-guide)
      - [Issue 1: A container keeps restarting](#issue-1-a-container-keeps-restarting)
      - [Issue 2: Polaris returns 500 errors](#issue-2-polaris-returns-500-errors)
      - [Issue 3: DuckDB tables not found](#issue-3-duckdb-tables-not-found)
      - [Issue 4: dbt models fail to build](#issue-4-dbt-models-fail-to-build)
      - [Issue 5: Streamlit shows empty charts](#issue-5-streamlit-shows-empty-charts)
    - [Phase 8: Next Steps After MVP](#phase-8-next-steps-after-mvp)
      - [Enhancement 1: Add Airflow Orchestration](#enhancement-1-add-airflow-orchestration)
      - [Enhancement 2: Document Ingestion with Docling](#enhancement-2-document-ingestion-with-docling)
      - [Enhancement 2b: Implement RAG Layer](#enhancement-2b-implement-rag-layer)
      - [Enhancement 3: Add Production Dash Dashboard](#enhancement-3-add-production-dash-dashboard)
      - [Enhancement 4: Implement CI/CD](#enhancement-4-implement-cicd)
      - [Enhancement 5: Multi-Environment Setup](#enhancement-5-multi-environment-setup)
      - [Enhancement 6: Local LLM via Ollama](#enhancement-6-local-llm-via-ollama)
    - [Phase 9: Performance Optimization](#phase-9-performance-optimization)
      - [Optimization 1: DuckDB Configuration](#optimization-1-duckdb-configuration)
      - [Optimization 2: Iceberg Table Maintenance](#optimization-2-iceberg-table-maintenance)
      - [Optimization 3: dbt Incremental Models](#optimization-3-dbt-incremental-models)
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
│              PDFs │ APIs │ Databases │ Files                     │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                 Ingestion Layer (Python)                         │
│           dlt + Marker/pymupdf (PDF) + Dagster                  │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              Transformation Layer (Python)                       │
│                  dbt Core + dbt-duckdb                           │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│          Table Format + Catalog (Pure Python)                    │
│       Apache Iceberg (PyIceberg SQL → PostgreSQL)                │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                  Storage Layer (S3 API)                          │
│                      SeaweedFS                                   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              Query & Analytics (Python)                          │
│            DuckDB (views over Iceberg on S3)                     │
└─────────────────────────────────────────────────────────────────┘
                              ↓
        ┌────────────────────┬─────────────────────┐
        ↓                    ↓                     ↓
┌──────────────────┐ ┌──────────────────┐ ┌────────────────────┐
│  Visualization   │ │  AI Enrichment   │ │  Local LLM         │
│  Dash + Streamlit│ │  Marker+pymupdf  │ │  Ollama@GPU        │
└──────────────────┘ │  Ollama (direct) │ │  (RTX 4090)        │
                     └──────────────────┘ └────────────────────┘
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
- Persistent storage for ChromaDB vectors and parsed document markdown
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

### 3. Catalog Layer: PyIceberg SQL on PostgreSQL

**What It Is**: Pure Python Iceberg catalog backed by PostgreSQL — no Java required

**Why PyIceberg SQL** (replaces Apache Polaris):
- Pure Python — eliminates the only JVM dependency in the stack
- Uses PostgreSQL (already needed for Dagster) as the catalog backend
- Full Iceberg catalog operations: create/drop/list tables and namespaces
- Direct integration with PyIceberg and DuckDB
- Multi-namespace support (bronze/silver/gold isolation)
- Zero additional services to deploy or monitor

**Language**: Pure Python (PyIceberg library + psycopg2 driver)

**Why We Removed Polaris**:
- Polaris was the only Java/JVM service in the stack
- PyIceberg SQL catalog reached maturity and provides all needed functionality
- Reuses the PostgreSQL instance already required for Dagster run storage
- Simpler deployment — one fewer container, one fewer health check
- No REST API overhead — direct Python calls to PostgreSQL

**Role in Stack**:
- Tracks all Iceberg tables and their locations on S3
- Manages table metadata pointers and snapshots
- Supports namespaces: `bronze_tabletop`, `silver_tabletop`, `gold_tabletop`
- Enables atomic catalog operations (create, overwrite, drop)
- All catalog operations via `dlt/lib/iceberg_catalog.py`

**Configuration** (in `config/lakehouse.yaml`):
```yaml
catalog:
  name: lakehouse
  type: sql
  uri: postgresql+psycopg2://iceberg:iceberg_secret@postgres:5432/iceberg
  warehouse: s3://lakehouse/warehouse
  s3.endpoint: http://seaweedfs-s3:8333
  s3.access-key-id: lakehouse_key
  s3.secret-access-key: lakehouse_secret
```

**Governance Approach** (Built-in):
- **Namespace isolation**: bronze/silver/gold separation
- **PostgreSQL access control**: Database-level user permissions
- **Immutable snapshots**: Iceberg tracks all changes
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
- Dash connects via `get_reader()` (DuckDB views over Iceberg on S3)
- Reads Iceberg tables directly from SeaweedFS
- Uses PyIceberg SQL catalog (direct PostgreSQL connection, no REST API)

**Governance Approach** (Built-in):
- **Database-level users**: CREATE USER, password management
- **Schema-level permissions**: GRANT SELECT/INSERT/UPDATE
- **Read-only users**: For dashboards and RAG queries
- **Query logging**: Built-in logging for audit
- No additional governance tools needed

---

### 5. Ingestion Layer: dlt + Dagster

**What It Is**: Python-native data ingestion framework orchestrated by Dagster

**Why dlt**:
- ✅ **Pure Python**: No Java dependency
- ✅ **Covers 90% of use cases**: APIs, databases, files, SaaS, PDFs
- ✅ **Schema inference**: Automatically detects schemas
- ✅ **Incremental loading**: Built-in state management
- ✅ **Simple deployment**: Just Python packages
- ✅ **Developer-friendly**: Code-first, easy to test

**dlt Capabilities**:
- **PDF ingestion**: Marker (layout-aware OCR) + pymupdf (raw text extraction)
- **REST APIs**: Built-in connectors + custom sources
- **Databases**: PostgreSQL, MySQL, SQL Server replication
- **Files**: CSV, JSON, Parquet processing with Python
- **SaaS Platforms**: 50+ verified sources (Salesforce, HubSpot, etc.)
- **Custom logic**: Full Python for complex transformations

**Dagster Role** (replaced Apache Airflow):
- **Asset-based orchestration**: Define data assets with dependencies
- **Monitoring**: Pipeline status via web UI (port 3000)
- **Dependency graph**: Automatic asset lineage
- **Retry logic**: Per-asset retry and error handling
- **Backfills**: Re-materialize specific assets

**Language**: Both pure Python

**Integration Pattern**:
```
Dagster job triggers dlt pipeline (bronze_tabletop asset)
  ↓
dlt extracts data from PDFs via Marker + pymupdf
  ↓
dlt writes to Iceberg tables (via PyIceberg SQL catalog)
  ↓
Tables stored on S3, metadata in PostgreSQL
  ↓
dbt transformations run (dbt_build asset)
  ↓
Silver/gold published to Iceberg (publish_to_iceberg asset)
```

**Governance Approach**:
- **Source credentials**: Stored in environment variables
- **dlt validation**: Data contracts at ingestion
- **Dagster UI**: Control who can trigger pipelines
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

### 7. Visualization Layer: Plotly Dash + Streamlit

**Two Tools, Different Purposes**:

#### **Plotly Dash** (Production Browser)

**What It Is**: Framework for production-grade analytical web applications

**Current Implementation**: The Tabletop Rules Browser (`dashapp/tabletop_browser.py`) is the primary user-facing application — a full scrollable book view with ToC sidebar navigation. It reads exclusively from gold Iceberg tables via DuckDB and serves on port 8000. Publicly accessible at `gamerules.ai` via Cloudflare Tunnel.

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

**Language**: Pure Python (wraps React components)

**Integration**: Reads gold Iceberg tables via `get_reader()` (DuckDB views over S3)

---

#### **Streamlit** (Rapid Prototyping & Internal Tools)

**What It Is**: Python library for building data apps quickly

**When to Use Streamlit**:
- ✅ Rapid prototyping (build in hours)
- ✅ Internal data exploration tools
- ✅ Ad-hoc analysis interfaces
- ✅ Developer tools and admin panels

**Strengths**:
- Extremely fast development (pure Python)
- No HTML/CSS/JavaScript needed
- Auto-reloading on code changes
- Built-in widgets (sliders, dropdowns, file uploads)

**Language**: Pure Python

**Integration**: Connect directly to DuckDB via Python API

---

**Governance Approach**:
- **DuckDB read-only users**: Dashboards use limited permissions
- **Cloudflare Access**: For public-facing Dash apps (free for up to 50 users)
- **Query timeout limits**: Prevent runaway queries
- No additional governance tools needed

---

### 8. AI/RAG Integration Layer

**What It Is**: AI-powered document ingestion pipeline and enrichment layer for lakehouse data

**Components** (All Python):
- **Marker**: Layout-aware PDF to markdown conversion with multi-column support, GPU-accelerated (Datalab, GPL license)
- **PyMuPDF**: Raw PDF text extraction — primary content source (pymupdf page_texts), also provides ToC parsing and page rendering
- **VLM via Ollama**: Vision language model (MiniCPM-V) for structured content extraction from rendered page images
- **Ollama (direct API)**: LLM inference for AI summaries (qwen3:30b-a3b) and annotations (llama3:70b) — called directly, no LangChain wrapper
- **Sentence Transformers**: Generate embeddings locally (all-MiniLM-L6-v2)

**Architecture Pattern — Document Ingestion (Current Implementation)**:
```
PDF ingestion (two-pass hybrid, orchestrated by Dagster):
  Source PDFs (tabletop RPG rulebooks)
    ↓
  Pass 1: Marker parses PDF → layout-aware markdown (multi-column, tables, reading order)
    Cached at documents/tabletop_rules/processed/marker/<book>.md
    ↓
  Pass 2: pymupdf extracts raw page text (primary content source)
    Marker drops pages; pymupdf is authoritative for content
    ↓
  Bronze extraction: ToC parsing, entry building, table extraction, spell lists
    Config-driven via YAML (documents/tabletop_rules/configs/)
    All raw data → Iceberg tables in bronze_tabletop namespace
    ↓
  dbt transforms: silver models (cleanup, dedup, joins) → gold models (entries, index, descriptions)
    ↓
  Publish to Iceberg: silver + gold tables on S3
    ↓
  AI enrichment (Ollama, ~70min total):
    - gold_ai_summaries: qwen3:30b-a3b generates 1-3 sentence summaries per entry
    - gold_ai_annotations: llama3:70b classifies entries as combat/popular
    ↓
  Dash browser reads gold tables at gamerules.ai
```

**VLM Pass (Optional)**:
```
Pages with incomplete structured content (stat blocks, key:value fields)
  → Rendered as images via pymupdf at 300 DPI
  → MiniCPM-V via Ollama extracts missing structured fields
  → Merged back into entry metadata
```

**LLM Models** (all local via Ollama on RTX 4090, zero API costs):
- **qwen3:30b-a3b**: AI summaries — fast, good quality for reference text (num_ctx: 4096, num_predict: 2048)
- **llama3:70b**: AI annotations (combat/popular classification), OCR silver pass
- **llama3:8b**: OCR bronze pass (fast scanning)
- **minicpm-v:latest**: Vision model for structured content extraction from page images
- **glm-4.7-flash**: Available for future use

**Enrichment Prompts** (config-driven, in `_default.yaml` under `gold:`):
- `summary_prompt`: Generates 1-3 sentence mechanic summaries per entry
- `annotation_prompt`: Classifies entries as combat/popular with JSON output
- Prompts are parameterized with `{entry_type}`, `{entry_title}`, `{content}`

**Future RAG Layer** (ChromaDB + Sentence Transformers installed but not yet active):
- ChromaDB for semantic search over document chunks
- Sentence Transformers (all-MiniLM-L6-v2) for embedding generation
- LangChain for RAG orchestration
- FastAPI service for REST query endpoint

**Language**: Pure Python (Marker, pymupdf, ollama, sentence-transformers)

**Governance Approach**:
- **Read-only DuckDB user**: Browser queries can't modify data
- **Gold-only access**: Browser reads only from gold_tabletop namespace
- **Audit logging**: All LLM queries logged via Dagster
- No additional governance tools needed

---

### 9. Local LLM Service: Ollama

**What It Is**: Self-hosted LLM inference engine with GPU acceleration

**Why Ollama as Core Infrastructure**:
- ✅ **Zero API costs**: Run large models locally on RTX 4090
- ✅ **Production-grade**: 70B parameter models in enterprise deployments
- ✅ **Fast inference**: 50-80 tokens/second on RTX 4090 for 70B models
- ✅ **Data privacy**: Models and queries never leave your infrastructure
- ✅ **Simple deployment**: Single executable for Windows, works seamlessly with Docker
- ✅ **Network access**: Accessible from Docker containers via `http://host.docker.internal:11434`

**Language**: Go (but provides standard REST API)

**Hardware Requirements**:
- **RTX 4090 (24 GB VRAM)**: Supports Llama 3 70B at Q4 quantization
- **RTX 4080 / RTX 4070**: Supports smaller models (13B-30B parameters)

**Models in Use** (configured in `config/lakehouse.yaml`):
- **qwen3:30b-a3b**: AI summaries — fast, good quality for reference text
- **llama3:70b**: AI annotations (combat/popular), OCR silver pass
- **llama3:8b**: OCR bronze pass (fast scanning)
- **minicpm-v:latest**: Vision model for structured content extraction
- **glm-4.7-flash**: Available for future use

**Role in Stack**:
- Generate AI summaries for gold entries (qwen3:30b-a3b)
- Classify entries as combat/popular (llama3:70b)
- OCR validation — two-pass: fast scan (llama3:8b) then review (llama3:70b)
- Vision-based content extraction from PDF page images (minicpm-v)

**Why Native Windows (Not Containerized)**:
- Running Ollama natively on Windows is simpler than containerizing it
- Docker on Windows (via WSL2) GPU passthrough adds complexity with marginal benefit
- **Native approach**: Windows NVIDIA driver → GPU directly, no WSL2 passthrough layer
- **Containerized approach**: Docker → WSL2 → NVIDIA Container Toolkit → GPU passthrough (more failure points)
- Models stored on D drive (`D:\ollama\models` via `OLLAMA_MODELS` env var)
- Docker containers reach native Ollama via `http://host.docker.internal:11434`

**Installation (Windows)**:
1. `winget install Ollama.Ollama`
2. Set model storage: `[System.Environment]::SetEnvironmentVariable("OLLAMA_MODELS", "D:\ollama\models", "Machine")`
3. Models pulled automatically by `seed_models` Dagster job
4. Accessible from Docker at `http://host.docker.internal:11434`

**Integration Pattern**:
```
Dagster triggers enrichment asset (gold_ai_summaries or gold_ai_annotations)
  ↓
Python script reads gold entries from Iceberg via get_reader()
  ↓
Direct HTTP POST to Ollama /api/generate (streaming)
  ↓
Ollama runs inference on GPU (RTX 4090)
  ↓
Results written back to Iceberg via write_iceberg()
```

**Cost Comparison**:
- **Cloud LLM** (GPT-4): $0.03 per 1K tokens = $30-60/month for active use
- **Ollama local**: Zero per-query cost; one-time $1,500 GPU investment already made

**Governance Approach**:
- **No telemetry**: Ollama sends no usage data
- **No data leak risk**: All inference stays on GPU
- **Audit logging**: All LLM calls logged via Dagster asset runs
- No additional tools needed

---

### 10. Orchestration Layer: Dagster

**What It Is**: Asset-based data orchestration platform (replaced Apache Airflow)

**Why Dagster** (replaced Airflow):
- ✅ **Asset-based**: Define data assets with dependencies, not task DAGs
- ✅ **Pure Python**: Asset definitions are decorated Python functions
- ✅ **Built-in UI**: Web interface at port 3000 for monitoring and launching
- ✅ **Lightweight**: Two containers (webserver + daemon) vs Airflow's four (webserver + scheduler + worker + triggerer)
- ✅ **dbt integration**: Native dagster-dbt adapter
- ✅ **Run storage**: Uses PostgreSQL (shared with Iceberg catalog)

**Why Not Airflow**:
- Airflow requires more infrastructure (4+ containers, Redis/Celery or Kubernetes executor)
- DAG-based model is task-centric, not data-centric
- Heavier operational burden for a solo developer
- Dagster's asset model matches the lakehouse pattern (data → transforms → consumers)

**Language**: Pure Python

**Role in Stack**:
- Orchestrate the full pipeline: bronze → silver/gold → publish → enrichment
- Run dbt models and tests as assets
- Publish dbt results to Iceberg
- Trigger AI enrichment (summaries + annotations)
- Seed model dependencies (Ollama, HuggingFace, Marker cache)

**Asset Graph** (defined in `dagster/lakehouse_assets/assets.py`):
```
seed_ollama_models    seed_huggingface_models    seed_marker_cache
                              (independent)

bronze_tabletop → toc_review → bronze_ocr_check → dbt_build
  → publish_to_iceberg → dbt_test → gold_ai_summaries → gold_ai_annotations
```

**Jobs**:
- `tabletop_full_pipeline`: All assets bronze through enrichment (~70min)
- `tabletop_without_enrichment`: Bronze through dbt_test (~30s)
- `bronze_and_review`: Bronze extraction + ToC review only
- `silver_and_publish`: dbt_build + publish + test only
- `enrichment_only`: AI summaries + annotations only
- `seed_models`: Pull/validate all model dependencies

**Governance Approach**:
- **Dagster UI**: Control who can launch jobs
- **Run history**: All pipeline runs logged with timestamps and status
- **Asset lineage**: Automatic dependency tracking
- No additional governance tools needed

---

## Simplified Governance Strategy

**Philosophy**: Use built-in features, avoid dedicated governance tools

### Layer 1: Catalog (PyIceberg SQL)
- Namespace isolation (bronze/silver/gold separation)
- PostgreSQL access control (database-level user permissions)
- Immutable Iceberg snapshots (audit trail of all changes)

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

### Layer 5: Lineage (dbt + Dagster)
- dbt automatically tracks lineage
- Dagster asset graph shows dependencies
- Git history for change tracking
- No separate tool needed

### What We're NOT Using:
- ❌ Apache Polaris (replaced with PyIceberg SQL — no JVM needed)
- ❌ Apache Airflow (replaced with Dagster — lighter, asset-based)
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
Source PDFs in documents/tabletop_rules/raw/
  ↓
Dagster triggers bronze_tabletop asset
  ↓
dlt pipeline extracts via Marker (OCR) + pymupdf (text)
  ↓
Writes to Iceberg tables (via PyIceberg SQL → PostgreSQL)
  ↓
Tables stored on S3 (SeaweedFS), metadata in PostgreSQL
  ↓
Available for transformation
```

### Transformation Flow
```
Bronze Iceberg tables (bronze_tabletop namespace)
  ↓
dbt creates DuckDB views over bronze tables (via macro)
  ↓
SQL transformations execute in DuckDB (silver + gold models)
  ↓
publish_to_iceberg asset writes results to S3
  ↓
Silver/gold tables in Iceberg (silver_tabletop, gold_tabletop namespaces)
```

### Query Flow
```
User opens Dash browser at gamerules.ai
  ↓
App calls get_reader(namespaces=["gold_tabletop"])
  ↓
DuckDB creates views over Iceberg tables on S3
  ↓
Results displayed in Dash app
```

### AI Enrichment Flow
```
Dagster triggers gold_ai_summaries asset
  ↓
Script reads gold entries via get_reader()
  ↓
Sends content to Ollama (qwen3:30b-a3b) via direct HTTP
  ↓
Summaries written to Iceberg via write_iceberg()
  ↓
gold_ai_annotations asset runs next (llama3:70b)
  ↓
Annotations written to Iceberg
```

---

## Development Workflow

### Environment Isolation (Iceberg Namespaces)

**Namespaces map to medallion layers** (configured in `config/lakehouse.yaml`):
- `bronze_tabletop`: Raw extracted data from PDFs
- `silver_tabletop`: Cleaned, deduplicated, joined data (dbt transforms)
- `gold_tabletop`: Business-ready tables (entries, index, descriptions, AI enrichment)
- `meta`: Pipeline metadata (dbt test results, test failures)

**Single catalog, multiple namespaces**: PyIceberg SQL catalog on PostgreSQL

### Local Development
1. Edit code on host (VSCode) — Docker volume mounts sync to containers
2. Clear `__pycache__` + restart Dagster containers after code changes
3. Launch pipeline via Dagster UI at http://localhost:3000
4. Verify results in Dash browser at http://localhost:8000

### Schema Changes
1. Modify dbt model SQL
2. Run `tabletop_without_enrichment` job to rebuild silver/gold
3. Verify in Dash browser
4. Iceberg handles schema evolution — old snapshots still readable

---

## Deployment Patterns

### Docker Compose (Development & Small Production)

**Services** (defined in `docker/docker-compose.yml`):
- SeaweedFS (master + volume + filer + S3 gateway — object storage)
- PostgreSQL (Iceberg catalog backend + Dagster run storage)
- Dagster webserver (port 3000) + daemon (pipeline execution)
- Workspace container (Jupyter, Dash browser, GPU access)
- DuckDB (embedded in applications, no service needed)
- Ollama (native Windows, not containerized)
- Cloudflare Tunnel (`cloudflared`) for exposing Dash at gamerules.ai

**Single Machine Requirements**:
- 32-64 GB RAM (for DuckDB in-memory processing) — *this machine: 64 GB DDR5 @ 6000 MT/s*
- 500 GB–2 TB SSD (for local data caching) — *this machine: 2× 1 TB Samsung 990 PRO PCIe 4.0 NVMe*
- 4-8 CPU cores — *this machine: i9-13900K 24-core (8P + 16E)*
- GPU optional for local LLMs — *this machine: RTX 4090 24 GB VRAM (can run 70B parameter models locally)*

### Kubernetes (Larger Scale)

**Deployments**:
- SeaweedFS Operator
- PostgreSQL (for Iceberg catalog + Dagster)
- Dagster Helm Chart (dagster-cloud or self-hosted)
- Dash apps (multiple replicas)
- RAG API (multiple replicas with ChromaDB PVC)

**DuckDB Consideration**: Still runs embedded in apps (not clustered)

---

## Technology Summary

### Pure Python Components (95% of stack)
- dlt (ingestion)
- dbt (transformation)
- DuckDB (queries)
- PyIceberg (Iceberg catalog + table format)
- Dagster (orchestration)
- Plotly Dash (production browser)
- Streamlit (rapid prototyping)
- Marker (PDF OCR, GPU-accelerated)
- pymupdf (PDF text extraction)
- Sentence Transformers (embeddings)
- ollama Python client (LLM calls)

### Non-Python (with justification)
- **SeaweedFS** (Go): S3-compatible storage, language-agnostic API
- **PostgreSQL** (C): Catalog backend + Dagster run storage, standard infrastructure
- **DuckDB core** (C++): Embedded database, feels native to Python
- **Ollama** (Go): Self-hosted LLM inference, REST API, zero API costs, GPU acceleration
- **Cloudflare Tunnel** (Go): Expose local services to internet without port forwarding, free tier

### Why This Works
- Single language for development (Python)
- **Zero JVM dependencies** — Polaris (Java) was replaced with PyIceberg SQL, Airflow replaced with Dagster
- Ollama runs natively on Windows (not in Docker), accessible to all services
- Simpler debugging and monitoring
- Unified toolchain (pytest, black, ruff, mypy)
- Easier hiring (Python skills only)
- Lower infrastructure costs (fewer services, no API costs for LLMs)

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
- PyIceberg SQL → Polaris REST catalog (if multi-engine access needed)
- Dagster → Dagster Cloud (hosted orchestration)
- Built-in governance → Ranger (add centralized policies)
- All layers remain compatible (Iceberg abstraction)

---

## Why Python-First Matters

### Operational Benefits
- **Single runtime**: Python everywhere (no JVM services)
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
- **Lower memory**: No JVM overhead at all
- **Smaller VMs**: Python apps have lower baseline resource needs
- **Simplified licensing**: All open source, no enterprise editions

### Trade-offs Accepted
- **DuckDB limits**: Single-node only, but sufficient for scale
- **No real-time**: Batch only, but 90% of analytics is batch
- **DIY governance**: No Ranger, but built-in tools sufficient
- **Zero JVM**: Achieved by replacing Polaris with PyIceberg SQL

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
- Use PostgreSQL access control for catalog
- Create read-only DuckDB users for dashboards
- Store secrets in environment variables and config files
- Enable audit logging in Dagster
- Use namespace isolation for data layers (bronze/silver/gold)

### Monitoring
- Dagster UI for pipeline health and run history
- dbt test results for data quality (stored in Iceberg)
- DuckDB query logs for performance
- Dash app at gamerules.ai for content verification
- Ollama model status via API (`/api/ps`)

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
- **Zero JVM dependencies** — PyIceberg SQL replaced Polaris, Dagster replaced Airflow
- Built-in governance (no dedicated tools)
- Embedded query engine (no cluster management)

**Appropriate for Small Scale**:
- DuckDB handles 100GB-2TB efficiently
- dlt covers all ingestion needs
- PyIceberg SQL on PostgreSQL provides production-ready catalog
- Dash delivers production browser (gamerules.ai)
- Dagster orchestrates the full pipeline

**Future-Proof**:
- Iceberg tables work with any query engine
- Can add Trino later without data migration
- Standard tools (dbt, Dagster) scale to enterprise
- RAG layer (ChromaDB + LangChain) ready to activate

**Cost-Effective**:
- Fewer servers (DuckDB embedded)
- Lower operational burden (fewer tools)
- Open source (no licensing costs)
- Local LLMs via Ollama (no API fees)

**Developer-Friendly**:
- Python skills only
- Fast iteration cycles
- Version-controlled everything
- Easy debugging and testing

This stack proves you can build a production lakehouse with modern capabilities (ACID transactions, time travel, AI enrichment) without the complexity and cost of traditional big data architectures.

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

# Release WSL2 RAM back to Windows immediately when idle
# "dropcache" is more aggressive than "gradual" — drops Linux page cache on idle
# Critical for ML workloads (Marker, Sentence Transformers) that allocate large
# amounts of memory then release it, but WSL2 holds onto the pages
autoMemoryReclaim=dropcache

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
| NVIDIA-SMI 595.xx    Driver Version: 595.xx    CUDA Version: 13.x          |
|-------------------------------+----------------------+----------------------+
| GPU 0: NVIDIA GeForce RTX 4090 ...
```

**Current driver:** 595.97, CUDA 13.2, PyTorch 2.11+cu130.

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

#### Step 1.7: Install Ollama (RTX 4090 Local LLM — No API Costs)

Ollama runs natively on Windows and uses the RTX 4090 directly. Docker containers reach it via `http://host.docker.internal:11434`.

**Install Ollama** (PowerShell):

```powershell
winget install Ollama.Ollama
```

Close and reopen PowerShell after install.

**Move model storage to D drive** (PowerShell as Administrator) — Ollama defaults to `C:\Users\<user>\.ollama\models` which fills the OS drive. Set the system environment variable so all models are stored on D:

```powershell
[System.Environment]::SetEnvironmentVariable("OLLAMA_MODELS", "D:\ollama\models", "Machine")
```

Close and reopen PowerShell after setting this.

**Start Ollama** — it runs as a Windows service automatically after install. If you need to restart:

```powershell
$env:OLLAMA_HOST = "0.0.0.0:11434"
ollama serve
```

**Pull models** (or use the `seed_models` Dagster job after setup):

```powershell
ollama pull qwen3:30b-a3b
ollama pull llama3:70b
ollama pull llama3:8b
ollama pull minicpm-v:latest
```

**Verify GPU is being used**:

```powershell
curl http://localhost:11434/api/ps
# Expected: size_vram > 0 for loaded model
```

> **RTX 4090 performance:** qwen3:30b-a3b runs at ~80+ tokens/second; llama3:70b at ~50–80 tokens/second.

**Test from inside a Docker container** (after Phase 2 setup):

```bash
curl http://host.docker.internal:11434/api/tags
```

Should return a JSON list including all four models.

---

#### Step 1.8: Create Project Directory Structure

Open PowerShell and run:

```powershell
New-Item -ItemType Directory -Force -Path D:\source\lakehouse\lakehouse
cd D:\source\lakehouse\lakehouse
mkdir docker, data, dlt, dbt, streamlit, rag, storage, documents, chroma_db
```

Expected structure:
```
D:\source\lakehouse\lakehouse\
├── docker\        ← Docker configs, Dockerfile, requirements.txt
├── data\          ← Sample CSV files
├── dlt\           ← dlt pipeline scripts
├── dbt\           ← dbt project
├── streamlit\     ← Streamlit dashboard
├── rag\           ← RAG document ingestion, embedding, query, and API
├── storage\       ← SeaweedFS volume data (auto-populated)
├── documents\     ← Source PDFs and documents for Docling ingestion
└── chroma_db\     ← ChromaDB vectors (for RAG)
```

---

#### Step 1.9: Install Cloudflare Tunnel (Internet Access to Local Services)

Cloudflare Tunnel (`cloudflared`) exposes local services (Dash, Streamlit, Dagster) to the internet without port forwarding or router configuration. Free tier, no domain required.

**Install cloudflared** (PowerShell):

```powershell
winget install Cloudflare.cloudflared
```

Close and reopen PowerShell after install.

**Verify installation**:

```powershell
cloudflared --version
```

Should print a version like `cloudflared version 2024.x.x`.

**Quick test — temporary public URL** (no account needed):

```powershell
cloudflared tunnel --url http://localhost:8000
```

This prints a `https://xxx.trycloudflare.com` URL. Anyone with the link can access the service. Press Ctrl+C to stop.

**Named tunnel with custom domain** (requires free Cloudflare account + domain):

```powershell
cloudflared tunnel login
cloudflared tunnel create tabletop
cloudflared tunnel route dns tabletop browser.yourdomain.com
cloudflared tunnel run --url http://localhost:8000 tabletop
```

> **Security note**: Services exposed via tunnel have no auth by default. Add [Cloudflare Access](https://developers.cloudflare.com/cloudflare-one/policies/access/) (free for up to 50 users) to restrict access by email or SSO.

---

### Phase 2: Docker Compose Configuration

All files in this phase go in `D:\source\lakehouse\lakehouse\docker\`.

#### Step 2.1: Create docker-compose.yml

Create `D:\source\lakehouse\lakehouse\docker\docker-compose.yml`:

```yaml
services:

  # ── PostgreSQL ──────────────────────────────────────────────
  # Backend for PyIceberg SQL catalog + Dagster run storage
  postgres:
    image: postgres:15
    container_name: lakehouse-postgres
    environment:
      POSTGRES_DB: iceberg
      POSTGRES_USER: iceberg
      POSTGRES_PASSWORD: iceberg_secret
    ports:
      - "5432:5432"
    volumes:
      - ../db/postgres:/var/lib/postgresql/data
      - ./init-postgres.sh:/docker-entrypoint-initdb.d/init-postgres.sh
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U iceberg"]
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

  # ── Dagster ────────────────────────────────────────────────
  # Asset-based orchestration for the lakehouse pipeline
  # All three Python services share the same image (base + workspace).
  dagster-webserver:
    image: lakehouse-workspace:latest
    container_name: lakehouse-dagster-webserver
    ports:
      - "3000:3000"
    volumes:
      - ../config:/workspace/config
      - ../dagster:/workspace/dagster
      - ../dlt:/workspace/dlt
      - ../dbt:/workspace/dbt
      - ../scripts:/workspace/scripts
      - ../documents:/workspace/documents
      - ../db/duckdb:/workspace/db
      - ../cache:/workspace/cache
    environment:
      DAGSTER_HOME: /workspace/dagster
      PYTHONPATH: /workspace
      S3_ENDPOINT: http://seaweedfs-s3:8333
      S3_ACCESS_KEY: lakehouse_key
      S3_SECRET_KEY: lakehouse_secret
    command: dagster-webserver -h 0.0.0.0 -p 3000 -w /workspace/dagster/workspace.yaml
    depends_on:
      postgres:
        condition: service_healthy

  dagster-daemon:
    image: lakehouse-workspace:latest
    container_name: lakehouse-dagster-daemon
    volumes:
      - ../config:/workspace/config
      - ../dagster:/workspace/dagster
      - ../dlt:/workspace/dlt
      - ../dbt:/workspace/dbt
      - ../scripts:/workspace/scripts
      - ../documents:/workspace/documents
      - ../db/duckdb:/workspace/db
      - ../cache:/workspace/cache
      - ../cache/datalab:/root/.cache/datalab
    environment:
      DAGSTER_HOME: /workspace/dagster
      PYTHONPATH: /workspace
      HF_HOME: /workspace/cache/huggingface
      TRANSFORMERS_OFFLINE: "1"
      HF_HUB_OFFLINE: "1"
      S3_ENDPOINT: http://seaweedfs-s3:8333
      S3_ACCESS_KEY: lakehouse_key
      S3_SECRET_KEY: lakehouse_secret
    command: dagster-daemon run -w /workspace/dagster/workspace.yaml
    depends_on:
      dagster-webserver:
        condition: service_started
      postgres:
        condition: service_healthy

  # ── Python Workspace ────────────────────────────────────────
  # Interactive: Jupyter Lab, Dash browser, Streamlit, manual scripts
  workspace:
    build:
      context: .
      dockerfile: Dockerfile.workspace
    image: lakehouse-workspace:latest
    container_name: lakehouse-workspace
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: all
              capabilities: [gpu]
    ports:
      - "8889:8889"    # Jupyter Lab
      - "8501:8501"    # Streamlit
      - "8000:8000"    # Dash browser
    volumes:
      - ../config:/workspace/config
      - ../data:/workspace/data
      - ../dlt:/workspace/dlt
      - ../dbt:/workspace/dbt
      - ../streamlit:/workspace/streamlit
      - ../dashapp:/workspace/dashapp
      - ../rag:/workspace/rag
      - ../documents:/workspace/documents
      - ../scripts:/workspace/scripts
      - ../chroma_db:/workspace/chroma_db
      - ../db/duckdb:/workspace/db
      - ../dagster:/workspace/dagster
      - ../cache:/workspace/cache
      - ../cache/datalab:/root/.cache/datalab
    environment:
      HF_HOME: /workspace/cache/huggingface
      S3_ENDPOINT: http://seaweedfs-s3:8333
      S3_ACCESS_KEY: lakehouse_key
      S3_SECRET_KEY: lakehouse_secret
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
      postgres:
        condition: service_healthy

networks:
  default:
    name: lakehouse-network
```

---

#### Step 2.2: Create SeaweedFS S3 Auth Config

Create `D:\source\lakehouse\lakehouse\docker\s3.json`:

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

#### Step 2.3: Create Dockerfiles (two-image setup)

The Docker setup uses two images: a **base** image with shared packages that rarely change, and a **workspace** image that adds Dagster, dbt, PDF parsing, and AI tools.

Create `D:\source\lakehouse\lakehouse\docker\Dockerfile.base`:

```dockerfile
FROM python:3.11-slim

WORKDIR /workspace

RUN mkdir -p /workspace/cache

RUN apt-get update && apt-get install -y \
    curl \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-base.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements-base.txt
```

Create `D:\source\lakehouse\lakehouse\docker\Dockerfile.workspace`:

```dockerfile
FROM lakehouse-base:latest

WORKDIR /workspace

COPY requirements-workspace.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements-workspace.txt

EXPOSE 3000 8889 8501 8000
```

All three services (dagster-webserver, dagster-daemon, workspace) use `lakehouse-workspace:latest`.

---

#### Step 2.4: Create requirements files

Create `D:\source\lakehouse\lakehouse\docker\requirements-base.txt`:

```
# Base packages — shared by daemon and workspace images.
# Heavy dependencies that rarely change.

# Data ingestion
dlt[duckdb,filesystem]==1.21.0

# Query engine
duckdb==1.4.4

# Table format (with SQL catalog on PostgreSQL)
pyiceberg[s3fs,duckdb,sql-postgres]==0.11.1

# Data processing
polars==1.30.0
pyarrow==23.0.1

# S3 client
boto3==1.42.50
s3fs==2026.2.0

# Database drivers
psycopg2-binary==2.9.10

# Utilities
requests==2.32.5
python-dotenv==1.2.1
pyyaml==6.0.2

# OCR validation
pyspellchecker==0.8.1
```

Create `D:\source\lakehouse\lakehouse\docker\requirements-workspace.txt`:

```
# Workspace packages — installed on top of base image.
# Used by all services: dagster webserver, daemon, and workspace.

# Orchestration
dagster==1.12.20
dagster-webserver==1.12.20
dagster-dbt==0.28.20
dagster-duckdb==0.28.20
dagster-docker==0.28.20
dagster-postgres==0.28.20

# Transformation
dbt-core==1.11.5
dbt-duckdb==1.10.0

# Jupyter
jupyterlab==4.5.4
ipykernel==7.2.0

# PDF parsing
docling==2.31.0
pymupdf==1.25.4
marker-pdf==1.10.2

# Visualization
streamlit==1.54.0
dash==3.0.4
plotly==6.5.2

# RAG layer (installed, not yet active)
langchain==1.2.10
langchain-community==0.4.1
chromadb==1.0.0
sentence-transformers==5.2.2
fastapi==0.115.9
uvicorn==0.34.0
ollama==0.6.1
```

---

### Phase 3: Sample Data and Initial Setup

#### Step 3.1: Start All Docker Services

Open PowerShell in `D:\source\lakehouse\lakehouse\docker\` and run:

```powershell
cd D:\source\lakehouse\lakehouse\docker

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

#### Step 3.4: Initialize PyIceberg SQL Catalog and Namespaces

The PyIceberg SQL catalog stores metadata directly in PostgreSQL — no Java service needed. Namespaces are created automatically by the pipeline, but you can verify manually:

```python
from pyiceberg.catalog.sql import SqlCatalog

catalog = SqlCatalog(
    "lakehouse",
    **{
        "uri": "postgresql+psycopg2://iceberg:iceberg_secret@postgres:5432/iceberg",
        "warehouse": "s3://lakehouse/warehouse",
        "s3.endpoint": "http://seaweedfs-s3:8333",
        "s3.access-key-id": "lakehouse_key",
        "s3.secret-access-key": "lakehouse_secret",
        "s3.region": "us-east-1",
    }
)

# Create namespaces for the medallion architecture
for ns in ["bronze_tabletop", "silver_tabletop", "gold_tabletop", "meta"]:
    try:
        catalog.create_namespace(ns)
        print(f"Created namespace: {ns}")
    except Exception as e:
        print(f"Namespace already exists: {ns} ({e})")

print("All namespaces:", catalog.list_namespaces())
```

> **Note**: In practice, `dlt/lib/iceberg_catalog.py` handles all catalog operations. The `write_iceberg()` function creates namespaces and tables automatically. This manual step is only needed for initial verification.

---

### Phase 4: Data Pipeline Implementation

#### Step 4.1: Bronze Ingestion Pipeline

The bronze pipeline (`dlt/bronze_tabletop_rules.py`) extracts content from PDFs using two passes:

1. **Marker**: Layout-aware OCR → markdown (cached at `documents/tabletop_rules/processed/marker/`)
2. **pymupdf**: Raw page text extraction (primary content source)

The pipeline produces these bronze Iceberg tables:
- `files` — source file metadata and config hash (used for skip logic)
- `marker_extractions` — Marker markdown output
- `page_texts` — pymupdf raw page text (authoritative content)
- `toc_raw` — parsed table of contents
- `known_entries_raw` — entries from authority tables and indexes
- `spell_list_entries` — spell lists from PDF layout analysis
- `tables_raw` — extracted tables
- `authority_table_entries` — ground-truth entry names from tables
- `watermarks` — detected watermark text
- `ocr_issues` — spellcheck-flagged OCR errors
- `validation_results` — bronze validation checks

**Run via Dagster** (never manually):
1. Open http://localhost:3000
2. Launch `tabletop_without_enrichment` job
3. Monitor asset materialization in the UI

> **IMPORTANT**: Clear `__pycache__` and restart Dagster containers before every pipeline run. See `CLAUDE.md` for the full reset sequence.

---

#### Step 4.2: dbt Transformation Project

The dbt project (`dbt/lakehouse_mvp/`) transforms bronze data into silver and gold layers:

**Model structure**:
```
dbt/lakehouse_mvp/models/tabletop/
├── silver/          # Cleaned, deduplicated, joined tables
│   ├── silver_toc.sql
│   ├── silver_entries.sql
│   ├── silver_chunks.sql
│   ├── silver_spell_meta.sql
│   └── silver_entry_descriptions.sql
└── gold/            # Business-ready tables for consumers
    ├── gold_toc.sql
    ├── gold_entries.sql
    ├── gold_entry_index.sql
    └── gold_entry_descriptions.sql
```

**Key design decisions**:
- A `create_bronze_views.sql` macro creates DuckDB views over bronze Iceberg tables on S3
- Silver models clean, dedup, and join bronze data
- Gold models produce the final shape consumed by the Dash browser
- All thresholds and patterns are config-driven (YAML), never hardcoded
- 41+ dbt tests validate data quality

**dbt runs as a Dagster asset** (`dbt_build`), not manually. After dbt builds to DuckDB, `publish_to_iceberg` writes silver/gold tables to Iceberg on S3. Then `dbt_test` runs tests and stores results in `meta.dbt_test_results` on S3.

**Pipeline** (all via Dagster):
```
bronze_tabletop → toc_review → bronze_ocr_check → dbt_build
  → publish_to_iceberg → dbt_test → (optional) gold_ai_summaries → gold_ai_annotations
```

---

### Phase 5: Visualization Layer

#### Step 5.1: Dash Tabletop Rules Browser

The primary user-facing application is the Tabletop Rules Browser (`dashapp/tabletop_browser.py`), a Plotly Dash app that renders the full Player's Handbook as a scrollable web page with ToC sidebar navigation.

**Key features**:
- Reads exclusively from `gold_tabletop` namespace via `get_reader()`
- Full scrollable book view with entries ordered by `sort_order`
- ToC sidebar with anchor links to each section
- AI summary toggle (swap between AI summary and full content)
- No-cache HTTP headers for development
- Publicly accessible at http://gamerules.ai via Cloudflare Tunnel

**Running the Dash app**:
```bash
# Inside lakehouse-workspace container
python /workspace/dashapp/tabletop_browser.py
# Serves on http://localhost:8000
```

**Exposing to the internet**:
```powershell
# On Windows host — managed via scripts/tunnel.py
python scripts/tunnel.py
```

> **IMPORTANT**: The browser must NEVER access silver tables — gold only. Any browser code change must use `get_reader(namespaces=["gold_tabletop"])`.

---

### Phase 6: Smoke Tests

Run each test inside the `lakehouse-workspace` container to verify every layer works.

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

#### Test 2: PyIceberg SQL Catalog

```python
from pyiceberg.catalog.sql import SqlCatalog

catalog = SqlCatalog(
    "lakehouse",
    **{
        "uri": "postgresql+psycopg2://iceberg:iceberg_secret@postgres:5432/iceberg",
        "warehouse": "s3://lakehouse/warehouse",
        "s3.endpoint": "http://seaweedfs-s3:8333",
        "s3.access-key-id": "lakehouse_key",
        "s3.secret-access-key": "lakehouse_secret",
        "s3.region": "us-east-1",
    }
)

namespaces = catalog.list_namespaces()
print(f"PASS: PyIceberg SQL catalog connected, namespaces: {namespaces}")
```

#### Test 3: Dagster UI

```powershell
# From Windows host
curl http://localhost:3000/health
# Expected: 200 OK
```

Open http://localhost:3000 in your browser. You should see the Dagster asset graph.

#### Test 4: DuckDB Reads Iceberg Tables

```python
# After running the pipeline at least once
from dlt.lib.duckdb_reader import get_reader

reader = get_reader(namespaces=["gold_tabletop"])
result = reader.execute("SELECT COUNT(*) as cnt FROM gold_entries").fetchone()
print(f"PASS: gold_entries has {result[0]} rows")
reader.close()
```

#### Test 5: Dash Browser

```powershell
# From Windows host — after starting the Dash app in the workspace container
curl http://localhost:8000
# Expected: HTML response with the Tabletop Rules Browser
```

Open http://localhost:8000 in your browser. You should see the Player's Handbook content.

#### Test 6: Cloudflare Tunnel

```powershell
# Verify cloudflared is installed
cloudflared --version

# Start the named tunnel (configured in config/lakehouse.yaml)
python scripts/tunnel.py
# Should connect gamerules.ai to localhost:8000
```

#### Test 7: Ollama

```powershell
# From Windows host
curl http://localhost:11434/api/tags
# Expected: JSON list of available models including qwen3:30b-a3b, llama3:70b
```

---

### Phase 7: Troubleshooting Guide

#### Issue 1: A container keeps restarting

```powershell
# See the error for a specific container
docker logs lakehouse-dagster-daemon --tail 50
docker logs lakehouse-weed-master --tail 30

# Check for port conflicts
netstat -ano | findstr "3000"
netstat -ano | findstr "9333"

# Rebuild from scratch (keeps data)
cd D:\source\lakehouse\lakehouse\docker
docker compose down
docker compose up -d --build

# Nuclear reset (deletes all data, start fresh)
docker compose down -v
docker compose up -d --build
```

#### Issue 2: Dagster pipeline uses stale code

After any Python code change, you MUST clear caches and restart:

```powershell
# 1. Clear host pycache
find d:/source/lakehouse/lakehouse -name '__pycache__' -exec rm -rf {} +

# 2. Clear container pycache
docker exec lakehouse-dagster-daemon bash -c 'find /workspace -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null'

# 3. Restart BOTH Dagster containers
docker restart lakehouse-dagster-daemon lakehouse-dagster-webserver

# 4. Wait 15 seconds for gRPC servers to start
# 5. Then launch pipeline via http://localhost:3000
```

#### Issue 3: Iceberg tables not found

```python
# List all tables in the catalog
from dlt.lib.iceberg_catalog import get_catalog
catalog = get_catalog()
for ns in catalog.list_namespaces():
    tables = catalog.list_tables(ns[0])
    print(f"{ns[0]}: {[t[1] for t in tables]}")
```

- Bronze tables missing → re-run `tabletop_without_enrichment` job in Dagster
- Silver/gold tables missing → check if `publish_to_iceberg` asset succeeded

#### Issue 4: dbt models fail to build

```bash
# Inside container
cd /workspace/dbt/lakehouse_mvp

# Check connection first
dbt debug

# See the compiled SQL that failed
cat target/compiled/lakehouse_mvp/models/tabletop/silver/silver_toc.sql

# Run only one model with verbose output
dbt run --select silver_toc --debug
```

#### Issue 5: GPU not available for Marker/AI enrichment

```powershell
# Verify GPU works in workspace container
docker exec lakehouse-workspace python -c "import torch; print(torch.cuda.is_available(), torch.cuda.get_device_name(0))"
# Expected: True NVIDIA GeForce RTX 4090

# If False, try WSL GPU fix:
wsl --shutdown
# Then restart Docker Desktop
```

#### Issue 6: Ollama not responding

```powershell
# Check if Ollama is running on Windows host
curl http://localhost:11434/api/tags

# Check from inside container
docker exec lakehouse-workspace curl http://host.docker.internal:11434/api/tags

# Restart Ollama if needed
taskkill /IM ollama.exe /F
$env:OLLAMA_HOST = "0.0.0.0:11434"
ollama serve
```

---

### Phase 8: Next Steps After MVP

#### ~~Enhancement 1: Orchestration~~ — DONE

Dagster replaced Airflow. Asset-based orchestration with webserver (port 3000) and daemon. All pipeline steps are Dagster assets. See [Section 10](#10-orchestration-layer-dagster).

#### ~~Enhancement 2: Document Ingestion~~ — DONE

Marker + pymupdf replaced Docling. Two-pass PDF extraction with config-driven entry building. Full bronze pipeline with 11 Iceberg tables. See [Section 8](#8-airag-integration-layer).

#### ~~Enhancement 3: Production Dash Dashboard~~ — DONE

Tabletop Rules Browser at `dashapp/tabletop_browser.py`, served at http://localhost:8000, publicly accessible at http://gamerules.ai via Cloudflare Tunnel. Reads exclusively from gold_tabletop namespace.

#### ~~Enhancement 4: AI Enrichment~~ — DONE

AI summaries (qwen3:30b-a3b) and annotations (llama3:70b) via direct Ollama API calls. Orchestrated as Dagster assets (`gold_ai_summaries`, `gold_ai_annotations`). ~70 minutes for full enrichment run.

#### ~~Enhancement 5: Stable Integer Keys~~ — DONE

Hash-based stable int64 IDs (entry_id, toc_id, chunk_id) via `dlt/lib/stable_keys.py`. Same entity gets the same ID across rebuilds. SHA-256 based.

#### Enhancement 6: RAG Layer (Future)

ChromaDB, LangChain, Sentence Transformers, and FastAPI are installed but not yet active. When ready:
- Embed gold entry chunks into ChromaDB for semantic search
- Use DuckDB full-text search for exact keyword matching
- Combine both retrieval methods for rules-context AI queries
- Expose via FastAPI REST endpoint

#### Enhancement 7: Multi-Project Support (Future)

Refactor `lakehouse.yaml` to a `projects:` dict keyed by project name. Each project gets its own namespaces, paths, configs, bronze pipeline, dbt models, and Dagster asset group. See `.claude-memory/project_multi_project_plan.md`.

#### Enhancement 8: CI/CD (Future)

Create `.github/workflows/dbt_test.yml` to run `dbt test` on every pull request, catching data quality regressions before they reach production.

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
    credential="root:s3cr3t",
    scope="PRINCIPAL_ROLE:ALL",
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
- [ ] `nvidia-smi` in PowerShell shows RTX 4090 with driver ≥ 595 (CUDA 13.2)
- [ ] `nvcc --version` in WSL2 shows CUDA toolkit
- [ ] `docker run --rm --gpus all nvidia/cuda:12.6.0-runtime-ubuntu22.04 nvidia-smi` shows RTX 4090
- [ ] Ollama running on Windows, models at `D:\ollama\models`
- [ ] `curl http://localhost:11434/api/tags` lists all models

**Infrastructure**:
- [ ] All containers show `running` in `docker compose ps` (postgres, 4x seaweedfs, dagster-webserver, dagster-daemon, workspace)
- [ ] `http://localhost:3000` opens Dagster UI
- [ ] `http://localhost:9333` shows SeaweedFS master status
- [ ] PostgreSQL healthy: `docker exec lakehouse-postgres pg_isready -U iceberg`

**Data Layer**:
- [ ] S3 bucket `lakehouse` exists in SeaweedFS
- [ ] PyIceberg SQL catalog connects to PostgreSQL
- [ ] Namespaces `bronze_tabletop`, `silver_tabletop`, `gold_tabletop` exist
- [ ] Source PDFs in `documents/tabletop_rules/raw/`
- [ ] Marker cache in `documents/tabletop_rules/processed/marker/`

**Pipeline** (all via Dagster at http://localhost:3000):
- [ ] `seed_models` job passes (Ollama, HuggingFace, Marker cache)
- [ ] `tabletop_without_enrichment` job completes successfully
- [ ] Bronze Iceberg tables populated (page_texts, toc_raw, etc.)
- [ ] dbt models build (silver + gold)
- [ ] All dbt tests pass (41+)
- [ ] Silver/gold tables published to Iceberg on S3

**Visualization**:
- [ ] `http://localhost:8000` loads Dash Tabletop Rules Browser
- [ ] ToC sidebar shows chapters and sections
- [ ] Content renders with entries in correct order
- [ ] AI summaries display when toggle is enabled

**AI Enrichment** (optional, ~70 minutes):
- [ ] `enrichment_only` job completes
- [ ] AI summaries in `gold_entry_descriptions` table
- [ ] AI annotations (combat/popular) in `gold_ai_annotations` table

**Public Access**:
- [ ] Cloudflare Tunnel connects gamerules.ai to localhost:8000
- [ ] http://gamerules.ai loads the browser

---

## Conclusion: Current State and Next Steps

This is no longer an MVP — it's a working production system:

**What's Running**:
- Full lakehouse pipeline: PDF → Bronze → Silver → Gold → AI Enrichment
- Dagster orchestration with asset-based pipeline management
- PyIceberg SQL catalog on PostgreSQL (zero JVM)
- Iceberg tables on SeaweedFS S3 with DuckDB query views
- 41+ dbt tests passing
- Dash browser publicly accessible at gamerules.ai
- AI summaries and annotations via local Ollama (qwen3:30b-a3b, llama3:70b)
- Stable hash-based integer keys across all layers

**Current Focus**:
- Player's Handbook (PHB) — one book until all validation passes
- Content quality spot-checks in the browser
- Re-run AI enrichment after entry builder fixes

**Next Steps**:
1. Activate RAG layer (ChromaDB + Sentence Transformers + FastAPI)
2. Multi-project support (refactor config for second book)
3. Improve Dagster DX (reduce cache/restart friction)
4. CI/CD for dbt test regressions

**Migration Strategy** (when scaling is needed):
- Keep Iceberg tables (portable to any engine)
- Keep dbt models (engine-agnostic SQL)
- Add distributed query engine (Trino) if needed
- Scale storage horizontally (more SeaweedFS nodes)
- Maintain Python-first approach

The architecture works. Scale confidently knowing the foundation is solid.

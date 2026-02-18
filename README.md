## Overview

This project implements a parallel news aggregator in Java. It processes a large volume of news articles from JSON files, organizes them by category and language, removes duplicates, and generates statistical reports. The solution utilizes a fixed pool of worker threads to achieve parallelism while ensuring deterministic output and thread safety.

## Features

- Parallel processing of JSON article files using Java Threads
- Duplicate removal based on UUID and Title
- Categorization and language filtering based on configuration files
- Keyword frequency analysis for English articles
- Generation of structured output files (categories, languages, global list, reports)
- Scalable architecture with synchronized shared data structures

## Implementation Strategy

The parallelization strategy divides the workload among a fixed number of threads created at startup.

- Task Distribution: The main thread reads file paths and splits the list of articles into intervals assigned to worker threads.
- Local Processing: Each thread parses and processes its assigned articles into local lists, handling initial deduplication.
- Synchronization: Two CyclicBarriers are used to coordinate phases:
  - barrierRead: Ensures all threads complete parsing before merging data.
  - barrierWrite: Synchronizes threads after data aggregation; includes a barrier action for final file writing executed by the last arriving thread.
- Thread-Safe Structures: 
  - ConcurrentHashMap for shared statistics (e.g., author counts).
  - ConcurrentSkipListSet for maintaining ordered unique collections.
  - AtomicInteger for counting processed articles and duplicates.

## Performance Analysis

Testing was conducted on a Linux environment to evaluate scalability and speedup.

- System Configuration:
  - OS: Linux Mint
  - CPU: AMD Ryzen 7 6800H (8 Cores / 16 Threads)
  - RAM: 32GB
  - Java Version: 25
  - Dataset: Test_5 Checker Suite

- Results:
  - Significant time reduction observed from 1 to 4 threads.
  - Performance stabilizes around 6 threads due to synchronization overhead and I/O bottlenecks.
  - Optimal thread count identified as 6 for this specific hardware configuration.
  - Speedup diminishes beyond 6 threads due to barrier waiting times and contention on shared structures.

## Build and Run

### Prerequisites

- Java Development Kit (JDK) 25 or compatible
- Make (for build automation)
- Maven (optional, if using pom.xml)

### Compilation

```bash
make build
```

### Execution 

```bash
make run ARGS="<num_threads> <articles_file> <inputs_file>"
```

### Input Format
- articles.txt: List of JSON files containing news articles
- inputs.txt: Paths to configuration files (languages.txt, categories.txt, english_linking_words.txt)

### JSON Articles Structure
```
uuid
title
author
url
text
published
language
categories
```
### Output Files
- all_articles.txt: Global list of unique articles sorted by publication date
- <category>.txt: UUIDs of articles per valid category
- <language>.txt: UUIDs of articles per valid language
- keywords_count.txt: Frequency of significant words in English articles
- reports.txt: Statistical summary (duplicates, best author, top language, etc.)



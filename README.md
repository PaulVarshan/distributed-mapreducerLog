# Apache Log MapReduce Analyzer

INTE 22253 Distributed Systems and Cloud Computing, Part C, Option 2 (Cloud Programming Model Implementation)

## Objective

This project analyses a real Apache error log (`data/Apache.log`) with the **MapReduce** programming model.
The input is split into chunks. Each chunk is mapped by a separate worker process.
The intermediate key-value pairs are then shuffled (grouped by key) and reduced (summed).

This is a **local simulation** of distributed workers: all workers are processes on one computer.

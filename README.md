# Apache Log MapReduce Analyzer

INTE 22253 Distributed Systems and Cloud Computing, Part C, Option 2 (Cloud Programming Model Implementation)

## Objective

This project analyses a real Apache error log (`data/Apache.log`) with the **MapReduce** programming model.
The input is split into chunks. Each chunk is mapped by a separate worker process.
The intermediate key-value pairs are then shuffled (grouped by key) and reduced (summed).

This is a **local simulation** of distributed workers: all workers are processes on one computer.

## Technologies

- Python 3 (standard library only, no `pip install` needed)
- `tkinter` provides the desktop UI
- `multiprocessing` runs one worker process per chunk
- `re` parses log lines
- `collections` (`defaultdict`) groups pairs in the shuffle step
- `pathlib`, `datetime`, `threading`, `queue`, `unittest` are used for file paths, dates, keeping the UI responsive, and tests

## Project structure

```text
distributed-mapreducerLog/
├── data/
│   └── Apache.log            real input dataset (56,482 lines)
├── src/
│   ├── log_analysis.py       MapReduce engine (no UI code)
│   ├── ui.py                 Tkinter user interface
│   └── test_log_analysis.py  unit tests
├── README.md
└── requirements.txt
```

## How to run

```bash
python src/ui.py                              # the desktop UI
python src/log_analysis.py                    # command line: runs 1, 2 and 4 workers and compares
python -m unittest discover -s src -v         # the tests
```

In the UI:
- **Browse** picks a log file. It defaults to `data/Apache.log`.
- **Number of workers** can be 1 to 4.
- **Run Analysis** runs the job once.
- **Run Experiment (1, 2, 4 workers)** runs the job three times, fills the run-history table, and checks that all three runs produced identical counts.
- **Clear** resets the screen.

## How it works

```text
                     Apache.log
                         |
                    Input Reader              read_records()
                         |
                       Split                  split_records()
          +--------------+--------------+
          v              v              v
      Worker 1       Worker 2       Worker 3    worker()  <- separate processes
         Map            Map            Map      map_logs()
          +--------------+--------------+
                         |
                  Shuffle / Group             shuffle()
          +--------------+--------------+
          v              v              v
      Log Level      Error Type      Client IP
          +--------------+--------------+
                         |
                       Reduce                 reduce_counts()
                         |
                   Final Results              run_mapreduce() returns a dict
                         |
                     Tkinter UI               ui.py
```

1. **Split.** The records are divided into N chunks of almost equal size. For example, 56,482 lines with 4 workers gives chunks of 14,121 / 14,121 / 14,120 / 14,120 lines.
2. **Map.** Each worker process parses its lines. For every line it emits `(key, 1)` pairs, for example:
   ```text
   (("LEVEL", "error"), 1)
   (("ERROR_TYPE", "Directory index forbidden by rule"), 1)
   (("CLIENT_IP", "61.155.76.2"), 1)
   (("ERROR_DATE", "2005-11-27"), 1)
   ```
   The worker sends its pairs back to the main process through a `multiprocessing.Queue`.
3. **Shuffle.** The main process joins all workers' pairs and groups them by key, for example `("LEVEL","error") -> [1, 1, 1, ...]`.
4. **Reduce.** The main process sums each group, for example `error -> 38081`.

### Error-type normalisation

Error messages contain changing values such as paths and numbers. `classify_error()` checks the message against a short list of known phrases (`KNOWN_ERROR_TYPES`), and the first phrase found becomes the error type. So `File does not exist: /var/www/html/blog` becomes `File does not exist`, and `mod_jk child workerEnv in error state 5` becomes `mod_jk child workerEnv in error state`. A message that matches no phrase keeps the text before its first `:`, with digits replaced by `N`.

### Malformed lines

The dataset contains 4,478 lines that are just `script not found or unable to stat`, with no timestamp or level. They are fragments of the previous log entry. They are not dropped: Map emits `("RECORD", "malformed")` for them, so **parsed + malformed = total lines**.

## Analyses and results (measured from `data/Apache.log`)

| Item | Value |
|---|---|
| Total lines | 56,482 |
| Parsed records | 52,004 |
| Malformed lines | 4,478 |

**Log levels:** error 38,081 · notice 13,755 · warn 168

**Top error types:**

| Error type | Count |
|---|---|
| File does not exist | 20,861 |
| Directory index forbidden by rule | 6,745 |
| mod_jk child workerEnv in error state | 4,349 |
| script not found or unable to stat | 3,301 |
| mod_jk child init | 1,259 |
| Can't find child | 971 |
| Factory error creating | 180 |
| Can't create | 180 |
| attempt to invoke directory as script | 92 |
| request failed: URI too long | 39 |

**Top client IPs (error records):** 218.144.240.75 (1,002), 210.245.233.251 (624), 211.99.203.228 (440), 80.55.121.106 (322), 61.152.90.96 (315) …
There are 4,695 distinct IPs in total, across 31,115 error records that include a client IP.

**Errors by date:** 230 distinct dates, from 2005-06-09 to 2006-02-28.

These figures were checked against `grep` counts on the raw file:
- The error types sum to 38,081, the number of error records.
- The per-IP counts sum to 31,115, the number of error lines that contain `[client ...]`.

## Performance experiment

Here are example timings from one machine (Windows 11, Python 3.13). The times include reading the file, starting the processes, map, shuffle and reduce:

| Workers | Execution time (s) |
|---|---|
| 1 | ~1.29 to 1.48 |
| 2 | ~0.88 to 1.13 |
| 4 | ~0.92 to 1.16 |

Going from 1 to 2 workers is faster. Going from 2 to 4 workers is **not** faster.

The dataset is small (about 5 MB), so the fixed costs start to dominate. These costs are starting extra processes (Windows uses *spawn*), copying the chunks to them, and sending about 216,000 intermediate pairs back to be shuffled in a single process. This is the **parallelism vs overhead** trade-off.

The final counts are identical for 1, 2 and 4 workers. The **Run Experiment** button checks this automatically.

Your timings will be different. Use the numbers the program shows on your own machine.

## Limitations (it is not a real cluster)

```text
This project:                         Real distributed MapReduce:
  One computer                          Coordinator
   ├── Process 1                          ├── Machine 1 (workers)
   ├── Process 2                          ├── Machine 2 (workers)
   └── Process N                          └── Machine 3 (workers)
```

This implementation does **not** provide:
- network communication between machines
- distributed storage (such as HDFS)
- cluster scheduling
- automatic recovery when a worker fails. If a worker process crashes, the job reports an error instead of re-running the chunk.
- a distributed shuffle. Here, one process shuffles and reduces.

It demonstrates the programming model: split, parallel map, shuffle, reduce. To scale out, the chunks would be stored in distributed storage and sent to workers on different machines by a coordinator. The coordinator would re-run a chunk if a worker failed, and the shuffle and reduce would be partitioned by key across several reducers.

## Viva notes

- **What is MapReduce?** Input is divided among workers. Map turns each record into key-value pairs. Shuffle groups the pairs by key. Reduce aggregates each group.
- **Map** is `map_logs()`: one log line gives pairs such as `("LEVEL","error") -> 1`.
- **Reduce** is `reduce_counts()`: `error -> [1,1,1,1]` becomes `error -> 4`.
- **Why multiprocessing?** Separate processes run truly in parallel. Python threads cannot run CPU-heavy Python code in parallel because of the GIL. Processes also behave like independent workers with their own memory.
- **Is it truly distributed?** No. It is a local simulation using processes on one machine.

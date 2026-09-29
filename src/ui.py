"""
Tkinter UI for the Apache Log MapReduce Analyzer.

The UI only displays things. All processing is done by log_analysis.py.
The job runs in a background thread so the window does not freeze; the
engine reports progress through a queue that the UI reads every 50 ms.

Run:  python src/ui.py
"""

import queue
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

import log_analysis as la

TOP_N = 10
PIPELINE_STAGES = ["INPUT", "SPLIT", "MAP", "SHUFFLE", "REDUCE", "RESULT"]
COLOR_PENDING = "#e0e0e0"
COLOR_ACTIVE = "#ffd54f"
COLOR_DONE = "#81c784"


class AnalyzerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Apache Log MapReduce Analyzer")
        self.root.geometry("1200x850")
        try:
            self.root.state("zoomed")    # start maximised on Windows
        except tk.TclError:
            pass

        self.events = queue.Queue()      # messages from the background job
        self.running = False
        self.worker_rows = {}            # worker_id -> Treeview row id
        self.comparison = {}             # workers -> results (for 1/2/4 comparison)
        self.pipeline_states = {}        # stage name -> "active" / "done"
        self.worker_states = {}          # worker id  -> "running" / "done"
        self.pipeline_workers = 3

        self.file_var = tk.StringVar(value=str(la.DEFAULT_INPUT_FILE))
        self.workers_var = tk.StringVar(value="3")
        self.status_var = tk.StringVar(value="Status: Ready")
        self.records_var = tk.StringVar(value="Records processed: -")
        self.time_var = tk.StringVar(value="Execution time: -")

        self.build_ui()
        self.draw_pipeline(num_workers=int(self.workers_var.get()))
        self.root.after(50, self.poll_events)

    # ------------------------------------------------------------------ layout
    def build_ui(self):
        pad = {"padx": 8, "pady": 4}

        header = ttk.Frame(self.root)
        header.pack(fill="x", **pad)
        ttk.Label(header, text="Apache Log MapReduce Analyzer",
                  font=("Segoe UI", 16, "bold")).pack()
        ttk.Label(header, text="INTE 22253 — Distributed Systems & Cloud Computing  "
                  "(local simulation: worker processes on one machine)").pack()

        # Input file + controls
        controls = ttk.Frame(self.root)
        controls.pack(fill="x", **pad)
        ttk.Label(controls, text="Input file:").grid(row=0, column=0, sticky="w")
        ttk.Entry(controls, textvariable=self.file_var, width=90).grid(row=0, column=1, columnspan=4, sticky="we")
        ttk.Button(controls, text="Browse", command=self.browse).grid(row=0, column=5, padx=4)

        ttk.Label(controls, text="Number of workers:").grid(row=1, column=0, sticky="w", pady=4)
        workers_box = ttk.Combobox(controls, textvariable=self.workers_var, values=["1", "2", "3", "4"],
                                   width=5, state="readonly")
        workers_box.grid(row=1, column=1, sticky="w")
        workers_box.bind("<<ComboboxSelected>>",
                         lambda e: self.draw_pipeline(int(self.workers_var.get())))
        self.run_button = ttk.Button(controls, text="Run Analysis", command=self.run_analysis)
        self.run_button.grid(row=1, column=2, padx=4)
        self.compare_button = ttk.Button(controls, text="Run Experiment (1, 2, 4 workers)",
                                         command=self.run_comparison)
        self.compare_button.grid(row=1, column=3, padx=4)
        ttk.Button(controls, text="Clear", command=self.clear).grid(row=1, column=4, padx=4, sticky="w")

        # Pipeline diagram
        pipe_frame = ttk.LabelFrame(self.root, text="MapReduce Pipeline")
        pipe_frame.pack(fill="x", **pad)
        self.canvas = tk.Canvas(pipe_frame, height=130, bg="white", highlightthickness=0)
        self.canvas.pack(fill="x", padx=4, pady=4)
        # Redraw the diagram whenever the window is resized.
        self.canvas.bind("<Configure>", lambda e: self.draw_pipeline(
            self.pipeline_workers, self.pipeline_states, self.worker_states))

        # Partitioning | Worker activity | Execution + experiment history
        middle = ttk.Frame(self.root)
        middle.pack(fill="x", **pad)
        for col in range(3):
            middle.columnconfigure(col, weight=1)

        part_frame = ttk.LabelFrame(middle, text="Data Partitioning (Split)")
        part_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 4))
        self.chunk_table = self.make_table(part_frame, ("Chunk", "Lines"), (70, 80), height=4)

        worker_frame = ttk.LabelFrame(middle, text="Worker Activity (Map)")
        worker_frame.grid(row=0, column=1, sticky="nsew", padx=4)
        self.worker_table = self.make_table(
            worker_frame, ("Worker", "PID", "Records", "Pairs emitted", "Map time (s)", "Status"),
            (55, 55, 65, 85, 80, 80), height=4)

        exec_frame = ttk.LabelFrame(middle, text="Execution")
        exec_frame.grid(row=0, column=2, sticky="nsew", padx=(4, 0))
        ttk.Label(exec_frame, textvariable=self.status_var).pack(anchor="w", padx=6)
        ttk.Label(exec_frame, textvariable=self.records_var).pack(anchor="w", padx=6)
        ttk.Label(exec_frame, textvariable=self.time_var, font=("Segoe UI", 10, "bold")).pack(anchor="w", padx=6)
        ttk.Label(exec_frame, text="Run history (Workers | Time):").pack(anchor="w", padx=6, pady=(4, 0))
        self.history_table = self.make_table(exec_frame, ("Workers", "Execution time (s)", "Records"),
                                             (60, 120, 80), height=3)

        # Results: 2 x 2 grid
        results = ttk.Frame(self.root)
        results.pack(fill="both", expand=True, **pad)
        results.columnconfigure(0, weight=1)
        results.columnconfigure(1, weight=1)
        results.rowconfigure(0, weight=1)
        results.rowconfigure(1, weight=1)

        f = ttk.LabelFrame(results, text="Log Level Counts")
        f.grid(row=0, column=0, sticky="nsew", padx=(0, 4), pady=(0, 4))
        self.level_table = self.make_table(f, ("Level", "Count"), (250, 120), height=4)

        f = ttk.LabelFrame(results, text=f"Top {TOP_N} Error Types")
        f.grid(row=0, column=1, sticky="nsew", padx=(4, 0), pady=(0, 4))
        self.error_table = self.make_table(f, ("#", "Error type", "Count"), (30, 330, 90), height=6)

        f = ttk.LabelFrame(results, text=f"Top {TOP_N} Client IPs (by error records)")
        f.grid(row=1, column=0, sticky="nsew", padx=(0, 4))
        self.ip_table = self.make_table(f, ("#", "Client IP", "Error records"), (30, 220, 120), height=6)

        f = ttk.LabelFrame(results, text="Errors by Date (all dates, scroll)")
        f.grid(row=1, column=1, sticky="nsew", padx=(4, 0))
        self.date_table = self.make_table(f, ("Date", "Error records"), (200, 120), height=6)

    def make_table(self, parent, columns, widths, height):
        frame = ttk.Frame(parent)
        frame.pack(fill="both", expand=True, padx=4, pady=4)
        table = ttk.Treeview(frame, columns=columns, show="headings", height=height)
        for col, width in zip(columns, widths):
            table.heading(col, text=col)
            table.column(col, width=width, anchor="w")
        scroll = ttk.Scrollbar(frame, orient="vertical", command=table.yview)
        table.configure(yscrollcommand=scroll.set)
        table.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")
        return table

    # ---------------------------------------------------------------- pipeline
    def draw_pipeline(self, num_workers, states=None, worker_states=None):
        """Draw INPUT -> SPLIT -> MAP (N workers) -> SHUFFLE -> REDUCE -> RESULT."""
        states = states or {}
        worker_states = worker_states or {}
        self.pipeline_states = states
        self.worker_states = worker_states
        self.pipeline_workers = num_workers

        c = self.canvas
        c.delete("all")
        width = c.winfo_width() if c.winfo_width() > 1 else 1000
        step = width / len(PIPELINE_STAGES)
        box_w, box_h, y = min(110, step - 30), 30, 12
        centers = []
        for i, stage in enumerate(PIPELINE_STAGES):
            x = step * i + step / 2
            centers.append(x)
            color = {"active": COLOR_ACTIVE, "done": COLOR_DONE}.get(states.get(stage), COLOR_PENDING)
            c.create_rectangle(x - box_w / 2, y, x + box_w / 2, y + box_h, fill=color, outline="#555")
            c.create_text(x, y + box_h / 2, text=stage, font=("Segoe UI", 10, "bold"))
            if i > 0:
                c.create_line(centers[i - 1] + box_w / 2, y + box_h / 2, x - box_w / 2, y + box_h / 2,
                              arrow="last", width=2)

        # Worker boxes under MAP
        map_x = centers[PIPELINE_STAGES.index("MAP")]
        wy = y + box_h + 25
        wbox = min(100, step * 2 / num_workers - 10)
        gap = 10
        total = num_workers * wbox + (num_workers - 1) * gap
        left = map_x - total / 2
        for w in range(1, num_workers + 1):
            x0 = left + (w - 1) * (wbox + gap)
            color = {"running": COLOR_ACTIVE, "done": COLOR_DONE}.get(worker_states.get(w), COLOR_PENDING)
            c.create_line(map_x, y + box_h, x0 + wbox / 2, wy, fill="#888")
            c.create_rectangle(x0, wy, x0 + wbox, wy + 36, fill=color, outline="#555")
            c.create_text(x0 + wbox / 2, wy + 18, text=f"Worker {w}\n(process)", justify="center",
                          font=("Segoe UI", 9))
        c.create_text(10, 120, anchor="w", fill="#555", font=("Segoe UI", 9),
                      text="grey = waiting     yellow = running     green = completed")

    def set_stage(self, stage, state):
        self.pipeline_states[stage] = state
        self.draw_pipeline(self.pipeline_workers, self.pipeline_states, self.worker_states)

    def set_worker(self, worker_id, state):
        self.worker_states[worker_id] = state
        self.draw_pipeline(self.pipeline_workers, self.pipeline_states, self.worker_states)

    # ----------------------------------------------------------------- actions
    def browse(self):
        path = filedialog.askopenfilename(
            title="Select Apache log file",
            initialdir=str(la.DEFAULT_INPUT_FILE.parent),
            filetypes=[("Log files", "*.log"), ("All files", "*.*")])
        if path:
            self.file_var.set(path)

    def clear(self):
        if self.running:
            return
        for table in (self.chunk_table, self.worker_table, self.level_table, self.error_table,
                      self.ip_table, self.date_table, self.history_table):
            table.delete(*table.get_children())
        self.worker_rows = {}
        self.status_var.set("Status: Ready")
        self.records_var.set("Records processed: -")
        self.time_var.set("Execution time: -")
        self.draw_pipeline(int(self.workers_var.get()))

    def check_input(self):
        if self.running:
            return None
        path = Path(self.file_var.get())
        if not path.is_file():
            messagebox.showerror("File not found", f"Cannot find:\n{path}")
            return None
        return path

    def run_analysis(self):
        path = self.check_input()
        if path:
            self.start_job([int(self.workers_var.get())], path)

    def run_comparison(self):
        path = self.check_input()
        if path:
            self.start_job([1, 2, 4], path)

    def start_job(self, worker_counts, path):
        """Run one or more MapReduce jobs in a background thread."""
        self.running = True
        self.run_button.state(["disabled"])
        self.compare_button.state(["disabled"])
        self.comparison = {}

        def job():
            try:
                for n in worker_counts:
                    self.events.put(("job_start", {"workers": n}))
                    results = la.run_mapreduce(
                        path, n, progress=lambda event, info: self.events.put((event, info)))
                    self.events.put(("job_done", {"results": results}))
                self.events.put(("all_done", {"worker_counts": worker_counts}))
            except Exception as exc:          # show the error in the UI instead of crashing
                self.events.put(("failed", {"error": str(exc)}))

        threading.Thread(target=job, daemon=True).start()

    # --------------------------------------------------- progress from the job
    def poll_events(self):
        try:
            while True:
                event, info = self.events.get_nowait()
                self.handle_event(event, info)
        except queue.Empty:
            pass
        self.root.after(50, self.poll_events)

    def handle_event(self, event, info):
        if event == "job_start":
            n = info["workers"]
            self.workers_var.set(str(n))
            for table in (self.chunk_table, self.worker_table):
                table.delete(*table.get_children())
            self.worker_rows = {}
            self.draw_pipeline(n, {"INPUT": "active"})
            self.status_var.set(f"Status: Reading input ({n} worker(s))...")

        elif event == "read":
            self.set_stage("INPUT", "done")
            self.set_stage("SPLIT", "active")
            self.records_var.set(f"Records processed: {info['total']:,}")

        elif event == "split":
            for i, size in enumerate(info["chunk_sizes"], start=1):
                self.chunk_table.insert("", "end", values=(f"Chunk {i}", f"{size:,}"))
            self.set_stage("SPLIT", "done")
            self.set_stage("MAP", "active")
            self.status_var.set("Status: Map running in worker processes...")

        elif event == "worker_started":
            w = info["worker_id"]
            self.worker_rows[w] = self.worker_table.insert(
                "", "end", values=(w, info["pid"], f"{info['records']:,}", "-", "-", "Running"))
            self.set_worker(w, "running")

        elif event == "worker_done":
            w = info["worker_id"]
            self.worker_table.item(self.worker_rows[w], values=(
                w, info["pid"], f"{info['records']:,}", f"{info['pairs']:,}",
                f"{info['map_time']:.3f}", "Completed"))
            self.set_worker(w, "done")

        elif event == "shuffle":
            self.set_stage("MAP", "done")
            self.set_stage("SHUFFLE", "done")
            self.set_stage("REDUCE", "active")
            self.status_var.set(f"Status: Shuffled {info['pairs']:,} pairs into {info['keys']:,} keys, reducing...")

        elif event == "reduce":
            self.set_stage("REDUCE", "done")

        elif event == "job_done":
            results = info["results"]
            self.comparison[results["num_workers"]] = results
            self.set_stage("RESULT", "done")
            self.show_results(results)

        elif event == "all_done":
            self.running = False
            self.run_button.state(["!disabled"])
            self.compare_button.state(["!disabled"])
            if len(info["worker_counts"]) > 1:
                self.show_comparison()

        elif event == "failed":
            self.running = False
            self.run_button.state(["!disabled"])
            self.compare_button.state(["!disabled"])
            self.status_var.set("Status: FAILED")
            messagebox.showerror("MapReduce failed", info["error"])

    def show_results(self, r):
        self.status_var.set(f"Status: Analysis completed ({r['num_workers']} worker(s))")
        self.records_var.set(f"Records processed: {r['total_records']:,}  "
                             f"(parsed {r['parsed_records']:,}, malformed {r['malformed_records']:,})")
        self.time_var.set(f"Execution time: {r['elapsed_time']:.4f} seconds")
        self.history_table.insert("", "end", values=(
            r["num_workers"], f"{r['elapsed_time']:.4f}", f"{r['total_records']:,}"))
        self.history_table.yview_moveto(1)

        for table in (self.level_table, self.error_table, self.ip_table, self.date_table):
            table.delete(*table.get_children())
        for level, count in la.sort_counts(r["level_counts"]):
            self.level_table.insert("", "end", values=(level, f"{count:,}"))
        if r["malformed_records"]:
            self.level_table.insert("", "end", values=("(malformed line, no level)",
                                                       f"{r['malformed_records']:,}"))
        for rank, (name, count) in enumerate(la.sort_counts(r["error_counts"])[:TOP_N], start=1):
            self.error_table.insert("", "end", values=(rank, name, f"{count:,}"))
        for rank, (ip, count) in enumerate(la.sort_counts(r["ip_counts"])[:TOP_N], start=1):
            self.ip_table.insert("", "end", values=(rank, ip, f"{count:,}"))
        for date, count in sorted(r["date_counts"].items()):
            self.date_table.insert("", "end", values=(date, f"{count:,}"))

    def show_comparison(self):
        keys = ("level_counts", "error_counts", "ip_counts", "date_counts",
                "parsed_records", "malformed_records")
        runs = self.comparison
        first = runs[min(runs)]
        same = all(run[k] == first[k] for run in runs.values() for k in keys)
        lines = [f"{n} worker(s): {run['elapsed_time']:.4f} s" for n, run in sorted(runs.items())]
        lines.append("")
        lines.append("Final counts identical for all worker counts: " + ("YES" if same else "NO"))
        self.status_var.set("Status: Experiment completed — results identical: " + ("YES" if same else "NO"))
        messagebox.showinfo("Experiment: workers vs execution time", "\n".join(lines))


def main():
    root = tk.Tk()
    AnalyzerApp(root)
    root.mainloop()


if __name__ == "__main__":
    # Required on Windows: worker processes re-import this file, and must not
    # open a second window.
    main()

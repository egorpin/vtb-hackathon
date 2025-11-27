import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import time
import sys
import os
import json 
from collections import deque
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from config import DB_CONFIG, ANALYSIS_INTERVAL
from metrics import MetricsCollector
from analyzer import ProfileAnalyzer
from db_loader import load_profiles_from_db
from benchmark_runner import BenchmarkRunner

COLOR_VTB_BLUE_DARK = "#0A2896"
COLOR_VTB_BLUE_LIGHT = "#3A83F1"
COLOR_BG_MAIN = "#F0F4F7"
COLOR_BG_SIDEBAR = "#001D6E"
COLOR_WHITE = "#FFFFFF"
COLOR_TEXT_PRIMARY = "#2B2D33"
COLOR_TEXT_SECONDARY = "#6C757D"
COLOR_SUCCESS = "#28A745"
COLOR_DANGER = "#DC3545"

class VTBProfilerGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("VTB Load Profiler | Enterprise Monitor")
        self.root.geometry("1440x950")
        self.root.configure(bg=COLOR_BG_MAIN)

        self.setup_styles()

        self.profile_map = self._load_local_profiles()
        
        self.is_test_running = False

        try:
            self.collector = MetricsCollector(DB_CONFIG)
            self.analyzer = ProfileAnalyzer()
            self.benchmark_runner = BenchmarkRunner(DB_CONFIG)
            self.profiles_db = load_profiles_from_db()
            self.prev_snapshot = self.collector.get_snapshot()
            print("VTB System initialized successfully")
        except Exception as e:
            print(f"Warning: Database connection issue: {e}")

        self.history_tps = deque([0]*60, maxlen=60)
        self.history_lat = deque([0]*60, maxlen=60)
        self.history_ash = deque([0]*60, maxlen=60)
        self.history_rwr = deque([0]*60, maxlen=60)
        self.history_max_lat = deque([0]*60, maxlen=60)
        self.history_iwr = deque([0]*60, maxlen=60)

        self.running = True
        self.setup_ui()
        self.start_updates()

    def _load_local_profiles(self):
        return {
            'IDLE': {},
            'Classic OLTP': {
                "shared_buffers": "25% RAM",
                "random_page_cost": "1.1",
                "effective_io_concurrency": "200",
                "wal_buffers": "16MB",
                "checkpoint_completion_target": "0.9",
                "synchronous_commit": "on"
            },
            'Heavy OLAP': {
                "work_mem": "64MB",
                "maintenance_work_mem": "512MB",
                "max_parallel_workers_per_gather": "4",
                "effective_cache_size": "75% RAM",
                "jit": "on",
                "random_page_cost": "1.1"
            },
            'Disk-Bound OLAP': {
                "work_mem": "128MB",
                "effective_io_concurrency": "300",
                "max_worker_processes": "8",
                "max_parallel_workers": "8",
                "random_page_cost": "1.5",
                "seq_page_cost": "1.0"
            },
            'IoT / Ingestion': {
                "synchronous_commit": "off",
                "commit_delay": "1000",
                "max_wal_size": "10GB",
                "checkpoint_timeout": "30min",
                "wal_writer_delay": "200ms",
                "autovacuum_analyze_scale_factor": "0.05"
            },
            'Mixed / HTAP': {
                "shared_buffers": "40% RAM",
                "work_mem": "32MB",
                "min_wal_size": "2GB",
                "max_wal_size": "8GB",
                "random_page_cost": "1.25",
                "effective_cache_size": "60% RAM"
            },
            'Web / Read-Only': {
                "autovacuum_naptime": "5min",
                "wal_level": "minimal",
                "synchronous_commit": "off",
                "default_transaction_isolation": "read committed",
                "shared_buffers": "30% RAM"
            },
            'End of day Batch': {
                "max_wal_size": "40GB",
                "checkpoint_timeout": "60min",
                "autovacuum": "off",
                "full_page_writes": "off",
                "synchronous_commit": "off",
                "wal_buffers": "64MB"
            },
            'Data Maintenance': {
                "maintenance_work_mem": "2GB",
                "autovacuum_vacuum_cost_limit": "2000",
                "vacuum_cost_delay": "0",
                "max_parallel_maintenance_workers": "4",
                "wal_level": "minimal"
            }
        }

    def setup_styles(self):
        style = ttk.Style()
        style.theme_use('clam')
        style.configure(".", background=COLOR_BG_MAIN, font=("Segoe UI", 10))
        style.configure("Sidebar.TFrame", background=COLOR_BG_SIDEBAR)
        style.configure("Sidebar.TButton", font=("Segoe UI", 10, "bold"), background=COLOR_VTB_BLUE_LIGHT, foreground=COLOR_WHITE, borderwidth=0, anchor="w", padding=10)
        style.map("Sidebar.TButton", background=[('active', '#5DA3FF'), ('pressed', '#0056B3')])
        style.configure("Main.TFrame", background=COLOR_BG_MAIN)
        style.configure("Card.TFrame", background=COLOR_WHITE, relief="flat")

    def setup_ui(self):
        sidebar = ttk.Frame(self.root, style="Sidebar.TFrame", width=260)
        sidebar.pack(side=tk.LEFT, fill=tk.Y)
        sidebar.pack_propagate(False)

        logo_frame = tk.Frame(sidebar, bg=COLOR_BG_SIDEBAR, height=80)
        logo_frame.pack(fill=tk.X, pady=(20, 10), padx=20)
        tk.Label(logo_frame, text="VTB", font=("Arial", 28, "bold"), bg=COLOR_BG_SIDEBAR, fg=COLOR_WHITE).pack(anchor="w")
        tk.Label(logo_frame, text="Load Profiler System", font=("Segoe UI", 10), bg=COLOR_BG_SIDEBAR, fg="#AAB7D5").pack(anchor="w")

        ttk.Separator(sidebar).pack(fill=tk.X, padx=20, pady=20)

        self._create_sidebar_label(sidebar, "BENCHMARK SUITE")
        self._create_sidebar_btn(sidebar, "  Classic OLTP", lambda: self.run_benchmark("Classic OLTP", "OLTP"))
        self._create_sidebar_btn(sidebar, "  Heavy OLAP", lambda: self.run_benchmark("Heavy OLAP", "OLAP"))
        self._create_sidebar_btn(sidebar, "  Disk-Bound OLAP", lambda: self.run_benchmark("Disk-Bound OLAP", "DISK_OLAP"))
        self._create_sidebar_btn(sidebar, "  Web / Read-Only", lambda: self.run_benchmark("Web / Read-Only", "READ_ONLY"))
        self._create_sidebar_btn(sidebar, "  IoT Stream", lambda: self.run_benchmark("IoT / Ingestion", "IoT"))
        self._create_sidebar_btn(sidebar, "  Mixed / HTAP", lambda: self.run_benchmark("Mixed / HTAP", "Mixed"))
        self._create_sidebar_btn(sidebar, "  End of day Batch", lambda: self.run_benchmark("End of day Batch", "BATCH_JOB"))
        self._create_sidebar_btn(sidebar, "  Data Maintenance", lambda: self.run_benchmark("Data Maintenance", "MAINTENANCE"))

        ttk.Separator(sidebar).pack(fill=tk.X, padx=20, pady=20)

        status_frame = tk.Frame(sidebar, bg=COLOR_BG_SIDEBAR)
        status_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=20, pady=20)
        self.progress_var = tk.StringVar(value="System Ready")
        tk.Label(status_frame, textvariable=self.progress_var, bg=COLOR_BG_SIDEBAR, fg="#AAB7D5", font=("Segoe UI", 9), wraplength=220, justify="left").pack(anchor="w")

        main_content = ttk.Frame(self.root, style="Main.TFrame", padding=20)
        main_content.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        top_bar = tk.Frame(main_content, bg=COLOR_WHITE, height=80)
        top_bar.pack(fill=tk.X, pady=(0, 20))
        tk.Frame(main_content, bg="#DDE2E8", height=2).place(x=0, y=100, relwidth=1)

        self.profile_var = tk.StringVar(value="IDLE")
        self.confidence_var = tk.StringVar(value="Waiting for data...")

        info_frame = tk.Frame(top_bar, bg=COLOR_WHITE)
        info_frame.pack(side=tk.LEFT, padx=20, pady=10)
        tk.Label(info_frame, text="DETECTED WORKLOAD (REAL-TIME)", font=("Segoe UI", 8, "bold"), fg=COLOR_TEXT_SECONDARY, bg=COLOR_WHITE).pack(anchor="w")
        self.lbl_profile = tk.Label(info_frame, textvariable=self.profile_var, font=("Segoe UI", 20, "bold"), fg=COLOR_TEXT_PRIMARY, bg=COLOR_WHITE)
        self.lbl_profile.pack(anchor="w")

        conf_frame = tk.Frame(top_bar, bg=COLOR_WHITE)
        conf_frame.pack(side=tk.RIGHT, padx=20)
        tk.Label(conf_frame, textvariable=self.confidence_var, font=("Segoe UI", 10), fg=COLOR_VTB_BLUE_LIGHT, bg=COLOR_WHITE).pack()

        metrics_frame = ttk.Frame(main_content, style="Main.TFrame")
        metrics_frame.pack(fill=tk.X, pady=(0, 20))

        self.tps_var = tk.StringVar(value="0")
        self.latency_var = tk.StringVar(value="0.000s")
        self.ash_var = tk.StringVar(value="0")
        self.io_var = tk.StringVar(value="0")

        grid_frame = tk.Frame(metrics_frame, bg=COLOR_BG_MAIN)
        grid_frame.pack(fill=tk.X)
        self._create_metric_card(grid_frame, "TPS (Trans/Sec)", self.tps_var, 0)
        self._create_metric_card(grid_frame, "TX COST (Latency)", self.latency_var, 1)
        self._create_metric_card(grid_frame, "ACTIVE SESSIONS", self.ash_var, 2)
        self._create_metric_card(grid_frame, "IO WAIT EVENTS", self.io_var, 3)

        rec_container = tk.Frame(main_content, bg=COLOR_WHITE, height=130)
        rec_container.pack(side=tk.BOTTOM, fill=tk.X, pady=(10, 0))

        rec_header = tk.Frame(rec_container, bg=COLOR_VTB_BLUE_DARK, height=25)
        rec_header.pack(fill=tk.X)
        tk.Label(rec_header, text="CONFIGURATION RECOMMENDATIONS (Based on Last Completed Benchmark)",
                 bg=COLOR_VTB_BLUE_DARK, fg=COLOR_WHITE, font=("Segoe UI", 9, "bold"), anchor="w").pack(fill=tk.X, pady=2)

        self.rec_text = scrolledtext.ScrolledText(rec_container, height=5, font=("Consolas", 10),
                                                bg="#FAFAFA", fg="#333333", relief="flat", padx=10, pady=5)
        self.rec_text.pack(fill=tk.BOTH, expand=True)
        self.rec_text.insert(tk.END, "No benchmark completed yet. Run a test to see tuning recommendations.")
        self.rec_text.config(state=tk.DISABLED)

        log_frame = tk.Frame(main_content, bg=COLOR_WHITE, height=150)
        log_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=(0, 10))
        tk.Label(log_frame, text="  BENCHMARK REPORTS & SYSTEM LOGS", bg="#E9ECEF", fg=COLOR_TEXT_PRIMARY, font=("Segoe UI", 9, "bold"), anchor="w").pack(fill=tk.X)
        self.results_text = scrolledtext.ScrolledText(log_frame, height=8, font=("Consolas", 9), bg=COLOR_WHITE, fg=COLOR_TEXT_PRIMARY, relief="flat")
        self.results_text.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        self.results_text.insert(tk.END, "VTB Profiler System initialized. Ready to execute benchmarks.\n")
        self.results_text.config(state=tk.DISABLED)

        chart_container = tk.Frame(main_content, bg=COLOR_WHITE, padx=5, pady=5)
        chart_container.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        self.fig, ((self.ax1, self.ax2, self.ax3), (self.ax4, self.ax5, self.ax6)) = plt.subplots(2, 3, figsize=(10, 6))
        self.fig.patch.set_facecolor(COLOR_WHITE)
        plt.subplots_adjust(left=0.05, bottom=0.1, right=0.95, top=0.9, wspace=0.2, hspace=0.4)
        self.canvas = FigureCanvasTkAgg(self.fig, master=chart_container)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

    def _create_sidebar_label(self, parent, text):
        tk.Label(parent, text=text, font=("Segoe UI", 8, "bold"), bg=COLOR_BG_SIDEBAR, fg="#7F94C4").pack(anchor="w", padx=20, pady=(10, 5))

    def _create_sidebar_btn(self, parent, text, command):
        btn = ttk.Button(parent, text=text, style="Sidebar.TButton", command=command, cursor="hand2")
        btn.pack(fill=tk.X, padx=10, pady=2)

    def _create_metric_card(self, parent, title, variable, col_index):
        card = tk.Frame(parent, bg=COLOR_WHITE, padx=20, pady=15)
        card.grid(row=0, column=col_index, sticky="nsew", padx=5)
        parent.grid_columnconfigure(col_index, weight=1)
        tk.Label(card, text=title, font=("Segoe UI", 9, "bold"), bg=COLOR_WHITE, fg="#8898AA").pack(anchor="w")
        tk.Label(card, textvariable=variable, font=("Segoe UI", 22, "bold"), bg=COLOR_WHITE, fg=COLOR_VTB_BLUE_DARK).pack(anchor="w", pady=(5, 0))

    def start_updates(self):
        def update():
            if self.running:
                self.update_stats()
                self.root.after(int(ANALYSIS_INTERVAL * 1000), update)
        self.root.after(1000, update)

    def update_stats(self):
        try:
            curr_snapshot = self.collector.get_snapshot()
            profile, conf, metrics = self.analyzer.analyze(self.prev_snapshot, curr_snapshot, ANALYSIS_INTERVAL)
            self.prev_snapshot = curr_snapshot

            self.tps_var.set(f"{int(metrics['TPS'])}")
            self.latency_var.set(f"{metrics['Tx Cost (s)']:.4f}s")
            self.ash_var.set(f"{metrics['Active Sessions (ASH)']}")
            self.io_var.set(f"{metrics['IO Waits']}")

            self.profile_var.set(profile)
            self.confidence_var.set(f"Accuracy: {conf}")

            if "IDLE" in profile: self.lbl_profile.config(fg="#999999")
            elif "OLTP" in profile: self.lbl_profile.config(fg=COLOR_SUCCESS)
            elif "OLAP" in profile: self.lbl_profile.config(fg=COLOR_DANGER)
            else: self.lbl_profile.config(fg=COLOR_VTB_BLUE_DARK)

            self.history_tps.append(metrics["TPS"])
            self.history_lat.append(metrics["Tx Cost (s)"])
            self.history_ash.append(metrics["Active Sessions (ASH)"])
            self.history_rwr.append(min(metrics["Read/Write Ratio"], 100.0))
            self.history_max_lat.append(metrics["Max Latency (s)"])
            self.history_iwr.append(min(metrics["Insert/Write Ratio"], 100.0))

            self._draw_chart(self.ax1, self.history_tps, "TPS Trend", COLOR_SUCCESS)
            self._draw_chart(self.ax2, self.history_lat, "Avg Tx Latency (s)", COLOR_DANGER)
            self._draw_chart(self.ax3, self.history_ash, "DB Load (ASH)", "#6F42C1")
            self._draw_chart(self.ax4, self.history_rwr, "Read/Write Ratio", COLOR_VTB_BLUE_LIGHT)
            self._draw_chart(self.ax5, self.history_max_lat, "Max Tx Latency (s)", "#FFC107")
            self._draw_chart(self.ax6, self.history_iwr, "Insert/Write Ratio", "#20C997")
            self.canvas.draw()

        except Exception as e:
            print(f"Update error: {e}")

    def _update_recommendations(self, profile_name):
        recs = self.profile_map.get(profile_name, {})
        self.rec_text.config(state=tk.NORMAL)
        self.rec_text.delete(1.0, tk.END)

        if not recs:
            self.rec_text.insert(tk.END, f"# Benchmark Profile: {profile_name}\n# No specific tuning recommendations found for this profile.")
        else:
            self.rec_text.insert(tk.END, f"# Recommended Settings for Profile: {profile_name}\n\n")
            for key, value in recs.items():
                line = f"{key} = '{value}'\n"
                self.rec_text.insert(tk.END, line)
        self.rec_text.config(state=tk.DISABLED)

    def _draw_chart(self, ax, data, title, color):
        ax.clear()
        ax.plot(data, color=color, linewidth=2)
        ax.fill_between(range(len(data)), data, color=color, alpha=0.1)
        ax.set_title(title, fontsize=9, color="#666666", loc='left', pad=10)
        ax.grid(True, linestyle='--', alpha=0.3)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color('#DDDDDD')
        ax.spines['bottom'].set_color('#DDDDDD')
        ax.tick_params(axis='both', colors='#888888', labelsize=8)

    def _log(self, text):
        self.results_text.config(state=tk.NORMAL)
        if text.startswith("==="):
             self.results_text.delete(1.0, tk.END)
        self.results_text.insert(1.0, text + "\n")
        self.results_text.config(state=tk.DISABLED)

    def run_benchmark(self, profile_name, test_type, duration=20):
        if self.is_test_running:
            self._log("Test already running. Please wait.")
            return

        def run_test():
            self.is_test_running = True
            self.progress_var.set(f"RUNNING: {test_type} ({profile_name})")
            
            test_methods = {
                "OLTP": self.benchmark_runner.run_oltp_test,
                "OLAP": self.benchmark_runner.run_olap_test,
                "IoT": self.benchmark_runner.run_iot_test,
                "Mixed": self.benchmark_runner.run_mixed_test,
                "READ_ONLY": self.benchmark_runner.run_read_only_test,
                "DISK_OLAP": self.benchmark_runner.run_disk_bound_olap_test,
                "BATCH_JOB": self.benchmark_runner.run_batch_test,
                "MAINTENANCE": self.benchmark_runner.run_maintenance_test
            }

            method = test_methods.get(test_type)

            if method:
                try:
                    results = method(profile_name, duration=duration)
                    if 'error' in results:
                        self._log(f"Error: {results['error']}")
                        self.progress_var.set("Error detected")
                    else:
                        self.progress_var.set("Test Completed Successfully")
                        report = f"DONE: {test_type} | TPS: {results['tps']:.1f} | Lat: {results['avg_latency']:.2f}ms"
                        self._log(report)
                        self.root.after(0, lambda: self._update_recommendations(profile_name))
                except Exception as e:
                    self._log(f"Critical Error: {e}")
            else:
                 self._log(f"Error: Method for {test_type} not implemented.")
            
            self.is_test_running = False

        threading.Thread(target=run_test, daemon=True).start()

    def on_closing(self):
        self.running = False
        self.root.destroy()

if __name__ == "__main__":
    root = tk.Tk()
    app = VTBProfilerGUI(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()
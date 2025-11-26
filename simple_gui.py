import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import time
import subprocess
import psycopg2
from collections import deque
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import sys
import os
import re

# Добавляем текущую директорию в путь
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from config import DB_CONFIG, ANALYSIS_INTERVAL
from metrics import MetricsCollector
from analyzer import ProfileAnalyzer
from db_loader import load_profiles_from_db
from benchmark_runner import BenchmarkRunner

class SimpleVTBProfiler:
    def __init__(self, root):
        self.root = root
        self.root.title("VTB Load Profiler - Complete Testing Suite")
        self.root.geometry("1400x900")

        # Initialize components
        try:
            self.collector = MetricsCollector(DB_CONFIG)
            self.analyzer = ProfileAnalyzer()
            self.benchmark_runner = BenchmarkRunner(DB_CONFIG)
            self.profiles_db = load_profiles_from_db()
            self.prev_snapshot = self.collector.get_snapshot()
            print("✅ All components initialized successfully")
        except Exception as e:
            print(f"❌ Connection Error: {e}")
            messagebox.showerror("Error", f"Database connection failed: {e}")
            sys.exit(1)

        self.history_tps = deque([0]*60, maxlen=60)
        self.history_lat = deque([0]*60, maxlen=60)
        self.load_thread = None
        self.running = True

        self.setup_ui()
        self.start_updates()

    def setup_ui(self):
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Left panel - Controls
        left_frame = ttk.LabelFrame(main_frame, text="Load Controller", padding="15", width=300)
        left_frame.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 10))
        left_frame.pack_propagate(False)

        # Title
        ttk.Label(left_frame, text="⚡ VTB Profiler", font=('Arial', 14, 'bold')).pack(anchor=tk.W, pady=(0, 10))
        ttk.Label(left_frame, text="LOAD CONTROLLER").pack(anchor=tk.W, pady=(0, 20))

        # Load buttons
        ttk.Button(left_frame, text="🏦 Start OLTP Load",
                  command=lambda: self.start_load("OLTP"),
                  width=20).pack(fill=tk.X, pady=5)
        ttk.Button(left_frame, text="📊 Start OLAP Load",
                  command=lambda: self.start_load("OLAP"),
                  width=20).pack(fill=tk.X, pady=5)
        ttk.Button(left_frame, text="📡 Start IoT Load",
                  command=lambda: self.start_load("IoT"),
                  width=20).pack(fill=tk.X, pady=5)
        ttk.Button(left_frame, text="🔄 Start Mixed Load",
                  command=lambda: self.start_load("Mixed"),
                  width=20).pack(fill=tk.X, pady=5)
        ttk.Button(left_frame, text="⛔ STOP LOAD",
                  command=lambda: self.start_load("STOP"),
                  width=20).pack(fill=tk.X, pady=5)

        # Status
        ttk.Separator(left_frame, orient='horizontal').pack(fill=tk.X, pady=15)
        ttk.Label(left_frame, text="Status:").pack(anchor=tk.W)
        self.status_var = tk.StringVar(value="Ready")
        status_label = ttk.Label(left_frame, textvariable=self.status_var, foreground="darkgreen")
        status_label.pack(anchor=tk.W, pady=(0, 10))

        # Benchmark section
        ttk.Label(left_frame, text="SPECIALIZED TESTERS", font=('Arial', 12, 'bold')).pack(anchor=tk.W, pady=(10, 5))

        # Отдельные тестировщики для каждого типа нагрузки
        ttk.Button(left_frame, text="🧪 OLTP Tester",
                  command=lambda: self.run_specialized_test("OLTP"),
                  width=20).pack(fill=tk.X, pady=2)
        ttk.Button(left_frame, text="🧪 OLAP Tester",
                  command=lambda: self.run_specialized_test("OLAP"),
                  width=20).pack(fill=tk.X, pady=2)
        ttk.Button(left_frame, text="🧪 IoT Tester",
                  command=lambda: self.run_specialized_test("IoT"),
                  width=20).pack(fill=tk.X, pady=2)
        ttk.Button(left_frame, text="🧪 Mixed Tester",
                  command=lambda: self.run_specialized_test("Mixed"),
                  width=20).pack(fill=tk.X, pady=2)

        ttk.Separator(left_frame, orient='horizontal').pack(fill=tk.X, pady=10)

        # Стандартные тесты профилей
        ttk.Button(left_frame, text="📊 Benchmark Report",
                  command=self.show_benchmark_report,
                  width=20).pack(fill=tk.X, pady=5)
        ttk.Button(left_frame, text="🚀 Auto Test All",
                  command=self.run_auto_benchmarks,
                  width=20).pack(fill=tk.X, pady=2)
        ttk.Button(left_frame, text="🔄 Clean Failed Tests",
                  command=self.cleanup_failed_tests,
                  width=20).pack(fill=tk.X, pady=2)

        # Benchmark status
        ttk.Separator(left_frame, orient='horizontal').pack(fill=tk.X, pady=15)
        ttk.Label(left_frame, text="Benchmark Status:").pack(anchor=tk.W)
        self.benchmark_status_var = tk.StringVar(value="Ready for testing")
        benchmark_status_label = ttk.Label(left_frame, textvariable=self.benchmark_status_var, foreground="darkgreen")
        benchmark_status_label.pack(anchor=tk.W)

        # Right panel - Dashboard
        right_frame = ttk.Frame(main_frame)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        # Profile detection
        profile_frame = ttk.LabelFrame(right_frame, text="Profile Detection", padding="15")
        profile_frame.pack(fill=tk.X, pady=(0, 10))

        self.profile_var = tk.StringVar(value="DETECTING...")
        self.confidence_var = tk.StringVar(value="Confidence: --")

        self.profile_label = ttk.Label(profile_frame, textvariable=self.profile_var,
                                     font=("Arial", 20, "bold"), foreground="gray")
        self.profile_label.pack(side=tk.LEFT)

        ttk.Label(profile_frame, textvariable=self.confidence_var,
                font=("Arial", 12)).pack(side=tk.RIGHT)

        # Metrics cards
        metrics_frame = ttk.Frame(right_frame)
        metrics_frame.pack(fill=tk.X, pady=(0, 10))

        # Create metric cards
        self.tps_var = tk.StringVar(value="0")
        self.latency_var = tk.StringVar(value="0.000s")
        self.sessions_var = tk.StringVar(value="0")
        self.io_var = tk.StringVar(value="0")

        # TPS Card
        tps_card = ttk.LabelFrame(metrics_frame, text="TPS (Trans/sec)", padding="10")
        tps_card.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        ttk.Label(tps_card, textvariable=self.tps_var, font=("Arial", 18, "bold"),
                 foreground="green").pack()

        # Latency Card
        latency_card = ttk.LabelFrame(metrics_frame, text="Tx Cost (ASH)", padding="10")
        latency_card.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
        ttk.Label(latency_card, textvariable=self.latency_var, font=("Arial", 18, "bold"),
                 foreground="red").pack()

        # Sessions Card
        sessions_card = ttk.LabelFrame(metrics_frame, text="Active Sessions", padding="10")
        sessions_card.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
        ttk.Label(sessions_card, textvariable=self.sessions_var, font=("Arial", 18, "bold"),
                 foreground="purple").pack()

        # IO Waits Card
        io_card = ttk.LabelFrame(metrics_frame, text="IO Wait Events", padding="10")
        io_card.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(5, 0))
        ttk.Label(io_card, textvariable=self.io_var, font=("Arial", 18, "bold"),
                 foreground="orange").pack()

        # Charts
        chart_frame = ttk.LabelFrame(right_frame, text="Performance Charts", padding="10")
        chart_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        # Create matplotlib figure
        self.fig, (self.ax1, self.ax2) = plt.subplots(2, 1, figsize=(10, 6))
        self.canvas = FigureCanvasTkAgg(self.fig, master=chart_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        # Results text area
        results_frame = ttk.LabelFrame(right_frame, text="AI Recommendations & Benchmark Results", padding="10")
        results_frame.pack(fill=tk.BOTH, expand=False)

        self.results_text = scrolledtext.ScrolledText(results_frame, height=12, width=100)
        self.results_text.pack(fill=tk.BOTH, expand=True)
        self.results_text.insert(tk.END, "=== VTB Load Profiler ===\n\nSystem ready. Use specialized testers for each load type.")
        self.results_text.config(state=tk.DISABLED)

    def start_updates(self):
        def update():
            if self.running:
                self.update_stats()
                self.root.after(ANALYSIS_INTERVAL * 1000, update)

        self.root.after(1000, update)

    def update_stats(self):
        try:
            curr_snapshot = self.collector.get_snapshot()
            profile, conf, metrics = self.analyzer.analyze(self.prev_snapshot, curr_snapshot, ANALYSIS_INTERVAL)
            self.prev_snapshot = curr_snapshot

            # Update UI
            self.tps_var.set(f"{int(metrics['TPS'])}")
            self.latency_var.set(f"{metrics['Tx Cost (s)']:.4f}s")
            self.sessions_var.set(f"{metrics['Active Sessions (ASH)']}")
            self.io_var.set(f"{metrics['IO Waits']}")

            self.profile_var.set(f"PROFILE: {profile}")
            self.confidence_var.set(f"Confidence: {conf}")

            # Update color based on profile
            color = "gray"
            if "OLTP" in profile: color = "green"
            elif "OLAP" in profile: color = "red"
            elif "IoT" in profile: color = "purple"
            elif "Mixed" in profile: color = "orange"
            elif "Bulk" in profile: color = "brown"
            elif "Web" in profile: color = "darkblue"

            self.profile_label.configure(foreground=color)

            # Update Charts
            self.history_tps.append(metrics["TPS"])
            self.history_lat.append(metrics["Tx Cost (s)"])

            self.ax1.clear()
            self.ax1.plot(self.history_tps, color='green', linewidth=2, alpha=0.8)
            self.ax1.fill_between(range(len(self.history_tps)), self.history_tps, color='green', alpha=0.1)
            self.ax1.set_title("Transactions Per Second (TPS)", fontsize=12)
            self.ax1.grid(True, linestyle=':', alpha=0.3)
            self.ax1.set_facecolor('#f8f9fa')

            self.ax2.clear()
            self.ax2.plot(self.history_lat, color='red', linewidth=2, alpha=0.8)
            self.ax2.fill_between(range(len(self.history_lat)), self.history_lat, color='red', alpha=0.1)
            self.ax2.set_title("Latency Cost (ASH/Commit)", fontsize=12)
            self.ax2.grid(True, linestyle=':', alpha=0.3)
            self.ax2.set_facecolor('#f8f9fa')

            self.canvas.draw()

            # Update Recommendations
            base_name = profile.split(" (")[0]
            if base_name in self.profiles_db:
                data = self.profiles_db[base_name]
                text = f"🤖 AI CONFIG RECOMMENDATIONS for {base_name}:\n\n"
                for k, v in data.items():
                    text += f"• {k} = {v}\n"

                self._update_results_text(text)

        except Exception as e:
            print(f"❌ Error updating stats: {e}")

    def start_load(self, mode):
        def run_load():
            container = "vtb_postgres"
            try:
                if mode == "OLTP":
                    self.status_var.set("Generating OLTP load...")
                    cmd = ["docker", "exec", "-i", container, "pgbench", "-c", "6", "-j", "2", "-T", "30", "-U", "user", "mydb", "-r"]
                    result = subprocess.run(cmd, capture_output=True, text=True)
                    if result.returncode == 0:
                        self.status_var.set("OLTP Load Finished")
                        tps, latency = self._parse_pgbench_output(result.stdout)
                        result_text = f"📊 OLTP Load Results:\nTPS: {tps:.1f}, Latency: {latency:.2f}ms"
                        self._update_results_text(result_text)
                    else:
                        self.status_var.set("OLTP Load Failed")

                elif mode == "OLAP":
                    self.status_var.set("Generating OLAP load...")
                    # Тяжелые аналитические запросы
                    sql = """
                    SELECT count(*), avg(a.aid), sum(a.abalance)
                    FROM pgbench_accounts a
                    JOIN pgbench_branches b ON a.bid = b.bid
                    JOIN pgbench_tellers t ON a.bid = t.bid
                    WHERE a.abalance > 0
                    GROUP BY b.bid, t.tid;
                    """
                    cmd = ["docker", "exec", "-i", container, "psql", "-U", "user", "-d", "mydb", "-c", sql]

                    completed = 0
                    for i in range(10):
                        if not self.running: break
                        result = subprocess.run(cmd, capture_output=True, text=True)
                        if result.returncode == 0:
                            completed += 1
                        self.status_var.set(f"OLAP Load... {i+1}/10")
                        time.sleep(2)

                    self.status_var.set(f"OLAP Load Finished ({completed}/10 queries)")

                elif mode == "IoT":
                    self.status_var.set("Generating IoT load...")
                    # Интенсивные вставки
                    sql = "INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (1, 1, 1, 0, CURRENT_TIMESTAMP);"
                    cmd = ["docker", "exec", "-i", container, "psql", "-U", "user", "-d", "mydb", "-c", sql]

                    completed = 0
                    for i in range(100):
                        if not self.running: break
                        result = subprocess.run(cmd, capture_output=True, text=True)
                        if result.returncode == 0:
                            completed += 1
                        if i % 20 == 0:
                            self.status_var.set(f"IoT Load... {i}/100")

                    self.status_var.set(f"IoT Load Finished ({completed} inserts)")

                elif mode == "Mixed":
                    self.status_var.set("Generating Mixed load...")
                    # Смешанная нагрузка
                    queries = [
                        "SELECT count(*) FROM pgbench_accounts;",
                        "INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (1, 1, 1, 0, CURRENT_TIMESTAMP);",
                        "SELECT avg(abalance) FROM pgbench_accounts WHERE bid = 1;"
                    ]

                    for i in range(20):
                        if not self.running: break
                        query = queries[i % len(queries)]
                        cmd = ["docker", "exec", "-i", container, "psql", "-U", "user", "-d", "mydb", "-c", query]
                        subprocess.run(cmd, capture_output=True, text=True)
                        self.status_var.set(f"Mixed Load... {i+1}/20")
                        time.sleep(0.5)

                    self.status_var.set("Mixed Load Finished")

                elif mode == "STOP":
                    self.status_var.set("Stopping all loads...")
                    subprocess.run(["docker", "exec", "-i", container, "pkill", "pgbench"],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    subprocess.run(["docker", "exec", "-i", container, "pkill", "psql"],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    self.status_var.set("All Loads Stopped")

            except Exception as e:
                self.status_var.set(f"Error: {str(e)}")

        thread = threading.Thread(target=run_load, daemon=True)
        thread.start()

    def run_specialized_test(self, test_type):
        """Запускает специализированный тестировщик для конкретного типа нагрузки"""
        def run_test():
            self.benchmark_status_var.set(f"Running {test_type} tester...")

            profile_map = {
                "OLTP": "Classic OLTP",
                "OLAP": "Heavy OLAP",
                "IoT": "IoT / Ingestion",
                "Mixed": "Mixed / HTAP"
            }

            profile_name = profile_map.get(test_type, test_type)

            if test_type == "OLTP":
                results = self.benchmark_runner.run_oltp_test(profile_name, duration=25)
            elif test_type == "OLAP":
                results = self.benchmark_runner.run_olap_test(profile_name, duration=25)
            elif test_type == "IoT":
                results = self.benchmark_runner.run_iot_test(profile_name, duration=25)
            elif test_type == "Mixed":
                results = self.benchmark_runner.run_mixed_test(profile_name, duration=25)
            else:
                results = {'error': f'Unknown test type: {test_type}'}

            if 'error' in results:
                self.benchmark_status_var.set(f"Error: {results['error']}")
                self._update_results_text(f"❌ {test_type} test failed:\n{results['error']}")
            else:
                tps = results.get('tps', 0)
                latency = results.get('avg_latency', 0)
                self.benchmark_status_var.set(f"Done: {tps:.1f} TPS, {latency:.2f}ms latency")

                result_text = f"✅ {test_type.upper()} TEST RESULTS:\n\n"
                result_text += f"• Profile: {profile_name}\n"
                result_text += f"• TPS: {tps:.1f}\n"
                result_text += f"• Latency: {latency:.2f} ms\n"
                result_text += f"• Test Type: {results.get('test_type', 'N/A')}\n"
                result_text += f"• Duration: {results.get('duration_minutes', 0)} min\n"
                result_text += f"• Clients: {results.get('clients', 0)}\n"

                self._update_results_text(result_text)

        thread = threading.Thread(target=run_test, daemon=True)
        thread.start()

    def run_auto_benchmarks(self):
        """Запускает автоматическое тестирование всех типов нагрузки"""
        def run_auto():
            self.benchmark_status_var.set("Running comprehensive test suite...")

            test_types = ["OLTP", "OLAP", "IoT", "Mixed"]

            for i, test_type in enumerate(test_types):
                self.benchmark_status_var.set(f"Testing {test_type} ({i+1}/{len(test_types)})...")
                self.run_specialized_test(test_type)

                # Ждем завершения теста
                time.sleep(30)  # Увеличили время ожидания между тестами

            self.benchmark_status_var.set("Comprehensive testing completed")
            self.show_benchmark_report()

        thread = threading.Thread(target=run_auto, daemon=True)
        thread.start()

    def cleanup_failed_tests(self):
        """Очищает неудачные тесты из БД"""
        def run_cleanup():
            self.benchmark_status_var.set("Cleaning failed tests...")
            deleted_count = self.benchmark_runner.cleanup_failed_tests()
            self.benchmark_status_var.set(f"Cleaned {deleted_count} failed tests")
            self.show_benchmark_report()

        thread = threading.Thread(target=run_cleanup, daemon=True)
        thread.start()

    def show_benchmark_report(self):
        """Показывает сравнительный отчет"""
        results = self.benchmark_runner.get_comparison_report()

        report_text = "=== COMPREHENSIVE BENCHMARK REPORT ===\n\n"

        if not results:
            report_text += "No benchmark data available. Run some tests first.\n"
            report_text += "Use specialized testers or 'Auto Test All' button."
        else:
            report_text += f"{'Profile':<20} {'Test Type':<10} {'TPS':<8} {'TPM':<8} {'Latency':<12} {'Tests':<6}\n"
            report_text += "-" * 75 + "\n"

            for profile, test_type, tps, tpm, latency, tests in results:
                tps_str = f"{tps or 0:<8.1f}"
                tpm_str = f"{tpm or 0:<8.0f}"
                latency_str = f"{latency or 0:<12.2f}"
                report_text += f"{profile:<20} {test_type:<10} {tps_str} {tpm_str} {latency_str} {tests:<6}\n"

            report_text += "\n📊 PERFORMANCE ANALYSIS:\n"
            report_text += "• TPS (Transactions/Queries/Inserts Per Second) - higher is better\n"
            report_text += "• TPM (Transactions/Queries/Inserts Per Minute) - higher is better\n"
            report_text += "• Latency (ms) - lower is better\n"
            report_text += "• Tests - number of successful test runs\n\n"

            # Анализ лучшего профиля
            if results:
                best_tps = max([r[2] for r in results if r[2] is not None])
                for profile, test_type, tps, tpm, latency, tests in results:
                    if tps == best_tps:
                        report_text += f"🏆 BEST PERFORMANCE: {profile} ({test_type}) with {tps:.1f} TPS\n"
                        break

        self._update_results_text(report_text)

    def _parse_pgbench_output(self, output):
        """Парсит вывод pgbench"""
        tps = 0.0
        avg_latency = 0.0

        tps_match = re.search(r'tps = (\d+\.\d+)', output)
        if tps_match:
            tps = float(tps_match.group(1))

        latency_match = re.search(r'latency average = (\d+\.\d+) ms', output)
        if latency_match:
            avg_latency = float(latency_match.group(1))

        return tps, avg_latency

    def _update_results_text(self, text):
        """Обновляет текстовое поле с результатами"""
        self.results_text.config(state=tk.NORMAL)
        self.results_text.delete(1.0, tk.END)
        self.results_text.insert(1.0, text)
        self.results_text.config(state=tk.DISABLED)

    def on_closing(self):
        self.running = False
        self.root.destroy()

if __name__ == "__main__":
    root = tk.Tk()
    app = SimpleVTBProfiler(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()

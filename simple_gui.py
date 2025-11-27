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
        self.root.title("VTB Load Profiler - Benchmark System")
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
        self.running = True

        self.setup_ui()
        self.start_updates()

    def setup_ui(self):
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Left panel - Benchmark Controller
        left_frame = ttk.LabelFrame(main_frame, text="Benchmark Controller", padding="15", width=300)
        left_frame.pack(side=tk.LEFT, fill=tk.Y, padx=(0, 10))
        left_frame.pack_propagate(False)

        # Title
        ttk.Label(left_frame, text="⚡ VTB Profiler", font=('Arial', 14, 'bold')).pack(anchor=tk.W, pady=(0, 10))
        ttk.Label(left_frame, text="BENCHMARK SYSTEM").pack(anchor=tk.W, pady=(0, 20))

        # Individual Benchmark Buttons
        ttk.Label(left_frame, text="RUN INDIVIDUAL TESTS:", font=('Arial', 11, 'bold')).pack(anchor=tk.W, pady=(0, 5))

        ttk.Button(left_frame, text="🧪 OLTP Benchmark",
                  command=lambda: self.run_benchmark("Classic OLTP", "OLTP"),
                  width=20).pack(fill=tk.X, pady=2)
        ttk.Button(left_frame, text="🧪 OLAP Benchmark",
                  command=lambda: self.run_benchmark("Heavy OLAP", "OLAP"),
                  width=20).pack(fill=tk.X, pady=2)
        ttk.Button(left_frame, text="🧪 IoT Benchmark",
                  command=lambda: self.run_benchmark("IoT / Ingestion", "IoT"),
                  width=20).pack(fill=tk.X, pady=2)
        ttk.Button(left_frame, text="🧪 Mixed Benchmark",
                  command=lambda: self.run_benchmark("Mixed / HTAP", "Mixed"),
                  width=20).pack(fill=tk.X, pady=2)
        ttk.Button(left_frame, text="🏭 TPC-C Benchmark",
                  command=lambda: self.run_benchmark("TPC-C OLTP", "TPC-C"),
                  width=20).pack(fill=tk.X, pady=2)

        ttk.Separator(left_frame, orient='horizontal').pack(fill=tk.X, pady=15)

        # Test Suite Controls
        ttk.Label(left_frame, text="TEST SUITE CONTROLS:", font=('Arial', 11, 'bold')).pack(anchor=tk.W, pady=(0, 5))

        ttk.Button(left_frame, text="🚀 Run Full Test Suite",
                  command=self.run_full_test_suite,
                  width=20).pack(fill=tk.X, pady=5)
        ttk.Button(left_frame, text="📊 Show Benchmark Report",
                  command=self.show_benchmark_report,
                  width=20).pack(fill=tk.X, pady=2)
        ttk.Button(left_frame, text="🔄 Clean Failed Tests",
                  command=self.cleanup_failed_tests,
                  width=20).pack(fill=tk.X, pady=2)

        # Status
        ttk.Separator(left_frame, orient='horizontal').pack(fill=tk.X, pady=15)
        ttk.Label(left_frame, text="Benchmark Status:").pack(anchor=tk.W)
        self.benchmark_status_var = tk.StringVar(value="Ready to run benchmarks")
        benchmark_status_label = ttk.Label(left_frame, textvariable=self.benchmark_status_var, foreground="darkgreen")
        benchmark_status_label.pack(anchor=tk.W, pady=(0, 10))

        # Progress
        self.progress_var = tk.StringVar(value="No active tests")
        ttk.Label(left_frame, textvariable=self.progress_var, font=('Arial', 9), foreground="blue").pack(anchor=tk.W)

        # Right panel - Dashboard
        right_frame = ttk.Frame(main_frame)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        # Profile detection
        profile_frame = ttk.LabelFrame(right_frame, text="System Status & Profile Detection", padding="15")
        profile_frame.pack(fill=tk.X, pady=(0, 10))

        self.profile_var = tk.StringVar(value="SYSTEM IDLE")
        self.confidence_var = tk.StringVar(value="Run benchmarks to see performance data")

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
        chart_frame = ttk.LabelFrame(right_frame, text="Performance Metrics During Benchmarks", padding="10")
        chart_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        # Create matplotlib figure
        self.fig, (self.ax1, self.ax2) = plt.subplots(2, 1, figsize=(10, 6))
        self.canvas = FigureCanvasTkAgg(self.fig, master=chart_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        # Results text area
        results_frame = ttk.LabelFrame(right_frame, text="Benchmark Results & AI Recommendations", padding="10")
        results_frame.pack(fill=tk.BOTH, expand=False)

        self.results_text = scrolledtext.ScrolledText(results_frame, height=12, width=100)
        self.results_text.pack(fill=tk.BOTH, expand=True)
        self.results_text.insert(tk.END, "=== VTB LOAD PROFILER - BENCHMARK SYSTEM ===\n\n"
                                       "Welcome! Use the benchmark controls to:\n"
                                       "• Run individual load tests (OLTP, OLAP, IoT, Mixed, TPC-C)\n"
                                       "• Execute full test suite for comprehensive comparison\n"
                                       "• View detailed benchmark reports with performance analysis\n"
                                       "• Get AI-powered configuration recommendations\n\n"
                                       "Click 'Run Full Test Suite' to start!")
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

            # Update Recommendations only when we have meaningful data
            if metrics["TPS"] > 1 or metrics["Active Sessions (ASH)"] > 0.5:
                base_name = profile.split(" (")[0]
                if base_name in self.profiles_db:
                    data = self.profiles_db[base_name]
                    text = f"🤖 AI CONFIG RECOMMENDATIONS for {base_name}:\n\n"
                    for k, v in data.items():
                        text += f"• {k} = {v}\n"

                    # Only update if we're not showing benchmark results
                    current_text = self.results_text.get(1.0, tk.END)
                    if "BENCHMARK RESULTS" not in current_text and "COMPREHENSIVE BENCHMARK" not in current_text:
                        self._update_results_text(text)

        except Exception as e:
            print(f"❌ Error updating stats: {e}")

    def run_benchmark(self, profile_name, test_type):
        """Запускает один бенчмарк"""
        def run_test():
            self.benchmark_status_var.set(f"Running {test_type} benchmark...")
            self.progress_var.set(f"Testing: {profile_name}")

            # Map test type to benchmark method
            test_methods = {
                "OLTP": self.benchmark_runner.run_oltp_test,
                "OLAP": self.benchmark_runner.run_olap_test,
                "IoT": self.benchmark_runner.run_iot_test,
                "Mixed": self.benchmark_runner.run_mixed_test,
                "TPC-C": self.benchmark_runner.run_tpcc_test
            }

            method = test_methods.get(test_type)
            if not method:
                self.benchmark_status_var.set(f"Error: Unknown test type {test_type}")
                return

            # Set appropriate duration for each test type
            if test_type == "TPC-C":
                results = method(profile_name, duration=120)  # 2 minutes for TPC-C
            else:
                results = method(profile_name, duration=25)   # 25 seconds for others

            if 'error' in results:
                self.benchmark_status_var.set(f"Error: {results['error']}")
                self._update_results_text(f"❌ {test_type} benchmark failed:\n{results['error']}")
            else:
                tps = results.get('tps', 0)
                latency = results.get('avg_latency', 0)
                self.benchmark_status_var.set(f"Completed: {tps:.1f} TPS, {latency:.2f}ms latency")
                self.progress_var.set("Test completed successfully")

                result_text = f"✅ {test_type.upper()} BENCHMARK RESULTS:\n\n"
                result_text += f"• Profile: {profile_name}\n"
                result_text += f"• TPS: {tps:.1f}\n"
                result_text += f"• TPM: {results.get('tpm', 0):.0f}\n"
                result_text += f"• Average Latency: {latency:.2f} ms\n"
                result_text += f"• Duration: {results.get('duration_minutes', 0)} minutes\n"
                result_text += f"• Clients: {results.get('clients', 0)}\n\n"

                # Add performance analysis
                if test_type == "TPC-C":
                    if tps > 100:
                        result_text += "📈 EXCELLENT PERFORMANCE - System handles realistic OLTP load well\n"
                    elif tps > 50:
                        result_text += "📊 GOOD PERFORMANCE - Solid TPC-C throughput\n"
                    else:
                        result_text += "⚠️  MODERATE PERFORMANCE - Consider tuning configuration\n"
                else:
                    if tps > 1000:
                        result_text += "📈 EXCELLENT PERFORMANCE - System handles high transactional load well\n"
                    elif tps > 500:
                        result_text += "📊 GOOD PERFORMANCE - Solid transactional throughput\n"
                    else:
                        result_text += "⚠️  MODERATE PERFORMANCE - Consider tuning configuration\n"

                self._update_results_text(result_text)

        thread = threading.Thread(target=run_test, daemon=True)
        thread.start()

    def run_full_test_suite(self):
        """Запускает полную батарею тестов"""
        def run_suite():
            self.benchmark_status_var.set("Starting comprehensive benchmark suite...")

            test_sequence = [
                ("Classic OLTP", "OLTP"),
                ("Heavy OLAP", "OLAP"),
                ("IoT / Ingestion", "IoT"),
                ("Mixed / HTAP", "Mixed"),
                ("TPC-C Standard", "TPC-C")
            ]

            total_tests = len(test_sequence)

            for i, (profile, test_type) in enumerate(test_sequence):
                current_test = i + 1
                self.benchmark_status_var.set(f"Running test {current_test}/{total_tests}: {test_type}")
                self.progress_var.set(f"Progress: {current_test}/{total_tests} - {profile}")

                # Run the benchmark with appropriate duration
                test_methods = {
                    "OLTP": self.benchmark_runner.run_oltp_test,
                    "OLAP": self.benchmark_runner.run_olap_test,
                    "IoT": self.benchmark_runner.run_iot_test,
                    "Mixed": self.benchmark_runner.run_mixed_test,
                    "TPC-C": self.benchmark_runner.run_tpcc_test
                }

                method = test_methods.get(test_type)
                if method:
                    if test_type == "TPC-C":
                        method(profile, duration=120)  # 2 minutes for TPC-C
                    else:
                        method(profile, duration=20)   # 20 seconds for others

                # Show progress
                progress_text = f"🏃‍♂️ Test Suite Progress: {current_test}/{total_tests} completed\n"
                progress_text += f"Current: {test_type} - {profile}\n"
                if i + 1 < total_tests:
                    progress_text += f"Next: {test_sequence[i+1][1]} - {test_sequence[i+1][0]}\n"
                else:
                    progress_text += "Next: COMPLETION\n"

                self._update_results_text(progress_text)

                # Wait between tests (except after the last one)
                if i < total_tests - 1:
                    time.sleep(5)

            self.benchmark_status_var.set("Full test suite completed!")
            self.progress_var.set("All tests finished - ready for analysis")
            self.show_benchmark_report()

        thread = threading.Thread(target=run_suite, daemon=True)
        thread.start()

    def cleanup_failed_tests(self):
        """Очищает неудачные тесты из БД"""
        def run_cleanup():
            self.benchmark_status_var.set("Cleaning failed tests...")
            deleted_count = self.benchmark_runner.cleanup_failed_tests()
            self.benchmark_status_var.set(f"Cleaned {deleted_count} failed tests")
            self.progress_var.set("Database maintenance completed")
            self.show_benchmark_report()

        thread = threading.Thread(target=run_cleanup, daemon=True)
        thread.start()

    def show_benchmark_report(self):
        """Показывает сравнительный отчет всех бенчмарков"""
        results = self.benchmark_runner.get_comparison_report()

        report_text = "=== COMPREHENSIVE BENCHMARK REPORT ===\n\n"

        if not results:
            report_text += "No benchmark data available.\n"
            report_text += "Run individual tests or the full test suite first.\n\n"
            report_text += "Recommended: Click 'Run Full Test Suite' for complete analysis."
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

            # Performance ranking and recommendations
            if results:
                # Find best performing profile
                best_tps = max([r[2] for r in results if r[2] is not None])
                best_profile = None
                for profile, test_type, tps, tpm, latency, tests in results:
                    if tps == best_tps:
                        best_profile = (profile, test_type, tps, latency)
                        break

                if best_profile:
                    report_text += f"🏆 BEST PERFORMANCE: {best_profile[0]} ({best_profile[1]})\n"
                    report_text += f"   • TPS: {best_profile[2]:.1f}\n"
                    report_text += f"   • Latency: {best_profile[3]:.2f} ms\n\n"

                # Configuration recommendations
                report_text += "🤖 AI CONFIGURATION RECOMMENDATIONS:\n"
                for profile, test_type, tps, tpm, latency, tests in results:
                    if profile in self.profiles_db:
                        data = self.profiles_db[profile]
                        report_text += f"\n{profile}:\n"
                        for k, v in data.items():
                            report_text += f"  • {k} = {v}\n"

        self._update_results_text(report_text)

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

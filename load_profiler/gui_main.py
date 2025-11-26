import sys
import subprocess
import psycopg2
from collections import deque
from PyQt6.QtWidgets import (QApplication, QMainWindow, QVBoxLayout, QHBoxLayout, 
                             QWidget, QLabel, QFrame, QTextEdit, QTableWidget, 
                             QTableWidgetItem, QHeaderView, QPushButton, QProgressBar)
from PyQt6.QtCore import QTimer, Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt

# Импорты логики
from config import DB_CONFIG, ANALYSIS_INTERVAL
from metrics import MetricsCollector
from analyzer import ProfileAnalyzer
from db_loader import load_profiles_from_db

# --- ФУНКЦИЯ ЗАГРУЗКИ СТИЛЕЙ ---
def load_stylesheet(filename):
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        print(f"Warning: Could not load stylesheet: {e}")
        return ""

# --- ПОТОК ГЕНЕРАЦИИ НАГРУЗКИ ---
class LoadGeneratorThread(QThread):
    finished_signal = pyqtSignal(str)
    
    def __init__(self, mode):
        super().__init__()
        self.mode = mode
        self.running = True

    def run(self):
        container = "vtb_postgres"
        try:
            if self.mode == "OLTP":
                # pgbench (работает 60 сек)
                cmd = ["docker", "exec", "-i", container, "pgbench", "-c", "10", "-j", "2", "-T", "60", "-U", "user", "mydb"]
                self.process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                while self.process.poll() is None:
                    if not self.running:
                        self.process.terminate()
                        break
                    self.msleep(100)
                self.finished_signal.emit("OLTP Load Finished")

            elif self.mode == "OLAP":
                # Цикл тяжелых запросов в Python
                sql = "SELECT count(*) FROM pgbench_accounts a, pgbench_branches b WHERE a.bid = b.bid"
                cmd = ["docker", "exec", "-i", container, "psql", "-U", "user", "-d", "mydb", "-c", sql]
                
                for _ in range(60): # ~60 итераций
                    if not self.running: break
                    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    self.msleep(500)
                self.finished_signal.emit("OLAP Load Finished")

            elif self.mode == "IoT":
                # Цикл быстрых вставок
                sql = "INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (1, 1, 1, 0, CURRENT_TIMESTAMP);"
                cmd = ["docker", "exec", "-i", container, "psql", "-U", "user", "-d", "mydb", "-c", sql]
                
                for _ in range(200): # Быстрый цикл
                    if not self.running: break
                    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                self.finished_signal.emit("IoT Load Finished")

            elif self.mode == "STOP":
                self.running = False
                subprocess.run(["docker", "exec", "-i", container, "pkill", "pgbench"])
                self.finished_signal.emit("Load Stopped")

        except Exception as e:
            self.finished_signal.emit(f"Error: {str(e)}")

    def stop(self):
        self.running = False

# --- UI КОМПОНЕНТЫ ---
class MetricCard(QFrame):
    def __init__(self, title, value, color="#ffffff"):
        super().__init__()
        self.setObjectName("Card")
        layout = QVBoxLayout(self)
        self.lbl_title = QLabel(title)
        self.lbl_title.setStyleSheet("color: #aaaaaa; font-size: 12px;")
        
        self.lbl_val = QLabel(str(value))
        self.lbl_val.setObjectName("MetricValue")
        self.lbl_val.setStyleSheet(f"color: {color};")
        
        layout.addWidget(self.lbl_title)
        layout.addWidget(self.lbl_val)
        layout.addStretch()

    def update_val(self, value):
        self.lbl_val.setText(str(value))

class MplCanvas(FigureCanvas):
    def __init__(self, width=5, height=4, dpi=100):
        plt.style.use('dark_background')
        self.fig = Figure(figsize=(width, height), dpi=dpi)
        self.fig.patch.set_facecolor('#1e1e1e')
        
        gs = self.fig.add_gridspec(2, 1, hspace=0.3)
        self.ax1 = self.fig.add_subplot(gs[0])
        self.ax2 = self.fig.add_subplot(gs[1])
        super().__init__(self.fig)

# --- ГЛАВНОЕ ОКНО ---
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("VTB Load Profiler | Control Center")
        self.resize(1280, 850)
        
        # ! ПОДКЛЮЧАЕМ СТИЛИ ИЗ ФАЙЛА !
        self.setStyleSheet(load_stylesheet("style.qss"))

        # Logic Init
        try:
            self.collector = MetricsCollector(DB_CONFIG)
            self.analyzer = ProfileAnalyzer()
            self.profiles_db = load_profiles_from_db()
            self.prev_snapshot = self.collector.get_snapshot()
        except Exception as e:
            print(f"Connection Error: {e}")
            sys.exit(1)

        self.history_tps = deque([0]*60, maxlen=60)
        self.history_lat = deque([0]*60, maxlen=60)
        self.load_thread = None

        # --- LAYOUT ---
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)
        main_layout.setContentsMargins(0,0,0,0)
        main_layout.setSpacing(0)

        # === ЛЕВАЯ ПАНЕЛЬ (SIDEBAR) ===
        sidebar = QFrame()
        sidebar.setObjectName("Sidebar")
        sidebar.setFixedWidth(260)
        sidebar_layout = QVBoxLayout(sidebar)
        sidebar_layout.setContentsMargins(20, 30, 20, 20)
        sidebar_layout.setSpacing(10)

        lbl_logo = QLabel("⚡ VTB Profiler")
        lbl_logo.setObjectName("Title")
        sidebar_layout.addWidget(lbl_logo)
        sidebar_layout.addWidget(QLabel("LOAD CONTROLLER"))
        sidebar_layout.addSpacing(10)
        
        # Кнопки
        self.btn_oltp = QPushButton("  🏦  Start OLTP Load")
        self.btn_oltp.setObjectName("BtnStartOLTP")
        self.btn_oltp.clicked.connect(lambda: self.start_load("OLTP"))
        
        self.btn_olap = QPushButton("  📊  Start OLAP Load")
        self.btn_olap.setObjectName("BtnStartOLAP")
        self.btn_olap.clicked.connect(lambda: self.start_load("OLAP"))
        
        self.btn_iot = QPushButton("  📡  Start IoT Load")
        self.btn_iot.setObjectName("BtnStartIoT")
        self.btn_iot.clicked.connect(lambda: self.start_load("IoT"))

        self.btn_stop = QPushButton("⛔ STOP LOAD")
        self.btn_stop.setObjectName("BtnStop")
        self.btn_stop.clicked.connect(lambda: self.start_load("STOP"))

        sidebar_layout.addWidget(self.btn_oltp)
        sidebar_layout.addWidget(self.btn_olap)
        sidebar_layout.addWidget(self.btn_iot)
        sidebar_layout.addWidget(self.btn_stop)
        
        # Статус
        self.lbl_status = QLabel("Ready")
        self.lbl_status.setStyleSheet("color: #666; font-style: italic; margin-top: 15px;")
        self.progress = QProgressBar()
        self.progress.setVisible(False)
        
        sidebar_layout.addWidget(self.lbl_status)
        sidebar_layout.addWidget(self.progress)
        sidebar_layout.addStretch()
        main_layout.addWidget(sidebar)

        # === ПРАВАЯ ЧАСТЬ (DASHBOARD) ===
        dashboard = QWidget()
        dash_layout = QVBoxLayout(dashboard)
        dash_layout.setContentsMargins(30, 30, 30, 30)
        dash_layout.setSpacing(20)

        # Header
        header_frame = QFrame()
        header_frame.setObjectName("Card")
        header_layout = QHBoxLayout(header_frame)
        
        self.lbl_profile = QLabel("DETECTING...")
        self.lbl_profile.setFont(QFont("Segoe UI", 26, QFont.Weight.Bold))
        self.lbl_profile.setStyleSheet("color: #888;")
        
        self.lbl_conf = QLabel("Confidence: --")
        
        header_layout.addWidget(self.lbl_profile)
        header_layout.addSpacing(20)
        header_layout.addWidget(self.lbl_conf)
        header_layout.addStretch()
        dash_layout.addWidget(header_frame)

        # KPI
        kpi_layout = QHBoxLayout()
        self.card_tps = MetricCard("TPS (Trans/sec)", "0", "#00ff00")
        self.card_lat = MetricCard("Tx Cost (ASH)", "0.000s", "#ff3333")
        self.card_ash = MetricCard("Active Sessions", "0", "#00ccff")
        self.card_io = MetricCard("IO Wait Events", "0", "#ffaa00")
        
        kpi_layout.addWidget(self.card_tps)
        kpi_layout.addWidget(self.card_lat)
        kpi_layout.addWidget(self.card_ash)
        kpi_layout.addWidget(self.card_io)
        dash_layout.addLayout(kpi_layout)

        # Charts
        chart_frame = QFrame()
        chart_frame.setObjectName("Card")
        chart_layout = QVBoxLayout(chart_frame)
        self.canvas = MplCanvas(width=5, height=5)
        chart_layout.addWidget(self.canvas)
        dash_layout.addWidget(chart_frame, stretch=2)

        # Recs
        dash_layout.addWidget(QLabel("🤖 AI CONFIG RECOMMENDATIONS"))
        self.txt_recs = QTextEdit()
        self.txt_recs.setReadOnly(True)
        self.txt_recs.setMaximumHeight(120)
        dash_layout.addWidget(self.txt_recs)

        main_layout.addWidget(dashboard)

        # Timer
        self.timer = QTimer()
        self.timer.setInterval(ANALYSIS_INTERVAL * 1000)
        self.timer.timeout.connect(self.update_stats)
        self.timer.start()

    def start_load(self, mode):
        # Остановка предыдущего потока
        if self.load_thread is not None and self.load_thread.isRunning():
            self.load_thread.stop()
            self.load_thread.wait()

        if mode == "STOP":
            self.lbl_status.setText("Stopping...")
            self.progress.setVisible(False)
        else:
            self.lbl_status.setText(f"Generating {mode} load...")
            self.progress.setVisible(True)
            self.progress.setRange(0, 0)
            
            # Старт нового потока
            self.load_thread = LoadGeneratorThread(mode)
            self.load_thread.finished_signal.connect(self.on_load_finished)
            self.load_thread.start()

    def on_load_finished(self, msg):
        self.lbl_status.setText(msg)
        self.progress.setVisible(False)

    def update_stats(self):
        curr_snapshot = self.collector.get_snapshot()
        profile, conf, metrics = self.analyzer.analyze(self.prev_snapshot, curr_snapshot, ANALYSIS_INTERVAL)
        self.prev_snapshot = curr_snapshot

        if not self.profiles_db:
             self.profiles_db = load_profiles_from_db()

        # Update UI
        self.card_tps.update_val(int(metrics["TPS"]))
        self.card_lat.update_val(f"{metrics['Tx Cost (s)']:.4f}s")
        self.card_ash.update_val(metrics["Active Sessions (ASH)"])
        self.card_io.update_val(metrics["IO Waits"])

        self.lbl_profile.setText(f"PROFILE: {profile}")
        self.lbl_conf.setText(f"Confidence: {conf}")
        
        color = "#888888"
        if "OLTP" in profile: color = "#00ff00"
        elif "OLAP" in profile: color = "#ff4444"
        elif "IoT" in profile: color = "#33b5e5"
        elif "Mixed" in profile: color = "#ffbb33"
        self.lbl_profile.setStyleSheet(f"color: {color}; font-weight: bold; font-size: 26px;")

        # Update Charts
        self.history_tps.append(metrics["TPS"])
        self.history_lat.append(metrics["Tx Cost (s)"])

        ax1 = self.canvas.ax1
        ax1.clear()
        ax1.plot(self.history_tps, color='#00ff00', linewidth=2, alpha=0.8)
        ax1.fill_between(range(len(self.history_tps)), self.history_tps, color='#00ff00', alpha=0.1)
        ax1.set_title("Transactions Per Second (TPS)", color='white', fontsize=10, loc='left')
        ax1.grid(True, linestyle=':', alpha=0.3)
        ax1.set_facecolor('#1e1e1e')

        ax2 = self.canvas.ax2
        ax2.clear()
        ax2.plot(self.history_lat, color='#ff4444', linewidth=2, alpha=0.8)
        ax2.fill_between(range(len(self.history_lat)), self.history_lat, color='#ff4444', alpha=0.1)
        ax2.set_title("Latency Cost (ASH/Commit)", color='white', fontsize=10, loc='left')
        ax2.grid(True, linestyle=':', alpha=0.3)
        ax2.set_facecolor('#1e1e1e')
        
        self.canvas.draw()

        # Update Recs
        base_name = profile.split(" (")[0]
        if base_name in self.profiles_db:
            data = self.profiles_db[base_name]
            text = f"# Config for {base_name}\n"
            for k, v in data.items():
                text += f"{k} = {v}\n"
            self.txt_recs.setText(text)
        else:
            self.txt_recs.setText("# System Stable. Waiting for load...")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
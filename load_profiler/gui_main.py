import sys
import subprocess
import psycopg2
from collections import deque
from PyQt6.QtWidgets import (QApplication, QMainWindow, QVBoxLayout, QHBoxLayout,
                             QWidget, QLabel, QFrame, QTextEdit, QTableWidget,
                             QTableWidgetItem, QHeaderView, QPushButton, QProgressBar)
from PyQt6.QtCore import QTimer, Qt, QThread, pyqtSignal
from PyQt6.QtGui import QFont, QColor
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt

# Импорты логики
from config import DB_CONFIG, ANALYSIS_INTERVAL
# Предполагаем, что metrics.py и analyzer.py обновлены
from metrics import MetricsCollector
from analyzer import ProfileAnalyzer
from db_loader import load_profiles_from_db

# --- ФУНКЦИЯ ЗАГРУЗКИ СТИЛЕЙ (Осталась без изменений) ---
def load_stylesheet(filename):
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        print(f"Warning: Could not load stylesheet: {e}")
        return ""

# --- ПОТОК ГЕНЕРАЦИИ НАГРУЗКИ (Изменен для запуска SQL-сценариев) ---
class LoadGeneratorThread(QThread):
    finished_signal = pyqtSignal(str)

    def __init__(self, mode):
        super().__init__()
        self.mode = mode

    def run(self):
        # Контейнер для выполнения команд psql, где лежат .sql файлы
        # Согласно docker-compose, это metrics-collector, и он подключен к db
        container = "metrics-collector"

        # Определяем имя файла сценария (предполагаем, что olap_scenario.sql - это OLAP, а hybrid_scenario.sql - HYBRID)
        if self.mode == "OLTP":
            # Для OLTP используем pgbench, как наиболее стабильный инструмент
            # Запускаем в фоне, чтобы GUI не вис
            cmd = [
                "docker", "exec", container,
                "pgbench", "-i", "-s", "10", "-h", "db", "-U", "user", "mydb", "&&",
                "pgbench", "-T", "60", "-c", "10", "-j", "2", "-h", "db", "-U", "user", "mydb"
            ]
            scenario_file = "pgbench (60s)"
        elif self.mode == "OLAP":
            # Запуск вашего кастомного OLAP сценария
            cmd = [
                "docker", "exec", container,
                "psql", "-h", "db", "-U", "user", "-d", "mydb", "-f", "/workloads/olap_scenario.sql"
            ]
            scenario_file = "olap_scenario.sql"
        elif self.mode == "HYBRID":
            # Запуск вашего кастомного HYBRID сценария
            cmd = [
                "docker", "exec", container,
                "psql", "-h", "db", "-U", "user", "-d", "mydb", "-f", "/workloads/hybrid_scenario.sql"
            ]
            scenario_file = "hybrid_scenario.sql"
        else:
            self.finished_signal.emit(f"Неизвестный режим нагрузки: {self.mode}")
            return

        try:
            print(f"Запуск сценария {scenario_file}...")
            # Выполняем команду
            subprocess.run(cmd, check=True)

            self.finished_signal.emit(f"Сценарий {self.mode} ({scenario_file}) завершен.")

        except subprocess.CalledProcessError as e:
            self.finished_signal.emit(f"Ошибка выполнения Docker-команды для {self.mode}: {e}")
        except Exception as e:
            self.finished_signal.emit(f"Непредвиденная ошибка при запуске нагрузки: {e}")

# --- КЛАСС ГРАФИКА ---
class MplCanvas(FigureCanvas):
    def __init__(self, parent=None, width=5, height=4, dpi=100):
        # Создаем фигуру с темным фоном
        self.fig = Figure(figsize=(width, height), dpi=dpi, facecolor='#1e1e1e')

        # Создаем два суб-графика
        self.ax1 = self.fig.add_subplot(211)
        self.ax2 = self.fig.add_subplot(212)

        FigureCanvas.__init__(self, self.fig)
        self.setParent(parent)

        # Настраиваем стили
        for ax in [self.ax1, self.ax2]:
            ax.tick_params(colors='lightgrey', labelcolor='lightgrey')
            ax.spines['left'].set_color('lightgrey')
            ax.spines['bottom'].set_color('lightgrey')
            ax.set_facecolor('#1e1e1e')

        self.fig.tight_layout(pad=1.5)


# --- ГЛАВНОЕ ОКНО ПРИЛОЖЕНИЯ ---
class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        self.setWindowTitle("VTB Education Hack: PostgreSQL Load Profile Analyzer")
        self.setGeometry(100, 100, 1200, 800)

        self.setStyleSheet(load_stylesheet("styles.qss")) # Предполагается наличие styles.qss

        self.collector = MetricsCollector(DB_CONFIG)
        self.analyzer = ProfileAnalyzer()
        self.profiles_recs = load_profiles_from_db()
        self.metrics_history = deque(maxlen=2)
        self.history_tps = deque(maxlen=100)
        self.history_lat = deque(maxlen=100)

        self.last_profile = "IDLE"

        self.init_ui()
        self.start_analysis()

    def init_ui(self):
        # Главный контейнер
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        self.main_layout = QHBoxLayout(main_widget)

        # --- ЛЕВАЯ ПАНЕЛЬ (Графики) ---
        chart_panel = QWidget()
        chart_layout = QVBoxLayout(chart_panel)

        self.canvas = MplCanvas(self, width=6, height=7, dpi=100)
        chart_layout.addWidget(self.canvas)

        self.main_layout.addWidget(chart_panel, 2) # Занимает 2/3 ширины

        # --- ПРАВАЯ ПАНЕЛЬ (Метрики, Статус, Управление) ---
        info_panel = QWidget()
        info_layout = QVBoxLayout(info_panel)

        # 1. Секция Статуса и Профиля
        self.profile_label = QLabel("Профиль: IDLE")
        self.profile_label.setStyleSheet("font-size: 24px; font-weight: bold; color: #4CAF50;") # Начальный цвет: зеленый
        info_layout.addWidget(self.profile_label)

        self.confidence_label = QLabel("Уверенность: High")
        self.confidence_label.setStyleSheet("font-size: 16px; color: lightgrey;")
        info_layout.addWidget(self.confidence_label)

        # 2. Текущие метрики
        self.metrics_text = QTextEdit()
        self.metrics_text.setReadOnly(True)
        self.metrics_text.setFixedHeight(200)
        self.metrics_text.setFont(QFont("Monospace", 10))
        info_layout.addWidget(QLabel("Текущие ключевые метрики:"))
        info_layout.addWidget(self.metrics_text)

        # 3. Рекомендации (Таблица)
        info_layout.addWidget(QLabel("Рекомендации PostgreSQL (postgresql.conf):"))
        self.recs_table = QTableWidget(0, 2)
        self.recs_table.setHorizontalHeaderLabels(["Параметр", "Значение"])
        self.recs_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.recs_table.setFixedWidth(400)
        info_layout.addWidget(self.recs_table)

        # 4. Управление нагрузкой (Кнопки для кастомных сценариев)
        info_layout.addWidget(QLabel("Запуск тестовых сценариев:"))
        load_layout = QHBoxLayout()

        # Кнопки для ваших кастомных сценариев
        self.btn_oltp = QPushButton("▶️ OLTP (pgbench)")
        self.btn_oltp.clicked.connect(lambda: self.start_load("OLTP"))
        load_layout.addWidget(self.btn_oltp)

        self.btn_olap = QPushButton("▶️ OLAP (Scenario)")
        self.btn_olap.clicked.connect(lambda: self.start_load("OLAP"))
        load_layout.addWidget(self.btn_olap)

        self.btn_hybrid = QPushButton("▶️ HYBRID (Scenario)")
        self.btn_hybrid.clicked.connect(lambda: self.start_load("HYBRID"))
        load_layout.addWidget(self.btn_hybrid)

        info_layout.addLayout(load_layout)

        # 5. Индикатор выполнения нагрузки
        self.load_status_label = QLabel("Статус нагрузки: Ожидание.")
        info_layout.addWidget(self.load_status_label)

        info_layout.addStretch(1) # Заполняем пустое пространство
        self.main_layout.addWidget(info_panel, 1) # Занимает 1/3 ширины

    def start_analysis(self):
        # Начинаем сбор метрик сразу
        self.timer = QTimer()
        self.timer.setInterval(ANALYSIS_INTERVAL * 1000)
        self.timer.timeout.connect(self.update_metrics_and_gui)

        # Сначала собираем baseline (первый снимок)
        try:
            baseline = self.collector.get_snapshot()
            self.metrics_history.append(baseline)
            self.timer.start()
            self.load_status_label.setText("Статус анализа: Анализ активен.")
        except ConnectionError as e:
            self.profile_label.setText("ОШИБКА ПОДКЛЮЧЕНИЯ")
            self.metrics_text.setText(str(e))
            self.load_status_label.setText("ОШИБКА: Проверьте Docker и порт 5433.")


    def update_metrics_and_gui(self):
        try:
            curr = self.collector.get_snapshot()
            self.metrics_history.append(curr)

            # Для анализа нужно минимум 2 снимка
            if len(self.metrics_history) < 2:
                return

            prev = self.metrics_history.popleft()
            duration = curr["time"] - prev["time"]

            # --- 1. АНАЛИЗ ---
            profile, confidence, metrics = self.analyzer.analyze(prev, curr, duration)

            # --- 2. ОБНОВЛЕНИЕ GUI ---

            # Обновление метрик
            metrics_display = "\n".join([f"{k}: {v}" for k, v in metrics.items()])
            self.metrics_text.setText(metrics_display)

            # Обновление Статуса и Профиля
            self.profile_label.setText(f"Профиль: {profile}")
            self.confidence_label.setText(f"Уверенность: {confidence}")

            # Светофор для уверенности
            color_map = {"High": "#4CAF50", "Medium": "#FFC107", "Low": "#F44336", "Unknown": "#9E9E9E"}
            self.profile_label.setStyleSheet(f"font-size: 24px; font-weight: bold; color: {color_map.get(confidence, 'white')};")

            # --- Обновление Графиков ---
            self.history_tps.append(metrics["TPS"])
            self.history_lat.append(metrics["Tx Cost (s)"])

            ax1 = self.canvas.ax1
            ax1.clear()
            ax1.plot(self.history_tps, color='#00ff00', linewidth=2, alpha=0.8)
            ax1.set_title("Transactions Per Second (TPS)", color='white', fontsize=10, loc='left')
            ax1.grid(True, linestyle=':', alpha=0.3)
            ax1.set_facecolor('#1e1e1e')

            ax2 = self.canvas.ax2
            ax2.clear()
            ax2.plot(self.history_lat, color='#ff4444', linewidth=2, alpha=0.8)
            ax2.set_title("Tx Cost (ASH/Commit, s)", color='white', fontsize=10, loc='left')
            ax2.grid(True, linestyle=':', alpha=0.3)
            ax2.set_facecolor('#1e1e1e')

            # **ЛОГИКА МАРКЕРА СМЕЩЕНИЯ ПРОФИЛЯ**
            if profile != self.last_profile and self.last_profile != "Unknown":
                # Добавляем вертикальную линию при смене профиля
                idx = len(self.history_tps) - 1
                ax1.axvline(x=idx, color='yellow', linestyle='--', alpha=0.7, label=f"Смена на {profile}")
                ax2.axvline(x=idx, color='yellow', linestyle='--', alpha=0.7)

            self.last_profile = profile

            self.canvas.draw()

            # --- Обновление Рекомендаций ---
            self.update_recommendations(profile)

            # Сохраняем текущий снимок для следующего цикла
            self.metrics_history.append(curr)

        except Exception as e:
            self.profile_label.setText("ОШИБКА АНАЛИЗА")
            self.metrics_text.setText(f"Ошибка в цикле анализа: {e}")
            # В случае ошибки, сбросить таймер или отключить его, чтобы не спамить
            self.timer.stop()


    def update_recommendations(self, profile):
        """Обновляет таблицу рекомендаций на основе текущего профиля."""

        # Убираем лишние слова типа " (CPU-Bound)" для поиска в словаре
        base_name = profile.split(" / ")[0].split(" (")[0]

        recs = self.profiles_recs.get(base_name, {})

        self.recs_table.setRowCount(len(recs))
        row = 0
        for param, value in recs.items():
            param_item = QTableWidgetItem(param)
            value_item = QTableWidgetItem(str(value))

            # Стилизация, чтобы было видно
            param_item.setForeground(QColor("#00ff00")) # зеленый для параметра

            self.recs_table.setItem(row, 0, param_item)
            self.recs_table.setItem(row, 1, value_item)
            row += 1


    def start_load(self, mode):
        """Запускает генератор нагрузки в отдельном потоке."""

        # Блокировка кнопок во время выполнения
        for btn in [self.btn_oltp, self.btn_olap, self.btn_hybrid]:
            btn.setEnabled(False)

        self.load_status_label.setText(f"Статус нагрузки: Запуск сценария {mode}...")

        self.load_thread = LoadGeneratorThread(mode)
        self.load_thread.finished_signal.connect(self.load_finished)
        self.load_thread.start()

    def load_finished(self, message):
        """Обрабатывает завершение потока нагрузки."""
        self.load_status_label.setText(f"Статус нагрузки: {message}")

        # Разблокировка кнопок
        for btn in [self.btn_oltp, self.btn_olap, self.btn_hybrid]:
            btn.setEnabled(True)

        # Немедленный сбор метрик после завершения нагрузки
        self.update_metrics_and_gui()

if __name__ == '__main__':
    # Обязательно устанавливаем бэкенд matplotlib
    plt.style.use('dark_background')

    app = QApplication(sys.argv)

    # Стилизация (минимальная, если styles.qss не найден)
    app.setStyleSheet("""
        QMainWindow { background-color: #1e1e1e; }
        QLabel { color: lightgrey; }
        QTextEdit { background-color: #333333; color: white; border: 1px solid #555; }
        QPushButton {
            background-color: #333333;
            color: white;
            border: 1px solid #555;
            padding: 5px;
        }
        QPushButton:hover { background-color: #444444; }
        QTableWidget {
            background-color: #333333;
            color: white;
            border: 1px solid #555;
            gridline-color: #444;
        }
        QHeaderView::section {
            background-color: #2c2c2c;
            color: white;
            border: 1px solid #555;
        }
    """)

    main_window = MainWindow()
    main_window.show()
    sys.exit(app.exec())

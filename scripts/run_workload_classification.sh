#!/bin/bash

set -e

echo "Starting workload classification testing..."

# Инициализация базы данных
./init_db.sh

# Создаем классификатор
CLASSIFIER_SCRIPT="/scripts/workload_classifier.py"

# Сценарий 1: Чистый OLTP
echo "=== ТЕСТ 1: OLTP Классификация ==="
collect_metrics_before() {
    python3 $CLASSIFIER_SCRIPT > /results/classification_oltp_before.json
}

collect_metrics_after() {
    python3 $CLASSIFIER_SCRIPT > /results/classification_oltp_after.json
}

collect_metrics_before

# Запуск OLTP нагрузки
pgbench -h postgres -U postgres -d tpc_tests -i -s 20
pgbench -h postgres -U postgres -d tpc_tests -c 32 -j 8 -T 120 -l

collect_metrics_after

# Анализ перехода
python3 -c "
from workload_classifier import WorkloadClassifier
import json

classifier = WorkloadClassifier()

with open('/results/classification_oltp_before.json') as f:
    before = json.load(f)
with open('/results/classification_oltp_after.json') as f:
    after = json.load(f)

transition = classifier.analyze_workload_transition(
    before['metrics'],
    after['metrics']
)

print('OLTP Transition:', transition)
with open('/results/transition_oltp.json', 'w') as f:
    json.dump(transition, f, indent=2)
"

echo "=== ТЕСТ 2: Смешанная нагрузка ==="
# Запуск фоновой OLTP нагрузки
pgbench -h postgres -U postgres -d tpc_tests -c 16 -j 4 -T 300 -l &
PGBENCH_PID=$!

# Периодическая классификация во время нагрузки
for i in {1..6}; do
    echo "Измерение $i/6..."
    python3 $CLASSIFIER_SCRIPT > /results/classification_hybrid_${i}.json
    sleep 30
done

wait $PGBENCH_PID

echo "Классификация завершена!"

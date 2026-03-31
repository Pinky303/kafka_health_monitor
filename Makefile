.PHONY: up down topics seed producer archiver anomaly dashboard test lint clean lambda-zip

# ── Docker ────────────────────────────────────────────
up:
	docker-compose up -d
	@echo "✅ Kafka running. UI at http://localhost:8080"
	@echo "⏳ Waiting 15s for Kafka to be ready..."
	@sleep 15

down:
	docker-compose down -v
	@echo "🛑 Kafka stopped and volumes removed"

logs:
	docker-compose logs -f kafka

# ── Setup ─────────────────────────────────────────────
topics:
	python setup/create_topics.py

seed:
	python setup/seed_patients.py

install:
	pip install -r requirements.txt

# ── Run services (open 4 terminals) ──────────────────
dashboard:
	uvicorn consumers.dashboard_server:app --reload --port 8000

archiver:
	python consumers/s3_archiver.py

anomaly:
	python consumers/anomaly_detector.py

producer:
	python producer/patient_simulator.py

# ── Testing ───────────────────────────────────────────
test:
	pytest tests/ -v --tb=short

test-watch:
	pytest tests/ -v --tb=short -f

lint:
	ruff check . --ignore E501

# ── Lambda ────────────────────────────────────────────
lambda-zip:
	cd lambda && zip alert_handler.zip alert_handler.py
	@echo "✅ lambda/alert_handler.zip ready"

lambda-deploy:
	aws lambda update-function-code \
		--function-name patient-alert-handler \
		--zip-file fileb://lambda/alert_handler.zip

# ── Terraform ─────────────────────────────────────────
tf-init:
	cd infra/terraform && terraform init

tf-plan:
	cd infra/terraform && terraform plan

tf-apply:
	cd infra/terraform && terraform apply

tf-destroy:
	cd infra/terraform && terraform destroy

# ── Cleanup ───────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	rm -f lambda/alert_handler.zip

# ── Full local boot (run in one shot) ────────────────
start-all:
	@echo "🚀 Starting full system..."
	$(MAKE) up
	$(MAKE) topics
	@echo "Open 4 terminals and run:"
	@echo "  make dashboard"
	@echo "  make archiver"
	@echo "  make anomaly"
	@echo "  make producer"
	@echo "Then open: http://localhost:8000"

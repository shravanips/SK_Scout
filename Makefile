# Makefile — GitGub project shortcuts
.PHONY: help install dirs test test-cov test-ingest test-analytics test-utils \
        lint format clean clean-data clean-all \
        ingest-quick ingest-full analytics \
        run-sanity run-quick run-4h run-day run-week notebook

help:
	@echo ""
	@echo "  GitGub — available targets"
	@echo "  ──────────────────────────────────────────────────────────────────"
	@echo "  Setup"
	@echo "    install        Install all dependencies"
	@echo "    dirs           Create required data/logs directories"
	@echo ""
	@echo "  Testing"
	@echo "    test           Run full test suite"
	@echo "    test-cov       Run tests with HTML coverage report"
	@echo "    test-ingest    Run only ingest tests"
	@echo "    test-analytics Run only analytics tests"
	@echo "    test-utils     Run only utils tests"
	@echo ""
	@echo "  Code quality"
	@echo "    lint           Run ruff linter"
	@echo "    format         Auto-format with ruff"
	@echo ""
	@echo "  Pipeline"
	@echo "    run-sanity     1h window, 5k events, no API (fastest)"
	@echo "    run-quick      1h window, no API"
	@echo "    run-4h         4h window, with GitHub enrichment"
	@echo "    run-day        24h window (2026-04-16)"
	@echo "    run-week       7-day window (2026-04-10 to 2026-04-16)"
	@echo "    analytics      Re-run analytics on last ingest (flat layout)"
	@echo ""
	@echo "  Misc"
	@echo "    notebook       Launch Jupyter notebook"
	@echo "    clean          Remove __pycache__ and .pyc files"
	@echo "    clean-data     Remove generated data/reports"
	@echo "    clean-all      Full clean"
	@echo ""

# ── setup ─────────────────────────────────────────────────────────────────────
install:
	pip install -r requirements.txt

dirs:
	mkdir -p data/raw/gharchive data/processed data/runs data/reports logs
	@echo "✓ Directories created"

# ── testing ───────────────────────────────────────────────────────────────────
test:
	pytest tests/ -v

test-cov:
	pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html:data/reports/coverage

test-ingest:
	pytest tests/test_ingest.py -v

test-analytics:
	pytest tests/test_analytics.py -v

test-utils:
	pytest tests/test_utils.py -v

# ── code quality ──────────────────────────────────────────────────────────────
lint:
	ruff check src/ tests/

format:
	ruff format src/ tests/

# ── pipeline — Shravani's named run-tag style ─────────────────────────────────
START ?= 2026-04-16 12
END   ?= 2026-04-16 13

run-sanity:
	python run_pipeline.py \
		--run-tag sanity \
		--start "2026-04-16 12" --end "2026-04-16 13" \
		--max-events 5000 --no-enrich

run-quick:
	python run_pipeline.py \
		--run-tag quick \
		--start "$(START)" --end "$(END)" \
		--no-enrich

run-4h:
	python run_pipeline.py \
		--run-tag fourhour \
		--start "2026-04-16 00" --end "2026-04-16 04"

run-day:
	python run_pipeline.py \
		--run-tag full24h \
		--start "2026-04-16 00" --end "2026-04-16 23" \
		--max-enrich 500

run-week:
	python run_pipeline.py \
		--run-tag run_week \
		--start "2026-04-10 00" --end "2026-04-16 23" \
		--max-enrich 500

# standalone steps (flat layout, for development)
ingest-quick:
	python src/ingest.py --start "$(START)" --end "$(END)" --max-events 5000

ingest-full:
	python src/ingest.py --start "2026-04-16 00" --end "2026-04-16 04"

analytics:
	python src/analytics.py --no-enrich

# ── notebook ──────────────────────────────────────────────────────────────────
notebook:
	jupyter notebook notebooks/exploration.ipynb

# ── clean ─────────────────────────────────────────────────────────────────────
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
	@echo "✓ Cache cleaned"

clean-data:
	rm -rf data/raw/gharchive/* data/processed/* data/runs/* data/reports/*
	@echo "✓ Data cleaned"

clean-all: clean clean-data
	rm -rf logs/*.log
	@echo "✓ Full clean"

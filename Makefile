# Makefile for PySpark Assignment
# Week 11 - NYC Taxi Data Processing

.PHONY: help install setup download-data run export clean check test all

# Python and Jupyter settings
PYTHON := python3
PIP := pip3
JUPYTER := jupyter
NOTEBOOK := pyspark_assignment.ipynb
HTML_OUTPUT := pyspark_assignment.html

# Directories
DATA_DIR := ./data
OUTPUT_DIR := ./output
SCREENSHOTS_DIR := ./screenshots

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m # No Color

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "$(BLUE)========================================$(NC)"
	@echo "$(BLUE)PySpark Assignment - Makefile Commands$(NC)"
	@echo "$(BLUE)========================================$(NC)"
	@echo ""
	@echo "$(GREEN)Setup Commands:$(NC)"
	@echo "  make install        - Install Python dependencies"
	@echo "  make setup          - Full setup (install + create directories)"
	@echo "  make download-data  - Download NYC Taxi dataset (~3GB)"
	@echo ""
	@echo "$(GREEN)Run Commands:$(NC)"
	@echo "  make run            - Start Jupyter notebook"
	@echo "  make export         - Export notebook to HTML"
	@echo "  make execute        - Execute notebook non-interactively"
	@echo ""
	@echo "$(GREEN)Utility Commands:$(NC)"
	@echo "  make check          - Check prerequisites (Python, Java)"
	@echo "  make test           - Quick test of PySpark installation"
	@echo "  make clean          - Remove output files (keep data)"
	@echo "  make clean-all      - Remove everything (including data)"
	@echo "  make spark-ui       - Open Spark UI in browser"
	@echo ""
	@echo "$(GREEN)Submission Commands:$(NC)"
	@echo "  make prepare-submit - Prepare files for submission"
	@echo "  make package        - Create submission zip file"
	@echo ""
	@echo "$(GREEN)Common Workflows:$(NC)"
	@echo "  make all            - Setup + download data + run notebook"
	@echo "  make quick-start    - Setup + run (assumes data exists)"
	@echo ""
	@echo "$(YELLOW)Note: Run 'make check' first to verify prerequisites$(NC)"
	@echo ""

check: ## Check if prerequisites are installed
	@echo "$(BLUE)Checking prerequisites...$(NC)"
	@echo ""
	@echo -n "Python 3: "
	@if command -v $(PYTHON) >/dev/null 2>&1; then \
		echo "$(GREEN)✓ $(shell $(PYTHON) --version)$(NC)"; \
	else \
		echo "$(RED)✗ Not found$(NC)"; \
		exit 1; \
	fi
	@echo -n "Java: "
	@if command -v java >/dev/null 2>&1; then \
		echo "$(GREEN)✓ $(shell java -version 2>&1 | head -n 1)$(NC)"; \
	else \
		echo "$(YELLOW)⚠ Not found (required for PySpark)$(NC)"; \
	fi
	@echo -n "Jupyter: "
	@if command -v $(JUPYTER) >/dev/null 2>&1; then \
		echo "$(GREEN)✓ Installed$(NC)"; \
	else \
		echo "$(YELLOW)⚠ Not installed (will be installed)$(NC)"; \
	fi
	@echo ""
	@echo "$(GREEN)Prerequisites check complete!$(NC)"

install: ## Install Python dependencies
	@echo "$(BLUE)Installing Python dependencies...$(NC)"
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "$(GREEN)✓ Dependencies installed successfully!$(NC)"

setup: check install ## Complete setup (check, install, create directories)
	@echo "$(BLUE)Creating directory structure...$(NC)"
	@mkdir -p $(DATA_DIR) $(OUTPUT_DIR) $(SCREENSHOTS_DIR)
	@touch $(DATA_DIR)/.gitkeep $(OUTPUT_DIR)/.gitkeep $(SCREENSHOTS_DIR)/.gitkeep
	@echo "$(GREEN)✓ Directory structure created!$(NC)"
	@echo ""
	@echo "$(GREEN)========================================$(NC)"
	@echo "$(GREEN)Setup complete!$(NC)"
	@echo "$(GREEN)========================================$(NC)"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Download data: $(YELLOW)make download-data$(NC)"
	@echo "  2. Run notebook:  $(YELLOW)make run$(NC)"
	@echo ""

download-data: ## Download NYC Taxi dataset
	@echo "$(BLUE)Starting data download...$(NC)"
	@echo "$(YELLOW)This will download ~3GB of data. Continue? [y/N]$(NC)"
	@read -r response; \
	if [ "$$response" = "y" ] || [ "$$response" = "Y" ]; then \
		$(PYTHON) download_data.py; \
		echo "$(GREEN)✓ Data download complete!$(NC)"; \
	else \
		echo "$(YELLOW)Download cancelled.$(NC)"; \
	fi

run: ## Start Jupyter notebook
	@echo "$(BLUE)Starting Jupyter Notebook...$(NC)"
	@echo ""
	@echo "$(YELLOW)Notebook will open in your browser$(NC)"
	@echo "$(YELLOW)Spark UI will be available at: http://localhost:4040$(NC)"
	@echo ""
	@echo "Press Ctrl+C to stop the server"
	@echo ""
	$(JUPYTER) notebook $(NOTEBOOK)

execute: ## Execute notebook non-interactively
	@echo "$(BLUE)Executing notebook...$(NC)"
	@$(JUPYTER) nbconvert --to notebook --execute --inplace $(NOTEBOOK)
	@echo "$(GREEN)✓ Notebook executed successfully!$(NC)"

export: ## Export notebook to HTML
	@echo "$(BLUE)Exporting notebook to HTML...$(NC)"
	@$(JUPYTER) nbconvert --to html $(NOTEBOOK)
	@if [ -f $(HTML_OUTPUT) ]; then \
		echo "$(GREEN)✓ HTML file created: $(HTML_OUTPUT)$(NC)"; \
	else \
		echo "$(RED)✗ Failed to create HTML file$(NC)"; \
		exit 1; \
	fi

test: ## Quick test of PySpark installation
	@echo "$(BLUE)Testing PySpark installation...$(NC)"
	@$(PYTHON) -c "from pyspark.sql import SparkSession; \
		spark = SparkSession.builder.appName('Test').getOrCreate(); \
		print('✓ PySpark is working!'); \
		print(f'Spark version: {spark.version}'); \
		spark.stop()" && echo "$(GREEN)✓ Test passed!$(NC)" || echo "$(RED)✗ Test failed!$(NC)"

spark-ui: ## Open Spark UI in browser
	@echo "$(BLUE)Opening Spark UI...$(NC)"
	@echo "$(YELLOW)Note: Spark must be running for this to work$(NC)"
	@if command -v xdg-open >/dev/null 2>&1; then \
		xdg-open http://localhost:4040; \
	elif command -v open >/dev/null 2>&1; then \
		open http://localhost:4040; \
	else \
		echo "$(YELLOW)Please open http://localhost:4040 in your browser$(NC)"; \
	fi

clean: ## Remove output files (keep data)
	@echo "$(BLUE)Cleaning output files...$(NC)"
	@rm -rf $(OUTPUT_DIR)/*
	@rm -f $(HTML_OUTPUT)
	@rm -rf .ipynb_checkpoints
	@rm -rf __pycache__
	@rm -f derby.log
	@rm -rf metastore_db
	@rm -rf spark-warehouse
	@mkdir -p $(OUTPUT_DIR)
	@touch $(OUTPUT_DIR)/.gitkeep
	@echo "$(GREEN)✓ Output files cleaned!$(NC)"

clean-all: clean ## Remove everything including downloaded data
	@echo "$(RED)This will delete ALL data (~3GB). Continue? [y/N]$(NC)"
	@read -r response; \
	if [ "$$response" = "y" ] || [ "$$response" = "Y" ]; then \
		rm -rf $(DATA_DIR)/*.parquet; \
		echo "$(GREEN)✓ All data removed!$(NC)"; \
	else \
		echo "$(YELLOW)Cancelled.$(NC)"; \
	fi

prepare-submit: export ## Prepare files for submission
	@echo "$(BLUE)Preparing submission files...$(NC)"
	@echo ""
	@echo "Checking required files:"
	@echo -n "  $(NOTEBOOK): "
	@if [ -f $(NOTEBOOK) ]; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi
	@echo -n "  $(HTML_OUTPUT): "
	@if [ -f $(HTML_OUTPUT) ]; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi
	@echo -n "  README.md: "
	@if [ -f README.md ]; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi
	@echo -n "  Screenshots: "
	@if [ "$$(ls -A $(SCREENSHOTS_DIR) 2>/dev/null | grep -v .gitkeep)" ]; then \
		echo "$(GREEN)✓$(NC)"; \
	else \
		echo "$(YELLOW)⚠ No screenshots found$(NC)"; \
	fi
	@echo ""
	@echo "$(YELLOW)Don't forget to add screenshots before submitting!$(NC)"

package: prepare-submit ## Create submission zip file
	@echo "$(BLUE)Creating submission package...$(NC)"
	@TIMESTAMP=$$(date +%Y%m%d_%H%M%S); \
	ZIP_FILE="pyspark_assignment_$$TIMESTAMP.zip"; \
	zip -r $$ZIP_FILE \
		$(NOTEBOOK) \
		$(HTML_OUTPUT) \
		README.md \
		requirements.txt \
		$(SCREENSHOTS_DIR) \
		-x "*.gitkeep" "*.DS_Store"; \
	echo "$(GREEN)✓ Package created: $$ZIP_FILE$(NC)"

all: setup download-data run ## Complete workflow: setup, download, and run

quick-start: setup run ## Quick start (assumes data exists)

# Development helpers
lint: ## Check notebook for common issues
	@echo "$(BLUE)Checking notebook...$(NC)"
	@$(JUPYTER) nbconvert --to notebook --execute --stdout $(NOTEBOOK) > /dev/null 2>&1 && \
		echo "$(GREEN)✓ Notebook is valid$(NC)" || \
		echo "$(RED)✗ Notebook has issues$(NC)"

info: ## Show project information
	@echo "$(BLUE)========================================$(NC)"
	@echo "$(BLUE)Project Information$(NC)"
	@echo "$(BLUE)========================================$(NC)"
	@echo ""
	@echo "Project: PySpark Data Processing Assignment"
	@echo "Dataset: NYC Yellow Taxi Trip Data"
	@echo ""
	@echo "Directories:"
	@echo "  Data:        $(DATA_DIR)"
	@echo "  Output:      $(OUTPUT_DIR)"
	@echo "  Screenshots: $(SCREENSHOTS_DIR)"
	@echo ""
	@echo "Files:"
	@echo "  Notebook: $(NOTEBOOK)"
	@echo "  HTML:     $(HTML_OUTPUT)"
	@echo ""
	@echo "Status:"
	@if [ -d $(DATA_DIR) ] && [ "$$(ls -A $(DATA_DIR)/*.parquet 2>/dev/null)" ]; then \
		echo "  Data: $(GREEN)Downloaded$(NC)"; \
		ls -lh $(DATA_DIR)/*.parquet 2>/dev/null | awk '{printf "    - %s (%s)\n", $$9, $$5}'; \
	else \
		echo "  Data: $(YELLOW)Not downloaded$(NC)"; \
	fi
	@if [ -f $(HTML_OUTPUT) ]; then \
		echo "  HTML Export: $(GREEN)Ready$(NC)"; \
	else \
		echo "  HTML Export: $(YELLOW)Not created$(NC)"; \
	fi
	@echo ""

# Virtual environment targets
venv: ## Create Python virtual environment
	@echo "$(BLUE)Creating virtual environment...$(NC)"
	@$(PYTHON) -m venv venv
	@echo "$(GREEN)✓ Virtual environment created!$(NC)"
	@echo ""
	@echo "Activate with:"
	@echo "  $(YELLOW)source venv/bin/activate$(NC)  # Linux/macOS"
	@echo "  $(YELLOW)venv\\Scripts\\activate$(NC)     # Windows"

venv-install: venv ## Create venv and install dependencies
	@echo "$(BLUE)Installing dependencies in virtual environment...$(NC)"
	@. venv/bin/activate && $(PIP) install --upgrade pip
	@. venv/bin/activate && $(PIP) install -r requirements.txt
	@echo "$(GREEN)✓ Virtual environment ready!$(NC)"

# Advanced targets
profile: ## Run notebook with profiling
	@echo "$(BLUE)Running notebook with profiling...$(NC)"
	@$(PYTHON) -m cProfile -o profile.stats -c "import subprocess; subprocess.run(['$(JUPYTER)', 'nbconvert', '--to', 'notebook', '--execute', '--inplace', '$(NOTEBOOK)'])"
	@echo "$(GREEN)✓ Profile saved to profile.stats$(NC)"

benchmark: ## Quick performance benchmark
	@echo "$(BLUE)Running PySpark benchmark...$(NC)"
	@$(PYTHON) -c "import time; from pyspark.sql import SparkSession; \
		start = time.time(); \
		spark = SparkSession.builder.appName('Benchmark').getOrCreate(); \
		df = spark.range(10000000); \
		count = df.count(); \
		elapsed = time.time() - start; \
		print(f'Processed {count:,} records in {elapsed:.2f}s'); \
		spark.stop()"

# GitHub integration
init-repo: ## Initialize git repository
	@echo "$(BLUE)Initializing git repository...$(NC)"
	@if [ ! -d .git ]; then \
		git init; \
		echo "$(GREEN)✓ Git repository initialized$(NC)"; \
	else \
		echo "$(YELLOW)Repository already initialized$(NC)"; \
	fi
	@git add .gitignore README.md requirements.txt Makefile
	@echo "$(GREEN)✓ Initial files staged$(NC)"

# Documentation
docs: ## Open documentation in browser
	@echo "$(BLUE)Opening documentation...$(NC)"
	@if command -v xdg-open >/dev/null 2>&1; then \
		xdg-open README.md; \
	elif command -v open >/dev/null 2>&1; then \
		open README.md; \
	else \
		cat README.md; \
	fi

# Installation verification
verify: ## Verify installation is complete
	@echo "$(BLUE)Verifying installation...$(NC)"
	@echo ""
	@errors=0; \
	echo -n "Python 3: "; \
	if command -v $(PYTHON) >/dev/null 2>&1; then \
		echo "$(GREEN)✓$(NC)"; \
	else \
		echo "$(RED)✗$(NC)"; \
		errors=$$((errors + 1)); \
	fi; \
	echo -n "Java: "; \
	if command -v java >/dev/null 2>&1; then \
		echo "$(GREEN)✓$(NC)"; \
	else \
		echo "$(RED)✗$(NC)"; \
		errors=$$((errors + 1)); \
	fi; \
	echo -n "PySpark: "; \
	if $(PYTHON) -c "import pyspark" 2>/dev/null; then \
		echo "$(GREEN)✓$(NC)"; \
	else \
		echo "$(RED)✗$(NC)"; \
		errors=$$((errors + 1)); \
	fi; \
	echo -n "Jupyter: "; \
	if command -v $(JUPYTER) >/dev/null 2>&1; then \
		echo "$(GREEN)✓$(NC)"; \
	else \
		echo "$(RED)✗$(NC)"; \
		errors=$$((errors + 1)); \
	fi; \
	echo -n "Data downloaded: "; \
	if [ -d $(DATA_DIR) ] && [ "$$(ls -A $(DATA_DIR)/*.parquet 2>/dev/null)" ]; then \
		echo "$(GREEN)✓$(NC)"; \
	else \
		echo "$(YELLOW)⚠$(NC)"; \
	fi; \
	echo ""; \
	if [ $$errors -eq 0 ]; then \
		echo "$(GREEN)✓ All checks passed!$(NC)"; \
	else \
		echo "$(RED)✗ $$errors check(s) failed$(NC)"; \
		exit 1; \
	fi
# Milvus Spark Connector Makefile
# Author: Zilliz
# Description: Simplified build system using milvus-storage JNI library

# Configuration
SCALA_VERSION := 2.13
SBT := sbt
JAVA_HOME ?= $(shell dirname $(shell dirname $(shell readlink -f $(shell which java))))

# Directories
RESOURCES_DIR := src/main/resources
MILVUS_STORAGE_CPP := milvus-storage/cpp
MILVUS_STORAGE_BUILD := $(MILVUS_STORAGE_CPP)/build/Release
TARGET_DIR := target

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
NC := \033[0m # No Color

# All phony targets
.PHONY: all help check-deps init-submodules build-milvus-storage copy-native-libs clean package test run-demo rebuild quick-build status

# Default target
all: clean build-milvus-storage copy-native-libs package

# Help target
help:
	@echo "$(BLUE)Milvus Spark Connector Build System$(NC)"
	@echo ""
	@echo "$(YELLOW)Available targets:$(NC)"
	@echo "  $(GREEN)all$(NC)                    - Complete build process"
	@echo "  $(GREEN)clean$(NC)                  - Clean all build artifacts"
	@echo "  $(GREEN)build-milvus-storage$(NC)   - Build milvus-storage with JNI support"
	@echo "  $(GREEN)copy-native-libs$(NC)       - Copy native libraries to resources"
	@echo "  $(GREEN)package$(NC)                - Package JAR with native libraries"
	@echo "  $(GREEN)test$(NC)                   - Run tests"
	@echo "  $(GREEN)run-demo$(NC)               - Run demo"
	@echo "  $(GREEN)init-submodules$(NC)        - Initialize Git submodules"
	@echo "  $(GREEN)check-deps$(NC)             - Check system dependencies"
	@echo ""
	@echo "$(YELLOW)Environment variables:$(NC)"
	@echo "  JAVA_HOME=$(JAVA_HOME)"
	@echo "  SCALA_VERSION=$(SCALA_VERSION)"

# Check system dependencies
check-deps:
	@echo "$(BLUE)Checking system dependencies...$(NC)"
	@command -v java >/dev/null 2>&1 || { echo "$(RED)Error: Java not found$(NC)"; exit 1; }
	@command -v javac >/dev/null 2>&1 || { echo "$(RED)Error: javac not found$(NC)"; exit 1; }
	@command -v $(SBT) >/dev/null 2>&1 || { echo "$(RED)Error: sbt not found$(NC)"; exit 1; }
	@command -v make >/dev/null 2>&1 || { echo "$(RED)Error: make not found$(NC)"; exit 1; }
	@command -v conan >/dev/null 2>&1 || { echo "$(RED)Error: conan not found$(NC)"; exit 1; }
	@echo "$(GREEN)All dependencies found$(NC)"

# Initialize Git submodules
init-submodules:
	@echo "$(BLUE)Initializing Git submodules...$(NC)"
	@git submodule update --init --recursive
	@echo "$(GREEN)Submodules initialized$(NC)"

# Build milvus-storage with JNI support
build-milvus-storage: check-deps init-submodules
	@echo "$(BLUE)Building milvus-storage with JNI support...$(NC)"
	@if [ ! -d "$(MILVUS_STORAGE_CPP)" ]; then \
		echo "$(RED)Error: milvus-storage/cpp directory not found$(NC)"; \
		echo "$(YELLOW)Run 'make init-submodules' first$(NC)"; \
		exit 1; \
	fi
	@cd $(MILVUS_STORAGE_CPP) && make build USE_JNI=True
	@if [ -f "$(MILVUS_STORAGE_BUILD)/libmilvus-storage.so" ] && [ -f "$(MILVUS_STORAGE_BUILD)/libmilvus-storage-jni.so" ]; then \
		echo "$(GREEN)✓ Successfully built milvus-storage with JNI$(NC)"; \
		ls -lh $(MILVUS_STORAGE_BUILD)/*.so; \
	else \
		echo "$(RED)Error: Failed to build milvus-storage JNI libraries$(NC)"; \
		exit 1; \
	fi

# Copy native libraries to resources directory
copy-native-libs: $(RESOURCES_DIR)/native
	@echo "$(BLUE)Copying native libraries to resources directory...$(NC)"
	@if [ ! -f "$(MILVUS_STORAGE_BUILD)/libmilvus-storage.so" ] || [ ! -f "$(MILVUS_STORAGE_BUILD)/libmilvus-storage-jni.so" ]; then \
		echo "$(YELLOW)Native libraries not found, building first...$(NC)"; \
		$(MAKE) build-milvus-storage; \
	fi
	@cp $(MILVUS_STORAGE_BUILD)/libmilvus-storage.so $(RESOURCES_DIR)/native/
	@cp $(MILVUS_STORAGE_BUILD)/libmilvus-storage-jni.so $(RESOURCES_DIR)/native/
	@echo "$(GREEN)✓ Copied native libraries to resources$(NC)"
	@ls -lh $(RESOURCES_DIR)/native/*.so

# Create necessary directories
$(RESOURCES_DIR)/native:
	@mkdir -p $(RESOURCES_DIR)/native

# Clean build artifacts
clean:
	@echo "$(BLUE)Cleaning build artifacts...$(NC)"
	@rm -rf $(TARGET_DIR)
	@rm -rf project/target
	@rm -rf project/project
	@echo "$(YELLOW)Cleaning native libraries from resources...$(NC)"
	@rm -f $(RESOURCES_DIR)/native/libmilvus-storage-jni.so
	@rm -f $(RESOURCES_DIR)/native/libmilvus-storage.so
	@$(SBT) clean
	@echo "$(GREEN)Clean complete$(NC)"

# Clean everything including milvus-storage build
clean-all: clean
	@echo "$(BLUE)Cleaning milvus-storage build...$(NC)"
	@if [ -d "$(MILVUS_STORAGE_CPP)" ]; then \
		cd $(MILVUS_STORAGE_CPP) && make clean; \
	fi
	@echo "$(GREEN)Clean all complete$(NC)"

# Package JAR with native libraries
package: copy-native-libs
	@echo "$(BLUE)Packaging JAR with native libraries...$(NC)"
	@$(SBT) package
	@echo "$(GREEN)Packaging complete$(NC)"

# Run tests
test: package
	@echo "$(BLUE)Running tests...$(NC)"
	@$(SBT) test
	@echo "$(GREEN)Tests complete$(NC)"

# Run demo
run-demo: package
	@echo "$(BLUE)Running demo...$(NC)"
	@$(SBT) "runMain com.zilliz.spark.connector.jni.MilvusStorageJNI"

# Development targets
rebuild: clean-all all

quick-build: copy-native-libs package

# Show build status
status:
	@echo "$(BLUE)Build Status:$(NC)"
	@echo -n "Milvus Storage SO: "
	@if [ -f "$(MILVUS_STORAGE_BUILD)/libmilvus-storage.so" ]; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi
	@echo -n "Milvus Storage JNI SO: "
	@if [ -f "$(MILVUS_STORAGE_BUILD)/libmilvus-storage-jni.so" ]; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi
	@echo -n "Native libs in resources: "
	@if [ -f "$(RESOURCES_DIR)/native/libmilvus-storage.so" ] && [ -f "$(RESOURCES_DIR)/native/libmilvus-storage-jni.so" ]; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi
	@echo -n "JAR package: "
	@if ls $(TARGET_DIR)/scala-$(SCALA_VERSION)/*.jar 1> /dev/null 2>&1; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi

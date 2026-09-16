# Milvus Spark Connector Makefile
# Author: Zilliz
# Description: Simplified build system using milvus-storage JNI library

# Configuration
SCALA_VERSION := 2.13
SBT := sbt
JAVA_HOME ?= $(shell dirname $(shell dirname $(shell readlink -f $(shell which java))))

# Directories
RESOURCES_DIR := native-storage/src/main/resources
MILVUS_STORAGE_CPP := milvus-storage/cpp
TARGET_DIR := target

# CMake writes the engine and JNI libraries to Release on both platforms.
# Conan collects their shared dependencies, including provider directories,
# under Release/libs. NativeLibraryLoader selects native/<platform>/ resources.
MILVUS_STORAGE_BUILD := $(MILVUS_STORAGE_CPP)/build/Release
MILVUS_STORAGE_DEPS := $(MILVUS_STORAGE_BUILD)/libs
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)
ifeq ($(UNAME_S),Darwin)
  LIB_SUFFIX := dylib
  NATIVE_PLATFORM := darwin-$(if $(filter arm64 aarch64,$(UNAME_M)),aarch64,x86_64)
else
  LIB_SUFFIX := so
  NATIVE_PLATFORM := linux-$(if $(filter arm64 aarch64,$(UNAME_M)),aarch64,x86_64)
endif
NATIVE_DIR := $(RESOURCES_DIR)/native/$(NATIVE_PLATFORM)
STORAGE_LIB := $(MILVUS_STORAGE_BUILD)/libmilvus-storage.$(LIB_SUFFIX)
STORAGE_JNI_LIB := $(MILVUS_STORAGE_BUILD)/libmilvus-storage-jni.$(LIB_SUFFIX)

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
NC := \033[0m # No Color

# All phony targets
.PHONY: all help check-deps init-submodules build-milvus-storage copy-native-libs clean package test compile-it run-demo rebuild quick-build status

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
	@echo "  $(GREEN)test$(NC)                   - Type-check tests (incl. integration) and run unit tests"
	@echo "  $(GREEN)compile-it$(NC)             - Type-check integration tests (no external services)"
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
	@command -v cmake >/dev/null 2>&1 || { echo "$(RED)Error: cmake not found$(NC)"; exit 1; }
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
	@cd $(MILVUS_STORAGE_CPP) && make java-lib
	@if [ -f "$(STORAGE_LIB)" ] && [ -f "$(STORAGE_JNI_LIB)" ]; then \
		echo "$(GREEN)✓ Successfully built milvus-storage with JNI$(NC)"; \
		ls -lh $(STORAGE_LIB) $(STORAGE_JNI_LIB); \
	else \
		echo "$(RED)Error: Failed to build milvus-storage JNI libraries$(NC)"; \
		echo "$(YELLOW)Looked for $(STORAGE_LIB) and $(STORAGE_JNI_LIB)$(NC)"; \
		exit 1; \
	fi

# Copy native libraries to resources directory
copy-native-libs: $(NATIVE_DIR)
	@echo "$(BLUE)Copying native libraries to $(NATIVE_DIR)...$(NC)"
	@if [ ! -f "$(STORAGE_LIB)" ] || [ ! -f "$(STORAGE_JNI_LIB)" ]; then \
		echo "$(YELLOW)Native libraries not found, building first...$(NC)"; \
		$(MAKE) build-milvus-storage; \
	fi
	@rm -f "$(NATIVE_DIR)/libnative-storage-jni.$(LIB_SUFFIX)"
	@cp -L "$(STORAGE_LIB)" "$(STORAGE_JNI_LIB)" "$(NATIVE_DIR)/"
	@set -e; \
	test -d "$(MILVUS_STORAGE_DEPS)"; \
	for library in "$(MILVUS_STORAGE_DEPS)"/*.$(LIB_SUFFIX)*; do \
		[ -f "$$library" ] || continue; \
		cp -L "$$library" "$(NATIVE_DIR)/"; \
	done; \
	for subdir in ossl-modules engines-3; do \
		if [ -d "$(MILVUS_STORAGE_DEPS)/$$subdir" ]; then \
			mkdir -p "$(NATIVE_DIR)/$$subdir"; \
			cp -RL "$(MILVUS_STORAGE_DEPS)/$$subdir/." "$(NATIVE_DIR)/$$subdir/"; \
		fi; \
	done
	@if [ "$(UNAME_S)" != Darwin ]; then \
		bash "$(MILVUS_STORAGE_CPP)/../java/patch_native_runpath.sh" "$(NATIVE_DIR)"; \
	fi
	@if [ ! -f "$(NATIVE_DIR)/libmilvus-storage-jni.$(LIB_SUFFIX)" ]; then \
		echo "$(RED)Error: libmilvus-storage-jni did not reach $(NATIVE_DIR)$(NC)"; \
		exit 1; \
	fi
	@echo "$(GREEN)✓ Copied native libraries to resources$(NC)"
	@ls -lh $(NATIVE_DIR)/ | head -20

# Create necessary directories
$(NATIVE_DIR):
	@mkdir -p $(NATIVE_DIR)

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
	@echo "$(BLUE)Type-checking integration tests...$(NC)"
	@$(SBT) "integration40/Test/compile"
	@echo "$(BLUE)Running unit tests...$(NC)"
	@$(SBT) test
	@echo "$(GREEN)Tests complete$(NC)"

# Type-check integration tests only (no Milvus/MinIO required at compile time)
compile-it: package
	@echo "$(BLUE)Type-checking integration tests...$(NC)"
	@$(SBT) "integration40/Test/compile"
	@echo "$(GREEN)Integration tests compile complete$(NC)"

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
	@echo "Platform: $(NATIVE_PLATFORM) (.$(LIB_SUFFIX))"
	@echo -n "Milvus Storage lib: "
	@if [ -f "$(STORAGE_LIB)" ]; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi
	@echo -n "Milvus Storage JNI lib: "
	@if [ -f "$(STORAGE_JNI_LIB)" ]; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi
	@echo -n "Native libs in resources: "
	@if [ -f "$(NATIVE_DIR)/libmilvus-storage.$(LIB_SUFFIX)" ] && [ -f "$(NATIVE_DIR)/libmilvus-storage-jni.$(LIB_SUFFIX)" ]; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi
	@echo -n "JAR package: "
	@if ls $(TARGET_DIR)/scala-$(SCALA_VERSION)/*.jar 1> /dev/null 2>&1; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi

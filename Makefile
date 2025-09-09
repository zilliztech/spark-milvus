# Milvus Storage JNI Demo Makefile
# Author: Zilliz
# Description: Build system for Milvus Storage JNI wrapper

# Configuration
SCALA_VERSION := 2.13
SBT := sbt
JAVA_HOME ?= $(shell dirname $(shell dirname $(shell readlink -f $(shell which java))))
CMAKE := cmake
MAKE := make

# Directories
SRC_DIR := src/main
SCALA_SRC := $(SRC_DIR)/scala
C_SRC := $(SRC_DIR)/c
BUILD_DIR := build
C_BUILD_DIR := $(BUILD_DIR)/c
TARGET_DIR := target
SCRIPT_DIR := scripts
RESOURCES_DIR := $(SRC_DIR)/resources

# Generated files
JNI_HEADER := $(C_SRC)/include/com_zilliz_spark_connector_jni_MilvusStorageJNI.h
SHARED_LIB := $(C_BUILD_DIR)/libmilvus_storage_jni.so
JAR_FILE := $(TARGET_DIR)/scala-$(SCALA_VERSION)/milvus-spark-connector_$(SCALA_VERSION)-0.1.0-SNAPSHOT.jar

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
NC := \033[0m # No Color

# All phony targets
.PHONY: all help check-deps init-submodules setup-milvus fix-cmake clean compile-scala generate-headers compile-native package test run-demo install dev-setup rebuild quick-build debug-info status

# Default target
all: clean compile-scala generate-headers compile-native package

# Help target
help:
	@echo "$(BLUE)Milvus Storage JNI Demo Build System$(NC)"
	@echo ""
	@echo "$(YELLOW)Available targets:$(NC)"
	@echo "  $(GREEN)all$(NC)                 - Complete build process"
	@echo "  $(GREEN)clean$(NC)               - Clean all build artifacts"
	@echo "  $(GREEN)compile-scala$(NC)       - Compile Scala classes"
	@echo "  $(GREEN)generate-headers$(NC)    - Generate JNI headers"
	@echo "  $(GREEN)compile-native$(NC)      - Compile C++ native library"
	@echo "  $(GREEN)package$(NC)             - Package JAR with native library"
	@echo "  $(GREEN)test$(NC)                - Run tests"
	@echo "  $(GREEN)run-demo$(NC)            - Run demo using sbt custom task (recommended)"
	@echo "  $(GREEN)run-demo-java$(NC)       - Run demo using direct Java command"
	@echo "  $(GREEN)init-submodules$(NC)     - Initialize Git submodules"
	@echo "  $(GREEN)setup-milvus$(NC)        - Setup Milvus Storage dependency"
	@echo "  $(GREEN)check-deps$(NC)          - Check system dependencies"
	@echo "  $(GREEN)fix-cmake$(NC)           - Fix CMake configuration for libmilvus-storage.so"
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
	@command -v $(CMAKE) >/dev/null 2>&1 || { echo "$(RED)Error: cmake not found$(NC)"; exit 1; }
	@command -v $(MAKE) >/dev/null 2>&1 || { echo "$(RED)Error: make not found$(NC)"; exit 1; }
	@command -v pkg-config >/dev/null 2>&1 || { echo "$(RED)Error: pkg-config not found$(NC)"; exit 1; }
	@echo "$(GREEN)All dependencies found$(NC)"

# Initialize Git submodules
init-submodules:
	@echo "$(BLUE)Initializing Git submodules...$(NC)"
	@git submodule update --init --recursive
	@echo "$(GREEN)Submodules initialized$(NC)"

# Setup Milvus Storage
setup-milvus: init-submodules
	@echo "$(BLUE)Setting up Milvus Storage...$(NC)"
	@if [ ! -f "milvus-storage/cpp/build/src/libmilvus-storage.a" ]; then \
		echo "$(YELLOW)Milvus Storage not built, building now...$(NC)"; \
		cd milvus-storage/cpp && make build; \
	fi
	@echo "$(GREEN)Milvus Storage setup complete$(NC)"

# Fix CMake configuration for libmilvus-storage.so
fix-cmake:
	@echo "$(BLUE)Fixing CMake configuration for libmilvus-storage.so...$(NC)"
	@echo "$(YELLOW)This fixes the error: 'No rule to make target /usr/local/lib/libmilvus-storage.so'$(NC)"
	@if [ ! -f "CMakeLists.txt" ]; then \
		echo "$(RED)Error: CMakeLists.txt not found$(NC)"; \
		exit 1; \
	fi
	@if grep -q "NO_DEFAULT_PATH" CMakeLists.txt; then \
		echo "$(GREEN)✓ CMakeLists.txt already contains NO_DEFAULT_PATH fix$(NC)"; \
	else \
		echo "$(YELLOW)Applying NO_DEFAULT_PATH fix to find_library command...$(NC)"; \
		if grep -q "PATHS.*milvus-storage.*Release" CMakeLists.txt; then \
			sed -i 's|PATHS $${CMAKE_CURRENT_SOURCE_DIR}/milvus-storage/cpp/build/Release|PATHS $${CMAKE_CURRENT_SOURCE_DIR}/milvus-storage/cpp/build/Release\n    NO_DEFAULT_PATH|' CMakeLists.txt; \
			echo "$(GREEN)✓ CMakeLists.txt has been fixed$(NC)"; \
		else \
			echo "$(RED)Error: Expected find_library pattern not found in CMakeLists.txt$(NC)"; \
			exit 1; \
		fi; \
	fi
	@echo "$(YELLOW)Cleaning CMake cache to ensure fix takes effect...$(NC)"
	@rm -rf $(C_BUILD_DIR)/CMakeCache.txt $(C_BUILD_DIR)/CMakeFiles build/Release/CMakeCache.txt build/Release/CMakeFiles
	@echo "$(GREEN)✓ CMake configuration fix complete$(NC)"
	@echo ""
	@echo "$(BLUE)What this fix does:$(NC)"
	@echo "  • Adds NO_DEFAULT_PATH to find_library(MILVUS_STORAGE_LIB)"
	@echo "  • Forces CMake to only search in the specified project path"
	@echo "  • Prevents searching in system paths like /usr/local/lib/"
	@echo "  • Ensures the project uses its own built libmilvus-storage.so"
	@echo ""
	@echo "$(BLUE)Next steps:$(NC)"
	@echo "  • Run 'make compile-native' to test the fix"
	@echo "  • Run 'make all' for complete build process"

# Clean build artifacts
clean:
	@echo "$(BLUE)Cleaning build artifacts...$(NC)"
	@rm -rf $(BUILD_DIR)
	@rm -rf $(TARGET_DIR)
	@rm -rf project/target
	@rm -rf project/project
	@rm -f $(JNI_HEADER)
	@echo "$(YELLOW)Cleaning native libraries from resources...$(NC)"
	@rm -f $(RESOURCES_DIR)/native/libmilvus_storage_jni.so*
	@rm -f $(RESOURCES_DIR)/native/libmilvus-storage.so
	@$(SBT) clean
	@echo "$(GREEN)Clean complete$(NC)"

# Create necessary directories
$(BUILD_DIR):
	@mkdir -p $(BUILD_DIR)

$(C_BUILD_DIR): $(BUILD_DIR)
	@mkdir -p $(C_BUILD_DIR)

$(C_SRC)/include:
	@mkdir -p $(C_SRC)/include

$(RESOURCES_DIR)/native:
	@mkdir -p $(RESOURCES_DIR)/native

# Compile Scala classes
compile-scala: check-deps
	@echo "$(BLUE)Compiling Scala classes...$(NC)"
	@$(SBT) compile
	@echo "$(GREEN)Scala compilation complete$(NC)"

# Generate JNI headers
generate-headers: compile-scala $(C_SRC)/include
	@echo "$(BLUE)Generating JNI headers...$(NC)"
	@CLASSPATH=$$($(SBT) "export runtime:fullClasspath" | tail -n 1); \
	CLASS_DIR="$(TARGET_DIR)/scala-$(SCALA_VERSION)/classes"; \
	echo "Trying to generate header with javac -h..."; \
	if [ -f "$$CLASS_DIR/com/zilliz/spark/connector/jni/MilvusStorageJNI$$.class" ]; then \
		javac -h $(C_SRC)/include -cp "$$CLASSPATH" $$CLASS_DIR/com/zilliz/spark/connector/jni/MilvusStorageJNI$$.class 2>/dev/null || { \
			echo "javac -h with class file failed, trying javah..."; \
			if command -v javah >/dev/null 2>&1; then \
				javah -classpath "$$CLASSPATH" -d $(C_SRC)/include com.zilliz.spark.connector.jni.MilvusStorageJNI 2>/dev/null || { \
					echo "javah failed, using manual header generation..."; \
					$(SCRIPT_DIR)/generate_jni_header.sh; \
				}; \
			else \
				echo "javah not available, using manual header generation..."; \
				$(SCRIPT_DIR)/generate_jni_header.sh; \
			fi; \
		}; \
	else \
		echo "Class file not found, using manual header generation..."; \
		$(SCRIPT_DIR)/generate_jni_header.sh; \
	fi
	@if [ -f "$(JNI_HEADER)" ]; then \
		echo "$(GREEN)JNI header generation complete$(NC)"; \
		echo "Generated header file: $(JNI_HEADER)"; \
	else \
		echo "$(RED)Error: Failed to generate JNI header file$(NC)"; \
		exit 1; \
	fi

# Compile C++ native library using Conan
compile-native: generate-headers $(RESOURCES_DIR)/native
	@echo "$(BLUE)Compiling C++ native library using Conan...$(NC)"
	@echo "$(YELLOW)Checking milvus-storage dependency...$(NC)"
	@if [ ! -f "milvus-storage/cpp/build/Release/libmilvus-storage.so" ]; then \
		echo "$(YELLOW)libmilvus-storage.so not found, building it first...$(NC)"; \
		if [ -d "milvus-storage/cpp" ]; then \
			echo "$(BLUE)Building milvus-storage library...$(NC)"; \
			cd milvus-storage/cpp && make build; \
			if [ ! -f "build/Release/libmilvus-storage.so" ]; then \
				echo "$(RED)Error: Failed to build libmilvus-storage.so$(NC)"; \
				exit 1; \
			fi; \
			echo "$(GREEN)✓ Successfully built libmilvus-storage.so$(NC)"; \
		else \
			echo "$(RED)Error: milvus-storage/cpp directory not found$(NC)"; \
			echo "$(YELLOW)Run 'make init-submodules' first$(NC)"; \
			exit 1; \
		fi; \
	else \
		echo "$(GREEN)✓ libmilvus-storage.so already exists$(NC)"; \
	fi
	@mkdir -p $(C_BUILD_DIR)
	@mkdir -p target/native
	@cd $(C_BUILD_DIR) && \
	conan install ../.. --build=missing -s build_type=Release --update && \
	conan build ../..
	@if [ -f "target/native/libmilvus_storage_jni.so" ]; then \
		echo "$(YELLOW)Copying native libraries to resources directory...$(NC)"; \
		cp target/native/libmilvus_storage_jni.so $(RESOURCES_DIR)/native/; \
		if [ -f "milvus-storage/cpp/build/Release/libmilvus-storage.so" ]; then \
			cp milvus-storage/cpp/build/Release/libmilvus-storage.so $(RESOURCES_DIR)/native/; \
			echo "$(GREEN)✓ Copied libmilvus-storage.so to resources$(NC)"; \
		else \
			echo "$(YELLOW)libmilvus-storage.so not found, attempting to build it...$(NC)"; \
			if [ -d "milvus-storage/cpp" ]; then \
				echo "$(BLUE)Building milvus-storage library...$(NC)"; \
				cd milvus-storage/cpp && make build; \
				if [ -f "build/Release/libmilvus-storage.so" ]; then \
					echo "$(GREEN)✓ Successfully built libmilvus-storage.so$(NC)"; \
					cp build/Release/libmilvus-storage.so ../../$(RESOURCES_DIR)/native/; \
					echo "$(GREEN)✓ Copied libmilvus-storage.so to resources$(NC)"; \
				else \
					echo "$(RED)Error: Failed to build libmilvus-storage.so$(NC)"; \
					echo "$(YELLOW)Continuing without libmilvus-storage.so (may cause runtime issues)$(NC)"; \
				fi; \
			else \
				echo "$(RED)Error: milvus-storage/cpp directory not found$(NC)"; \
				echo "$(YELLOW)Run 'make init-submodules' first$(NC)"; \
			fi; \
		fi; \
		echo "$(GREEN)Native library compilation complete$(NC)"; \
		echo "Library location: target/native/libmilvus_storage_jni.so"; \
		echo "Resources directory contents:"; \
		ls -la $(RESOURCES_DIR)/native/; \
		file target/native/libmilvus_storage_jni.so; \
		ldd target/native/libmilvus_storage_jni.so | head -10 || true; \
	else \
		echo "$(RED)Error: Native library compilation failed$(NC)"; \
		exit 1; \
	fi

# Package JAR with native library
package: compile-native
	@echo "$(BLUE)Packaging JAR with native library...$(NC)"
	@$(SBT) package
	@echo "$(GREEN)Packaging complete$(NC)"

# Run tests
test: package
	@echo "$(BLUE)Running tests...$(NC)"
	@$(SBT) test
	@echo "$(GREEN)Tests complete$(NC)"

# Run demo using sbt with custom task
run-demo:
	@echo "$(BLUE)Running Milvus Storage JNI demo using sbt custom task...$(NC)"
	@$(SBT) "runMain com.zilliz.spark.connector.jni.MilvusStorageJNI"

# Install library to system
install: compile-native
	@echo "$(BLUE)Installing library to system...$(NC)"
	@cd $(C_BUILD_DIR) && $(MAKE) install
	@echo "$(GREEN)Installation complete$(NC)"

# Development targets
dev-setup: check-deps init-submodules setup-milvus
	@echo "$(BLUE)Setting up development environment...$(NC)"
	@mkdir -p $(SCRIPT_DIR)
	@echo "$(GREEN)Development setup complete$(NC)"

rebuild: clean all

quick-build: compile-scala compile-native

# Debug targets
debug-info:
	@echo "$(BLUE)Debug Information:$(NC)"
	@echo "JAVA_HOME: $(JAVA_HOME)"
	@echo "SBT: $(SBT)"
	@echo "CMAKE: $(CMAKE)"
	@echo "Build Directory: $(BUILD_DIR)"
	@echo "Target Directory: $(TARGET_DIR)"
	@echo "JNI Header: $(JNI_HEADER)"
	@echo "Shared Library: $(SHARED_LIB)"

# Show build status
status:
	@echo "$(BLUE)Build Status:$(NC)"
	@echo -n "Scala classes: "
	@if [ -d "$(TARGET_DIR)/scala-$(SCALA_VERSION)/classes" ]; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi
	@echo -n "JNI headers: "
	@if [ -f "$(JNI_HEADER)" ]; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi
	@echo -n "Native library: "
	@if [ -f "target/native/libmilvus_storage_jni.so" ] || [ -f "$(RESOURCES_DIR)/native/libmilvus_storage_jni.so" ]; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi
	@echo -n "Milvus Storage SO: "
	@if [ -f "$(RESOURCES_DIR)/native/libmilvus-storage.so" ]; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi
	@echo -n "JAR package: "
	@if ls $(TARGET_DIR)/scala-$(SCALA_VERSION)/*.jar 1> /dev/null 2>&1; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi 
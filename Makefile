# Milvus Spark Connector Makefile
# Author: Zilliz
# Description: Build both upstream JNI libraries with one dynamic dependency set

# Configuration
SCALA_VERSION := 2.13
SBT := sbt
# On macOS `which java` is often the /usr/bin stub, so prefer a JDK 21 that
# java_home or Homebrew knows about before deriving it from the java binary.
ifeq ($(shell uname -s),Darwin)
  JAVA_HOME ?= $(shell { test -d /opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home && echo /opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home; } \
    || /usr/libexec/java_home -v 21 2>/dev/null \
    || /usr/libexec/java_home 2>/dev/null)
else
  JAVA_HOME ?= $(shell dirname $(shell dirname $(shell readlink -f $(shell which java))))
endif
export JAVA_HOME

# The milvus-storage Rust bridge fetches crates from git (lance, vortex). Let
# cargo use the git CLI so ~/.gitconfig URL rewrites, SSH keys, proxies and
# credential helpers work the same way they do for `git clone`.
export CARGO_NET_GIT_FETCH_WITH_CLI ?= true

TARGET_DIR := target
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)
ifeq ($(UNAME_S),Darwin)
  NATIVE_PLATFORM := darwin-$(if $(filter arm64 aarch64,$(UNAME_M)),aarch64,x86_64)
else
  NATIVE_PLATFORM := linux-$(if $(filter arm64 aarch64,$(UNAME_M)),aarch64,x86_64)
endif

# The native libraries come from one unified bundle. Building it needs this
# platform's Conan profile under native-build/profiles/ and its adapter in
# native-build/platforms.py; build.py names the one that is missing.
# NATIVE_BUNDLE selects a prebuilt platform JAR instead, and sbt's
# verifyNativeBundle checks that it matches this platform.
# NATIVE_BUNDLE_OUTPUT names the locally built JAR.
NATIVE_WORK_DIR ?= $(CURDIR)/$(TARGET_DIR)/native-build/$(NATIVE_PLATFORM)
NATIVE_JOBS ?= 50
NATIVE_BUILD_OPTIONS ?=
NATIVE_BUNDLE ?=
NATIVE_BUNDLE_OUTPUT ?= $(NATIVE_WORK_DIR)/milvus-native-$(NATIVE_PLATFORM).jar
# GNU Make's abspath treats spaces as separate paths. Keep each input intact.
absolute_path = $(if $(filter /%,$(firstword $(1))),$(1),$(CURDIR)/$(1))
NATIVE_BUNDLE_JAR := $(call absolute_path,$(if $(strip $(NATIVE_BUNDLE)),$(NATIVE_BUNDLE),$(NATIVE_BUNDLE_OUTPUT)))
NATIVE_SBT = $(SBT) "-Dmilvus.native.bundle=$(NATIVE_BUNDLE_JAR)"
# The JVM's signal-chaining library, by the platform's preload mechanism.
ifeq ($(UNAME_S),Darwin)
  NATIVE_TEST_ENV = DYLD_INSERT_LIBRARIES="$(JAVA_HOME)/lib/libjsig.dylib"
else
  NATIVE_TEST_ENV = LD_PRELOAD="$(JAVA_HOME)/lib/libjsig.so"
endif

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
NC := \033[0m # No Color

# All phony targets
.PHONY: all help init-submodules init-missing-submodules native-build native-bundle native-resources clean package test compile-it run-demo rebuild quick-build status

# Default target
all: package

# Help target
help:
	@echo "$(BLUE)Milvus Spark Connector Build System$(NC)"
	@echo ""
	@echo "$(YELLOW)Available targets:$(NC)"
	@echo "  $(GREEN)all$(NC)                    - Complete build process"
	@echo "  $(GREEN)clean$(NC)                  - Clean all build artifacts"
	@echo "  $(GREEN)native-build$(NC)           - Build unified native dependencies (storage and Knowhere)"
	@echo "  $(GREEN)native-bundle$(NC)          - Build and package, or select NATIVE_BUNDLE"
	@echo "  $(GREEN)native-resources$(NC)       - Same as native-bundle; the target Docker calls"
	@echo "  $(GREEN)package$(NC)                - Package JAR with the selected platform resources"
	@echo "  $(GREEN)test$(NC)                   - Type-check tests (incl. integration) and run unit tests"
	@echo "  $(GREEN)compile-it$(NC)             - Type-check integration tests (no external services)"
	@echo "  $(GREEN)run-demo$(NC)               - Run demo"
	@echo "  $(GREEN)init-submodules$(NC)        - Initialize Git submodules"
	@echo "  $(GREEN)status$(NC)                 - Show build status"
	@echo ""
	@echo "$(YELLOW)Environment variables:$(NC)"
	@echo "  JAVA_HOME=$(JAVA_HOME)"
	@echo "  SCALA_VERSION=$(SCALA_VERSION)"
	@echo "  NATIVE_WORK_DIR=$(NATIVE_WORK_DIR)"
	@echo "  NATIVE_JOBS=$(NATIVE_JOBS) (1..50)"
	@echo "  NATIVE_BUILD_OPTIONS=$(NATIVE_BUILD_OPTIONS) (for example --with-cardinal)"
	@echo "  NATIVE_BUNDLE=$(NATIVE_BUNDLE) (prebuilt platform JAR plus .properties)"
	@echo "  NATIVE_BUNDLE_OUTPUT=$(NATIVE_BUNDLE_OUTPUT)"
	@echo "  Without NATIVE_BUNDLE, the platform's profile under native-build/profiles/ builds both engines."
	@echo "  Profiles present: $(notdir $(wildcard native-build/profiles/*))"
	@echo "  NATIVE_PLATFORM=$(NATIVE_PLATFORM)"

# Initialize Git submodules
init-submodules:
	@echo "$(BLUE)Initializing Git submodules...$(NC)"
	@git submodule update --init --recursive
	@echo "$(GREEN)Submodules initialized$(NC)"

# Preserve initialized worktrees so the builder records their actual revisions and changes.
init-missing-submodules:
	@set -e; for submodule in milvus-storage milvus-proto knowhere; do \
		if [ ! -e "$$submodule/.git" ]; then git submodule update --init --recursive -- "$$submodule"; fi; \
	done

# Build both upstream engines; the script validates the job limit and host profile.
native-build: init-missing-submodules
	@echo "$(BLUE)Building unified native libraries with $(NATIVE_JOBS) jobs...$(NC)"
	@bash scripts/build-native.sh --work-dir "$(call absolute_path,$(NATIVE_WORK_DIR))" \
		--jobs "$(NATIVE_JOBS)" $(NATIVE_BUILD_OPTIONS)

ifneq ($(strip $(NATIVE_BUNDLE)),)
native-bundle: init-missing-submodules
	@test -s "$(NATIVE_BUNDLE_JAR)" || { echo "Missing prebuilt NATIVE_BUNDLE: $(NATIVE_BUNDLE_JAR)"; exit 1; }
	@test -s "$(NATIVE_BUNDLE_JAR).properties" || { echo "Missing native bundle checksum sidecar: $(NATIVE_BUNDLE_JAR).properties"; exit 1; }
	@echo "$(GREEN)Selected prebuilt bundle: $(NATIVE_BUNDLE_JAR) (sbt verifies platform, provenance and libraries)$(NC)"
else
native-bundle: native-build
	@python3 scripts/package-native.py \
		--lib-dir "$(call absolute_path,$(NATIVE_WORK_DIR))/bundle/lib" \
		--provenance "$(call absolute_path,$(NATIVE_WORK_DIR))/bundle/provenance.json" \
		--evidence "$(call absolute_path,$(NATIVE_WORK_DIR))/bundle/provenance" \
		--licenses "$(call absolute_path,$(NATIVE_WORK_DIR))/bundle/licenses" \
		--output "$(NATIVE_BUNDLE_JAR)"
endif

# Docker builds its native resources through this target.
native-resources: native-bundle

# Clean build artifacts
clean:
	@echo "$(BLUE)Cleaning build artifacts...$(NC)"
	@rm -rf $(TARGET_DIR)
	@rm -rf project/target
	@rm -rf project/project
	@$(SBT) clean
	@echo "$(GREEN)Clean complete$(NC)"

# Package JAR with the platform's unified native bundle.
package: native-bundle
	@echo "$(BLUE)Packaging JAR with $(NATIVE_BUNDLE_JAR)...$(NC)"
	@$(NATIVE_SBT) package
	@echo "$(GREEN)Packaging complete$(NC)"

# Run tests
test: package
	@echo "$(BLUE)Type-checking integration tests...$(NC)"
	@$(NATIVE_SBT) "integration40/Test/compile"
	@echo "$(BLUE)Running unit tests...$(NC)"
	@$(NATIVE_TEST_ENV) $(NATIVE_SBT) test
	@echo "$(GREEN)Tests complete$(NC)"

# Type-check integration tests only (no Milvus/MinIO required at compile time)
compile-it: package
	@echo "$(BLUE)Type-checking integration tests...$(NC)"
	@$(NATIVE_SBT) "integration40/Test/compile"
	@echo "$(GREEN)Integration tests compile complete$(NC)"

# Run demo
run-demo: package
	@echo "$(BLUE)Running demo...$(NC)"
	@$(NATIVE_TEST_ENV) $(NATIVE_SBT) "runMain com.zilliz.spark.connector.jni.MilvusStorageJNI"

# Development targets
rebuild:
	@$(MAKE) clean
	@$(MAKE) all

quick-build: package

# Show build status
status:
	@echo "$(BLUE)Build Status:$(NC)"
	@echo "Platform: $(NATIVE_PLATFORM)"
	@printf "Unified native bundle: "
	@if [ -s "$(NATIVE_BUNDLE_JAR)" ] && [ -s "$(NATIVE_BUNDLE_JAR).properties" ]; then echo "$(GREEN)$(NATIVE_BUNDLE_JAR)$(NC)"; else echo "$(YELLOW)not built$(NC)"; fi
	@printf "JAR package: "
	@if ls $(TARGET_DIR)/scala-$(SCALA_VERSION)/*.jar 1> /dev/null 2>&1; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi

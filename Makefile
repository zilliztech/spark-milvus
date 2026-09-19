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

# Unified source builds currently use the validated Linux x86_64 GCC 12 profile.
# Linux aarch64 can consume a separately built, matching platform JAR and sidecar.
# NATIVE_BUNDLE selects a prebuilt input; NATIVE_BUNDLE_OUTPUT names local output.
NATIVE_WORK_DIR ?= $(CURDIR)/$(TARGET_DIR)/native-build/$(NATIVE_PLATFORM)
NATIVE_JOBS ?= 50
NATIVE_BUILD_OPTIONS ?=
NATIVE_BUNDLE ?=
NATIVE_BUNDLE_OUTPUT ?= $(NATIVE_WORK_DIR)/milvus-native-$(NATIVE_PLATFORM).jar
# GNU Make's abspath treats spaces as separate paths. Keep each input intact.
absolute_path = $(if $(filter /%,$(firstword $(1))),$(1),$(CURDIR)/$(1))
NATIVE_BUNDLE_JAR := $(call absolute_path,$(if $(strip $(NATIVE_BUNDLE)),$(NATIVE_BUNDLE),$(NATIVE_BUNDLE_OUTPUT)))
# Keep the existing storage build on platforms without a unified source profile.
# A prebuilt unified bundle is always explicit and must match the target platform.
ifneq ($(strip $(NATIVE_BUNDLE)),)
  NATIVE_PREREQUISITE := native-bundle
else ifeq ($(NATIVE_PLATFORM),linux-x86_64)
  NATIVE_PREREQUISITE := native-bundle
else
  NATIVE_PREREQUISITE := copy-native-libs
endif
NATIVE_SBT = $(SBT) $(if $(filter native-bundle,$(NATIVE_PREREQUISITE)),"-Dmilvus.native.bundle=$(NATIVE_BUNDLE_JAR)")
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
.PHONY: all help check-deps init-submodules init-missing-submodules native-build native-bundle native-resources build-milvus-storage copy-native-libs clean clean-all package test compile-it run-demo rebuild quick-build status

# Default target
all: package

# Help target
help:
	@echo "$(BLUE)Milvus Spark Connector Build System$(NC)"
	@echo ""
	@echo "$(YELLOW)Available targets:$(NC)"
	@echo "  $(GREEN)all$(NC)                    - Complete build process"
	@echo "  $(GREEN)clean$(NC)                  - Clean all build artifacts"
	@echo "  $(GREEN)native-build$(NC)           - Build unified native dependencies (Linux x86_64)"
	@echo "  $(GREEN)native-bundle$(NC)          - Build and package, or select NATIVE_BUNDLE"
	@echo "  $(GREEN)native-resources$(NC)       - Prepare unified or existing platform storage resources"
	@echo "  $(GREEN)build-milvus-storage$(NC)   - Legacy standalone storage JNI build"
	@echo "  $(GREEN)copy-native-libs$(NC)       - Legacy storage resource packaging"
	@echo "  $(GREEN)package$(NC)                - Package JAR with the selected platform resources"
	@echo "  $(GREEN)test$(NC)                   - Type-check tests (incl. integration) and run unit tests"
	@echo "  $(GREEN)compile-it$(NC)             - Type-check integration tests (no external services)"
	@echo "  $(GREEN)run-demo$(NC)               - Run demo"
	@echo "  $(GREEN)init-submodules$(NC)        - Initialize Git submodules"
	@echo "  $(GREEN)check-deps$(NC)             - Check system dependencies"
	@echo "  $(GREEN)status$(NC)                 - Show build status"
	@echo ""
	@echo "$(YELLOW)Environment variables:$(NC)"
	@echo "  JAVA_HOME=$(JAVA_HOME)"
	@echo "  SCALA_VERSION=$(SCALA_VERSION)"
	@echo "  NATIVE_WORK_DIR=$(NATIVE_WORK_DIR)"
	@echo "  NATIVE_JOBS=$(NATIVE_JOBS) (1..50)"
	@echo "  NATIVE_BUILD_OPTIONS=$(NATIVE_BUILD_OPTIONS) (for example --with-cardinal)"
	@echo "  NATIVE_BUNDLE=$(NATIVE_BUNDLE) (prebuilt Linux platform JAR plus .properties)"
	@echo "  NATIVE_BUNDLE_OUTPUT=$(NATIVE_BUNDLE_OUTPUT)"
	@echo "  Without NATIVE_BUNDLE, Linux x86_64 builds both engines; other platforms retain storage-only builds."
	@echo "  Unified bundles require Linux; their source profile currently supports x86_64."
	@echo "  NATIVE_PLATFORM=$(NATIVE_PLATFORM) -> $(NATIVE_DIR)"
	@echo "  macOS: run scripts/macos_conan_fixups.sh once before build-milvus-storage (see docs/contributing.md)."

# Check system dependencies
check-deps:
	@echo "$(BLUE)Checking system dependencies...$(NC)"
	@command -v java >/dev/null 2>&1 || { echo "$(RED)Error: Java not found$(NC)"; exit 1; }
	@command -v javac >/dev/null 2>&1 || { echo "$(RED)Error: javac not found$(NC)"; exit 1; }
	@command -v $(SBT) >/dev/null 2>&1 || { echo "$(RED)Error: sbt not found$(NC)"; exit 1; }
	@command -v make >/dev/null 2>&1 || { echo "$(RED)Error: make not found$(NC)"; exit 1; }
	@command -v conan >/dev/null 2>&1 || { echo "$(RED)Error: conan not found$(NC)"; exit 1; }
	@command -v cmake >/dev/null 2>&1 || { echo "$(RED)Error: cmake not found$(NC)"; exit 1; }
	@command -v cargo >/dev/null 2>&1 || { echo "$(RED)Error: cargo not found (https://rustup.rs)$(NC)"; exit 1; }
	@test -f "$(JAVA_HOME)/include/jni.h" || { echo "$(RED)Error: JAVA_HOME=$(JAVA_HOME) has no include/jni.h (need a JDK)$(NC)"; exit 1; }
ifeq ($(UNAME_S),Darwin)
	@command -v install_name_tool >/dev/null 2>&1 || { echo "$(RED)Error: install_name_tool not found (xcode-select --install)$(NC)"; exit 1; }
	@command -v codesign >/dev/null 2>&1 || { echo "$(RED)Error: codesign not found$(NC)"; exit 1; }
	@test -f /opt/homebrew/opt/libomp/lib/libomp.dylib -o -f /usr/local/opt/libomp/lib/libomp.dylib || echo "$(YELLOW)Warning: libomp not found (brew install libomp); the milvus-common recipe needs it$(NC)"
else
	@command -v patchelf >/dev/null 2>&1 || { echo "$(RED)Error: patchelf not found$(NC)"; exit 1; }
endif
	@echo "$(GREEN)All dependencies found$(NC)"

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
	@test "$(UNAME_S)" = Linux || { echo "Unified native bundles require Linux; use the explicit legacy storage targets on macOS."; exit 1; }
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

# Make and Docker share the same platform selection.
native-resources: $(NATIVE_PREREQUISITE)

# Build milvus-storage with JNI support
build-milvus-storage: check-deps init-missing-submodules
	@echo "$(BLUE)Building milvus-storage with JNI support...$(NC)"
	@case "$(NATIVE_JOBS)" in ''|*[!0-9]*) echo "NATIVE_JOBS must be between 1 and 50"; exit 1 ;; esac; \
		test "$(NATIVE_JOBS)" -ge 1 && test "$(NATIVE_JOBS)" -le 50
	@if [ ! -d "$(MILVUS_STORAGE_CPP)" ]; then \
		echo "$(RED)Error: milvus-storage/cpp directory not found$(NC)"; \
		echo "$(YELLOW)Run 'make init-submodules' first$(NC)"; \
		exit 1; \
	fi
	@CARGO_BUILD_JOBS=1 CMAKE_BUILD_PARALLEL_LEVEL="$(NATIVE_JOBS)" \
		$(MAKE) -C "$(MILVUS_STORAGE_CPP)" java-lib \
		'CONAN_SETTINGS=$$(libcxx_setting) -s:h build_type=$$(build_type) -s:b build_type=$$(build_type) -c:h tools.build:jobs=$(NATIVE_JOBS) -c:b tools.build:jobs=$(NATIVE_JOBS)'
	@if [ -f "$(STORAGE_LIB)" ] && [ -f "$(STORAGE_JNI_LIB)" ]; then \
		echo "$(GREEN)✓ Successfully built milvus-storage with JNI$(NC)"; \
		ls -lh $(STORAGE_LIB) $(STORAGE_JNI_LIB); \
	else \
		echo "$(RED)Error: Failed to build milvus-storage JNI libraries$(NC)"; \
		echo "$(YELLOW)Looked for $(STORAGE_LIB) and $(STORAGE_JNI_LIB)$(NC)"; \
		exit 1; \
	fi

# Incrementally rebuild before copying so existing libraries cannot become stale.
# The platform directory is created here, not as a prerequisite, so a dry run
# or a test that overrides this target leaves no empty directory behind.
copy-native-libs: build-milvus-storage
	@echo "$(BLUE)Copying native libraries to $(NATIVE_DIR)...$(NC)"
	@mkdir -p "$(NATIVE_DIR)"
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
	@if [ "$(UNAME_S)" = Darwin ]; then \
		bash scripts/patch_native_macos.sh "$(NATIVE_DIR)"; \
	else \
		bash "$(MILVUS_STORAGE_CPP)/../java/patch_native_runpath.sh" "$(NATIVE_DIR)"; \
	fi
	@if [ ! -f "$(NATIVE_DIR)/libmilvus-storage-jni.$(LIB_SUFFIX)" ]; then \
		echo "$(RED)Error: libmilvus-storage-jni did not reach $(NATIVE_DIR)$(NC)"; \
		exit 1; \
	fi
	@echo "$(GREEN)✓ Copied native libraries to resources$(NC)"
	@ls -lh $(NATIVE_DIR)/ | head -20

# Clean build artifacts
clean:
	@echo "$(BLUE)Cleaning build artifacts...$(NC)"
	@rm -rf $(TARGET_DIR)
	@rm -rf project/target
	@rm -rf project/project
	@echo "$(YELLOW)Cleaning native libraries from resources...$(NC)"
	@rm -rf "$(NATIVE_DIR)"
	@$(SBT) clean
	@echo "$(GREEN)Clean complete$(NC)"

# Clean everything including milvus-storage build
clean-all: clean
	@echo "$(BLUE)Cleaning milvus-storage build...$(NC)"
	@if [ -d "$(MILVUS_STORAGE_CPP)" ]; then \
		cd $(MILVUS_STORAGE_CPP) && make clean; \
	fi
	@echo "$(GREEN)Clean all complete$(NC)"

# Package JAR with the platform's selected native resources.
package: native-resources
	@echo "$(BLUE)Packaging JAR with $(NATIVE_PREREQUISITE)...$(NC)"
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
	@$(MAKE) clean-all
	@$(MAKE) all

quick-build: package

# Show build status
status:
	@echo "$(BLUE)Build Status:$(NC)"
	@echo "Platform: $(NATIVE_PLATFORM) (.$(LIB_SUFFIX))"
	@printf "Unified native bundle: "
	@if [ -s "$(NATIVE_BUNDLE_JAR)" ] && [ -s "$(NATIVE_BUNDLE_JAR).properties" ]; then echo "$(GREEN)$(NATIVE_BUNDLE_JAR)$(NC)"; else echo "$(YELLOW)not built$(NC)"; fi
	@printf "Milvus Storage lib: "
	@if [ -f "$(STORAGE_LIB)" ]; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi
	@printf "Milvus Storage JNI lib: "
	@if [ -f "$(STORAGE_JNI_LIB)" ]; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi
	@printf "Native libs in resources: "
	@if [ -f "$(NATIVE_DIR)/libmilvus-storage.$(LIB_SUFFIX)" ] && [ -f "$(NATIVE_DIR)/libmilvus-storage-jni.$(LIB_SUFFIX)" ]; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi
	@printf "JAR package: "
	@if ls $(TARGET_DIR)/scala-$(SCALA_VERSION)/*.jar 1> /dev/null 2>&1; then echo "$(GREEN)✓$(NC)"; else echo "$(RED)✗$(NC)"; fi

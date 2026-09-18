"""Resolve one host dependency graph for both native engines."""
import json
import os

from conan import ConanFile
from conan.tools.cmake import CMakeDeps, CMakeToolchain
from conan.tools.env import VirtualBuildEnv, VirtualRunEnv


class MilvusNativeDependencies(ConanFile):
    name = "spark-milvus-native-dependencies"
    version = "1"
    settings = "os", "arch", "compiler", "build_type"
    default_options = {
        "arrow/*:with_jemalloc": False,
        "arrow/*:with_s3": True,
        "arrow/*:with_azure": True,
        "arrow/*:filesystem_layer": True,
        "arrow/*:dataset_modules": True,
        "arrow/*:acero": True,
        "arrow/*:parquet": True,
        "arrow/*:with_re2": True,
        "arrow/*:with_zstd": True,
        "arrow/*:with_boost": True,
        "arrow/*:with_thrift": True,
        "arrow/*:encryption": True,
        "arrow/*:with_openssl": True,
        "arrow/*:with_snappy": True,
        "arrow/*:with_lz4": True,
        "aws-sdk-cpp/*:config": True,
        "aws-sdk-cpp/*:s3-crt": False,
        "aws-sdk-cpp/*:text-to-speech": False,
        "aws-sdk-cpp/*:transfer": False,
        "boost/*:without_test": True,
        "boost/*:without_stacktrace": True,
        "glog/*:with_gflags": True,
        "grpc/*:secure": True,
        "libcurl/*:with_ssl": "openssl",
        "opentelemetry-cpp/*:with_stl": True,
        "openssl/*:no_apps": True,
        "prometheus-cpp/*:with_pull": False,
    }

    def requirements(self):
        with open(os.path.join(self.recipe_folder, "dependencies.json")) as source:
            pinned = json.load(source)
        # Some upstream recipes exist for one operating system only; the
        # engines' own conanfiles gate the same names, and dependencies.json
        # records which.
        scope = pinned.get("platform_scope", {})
        for name, reference in pinned["references"].items():
            allowed = scope.get(name)
            if allowed and str(self.settings.os) not in allowed:
                continue
            self.requires(reference, force=True)

    def generate(self):
        toolchain = CMakeToolchain(self)
        # The driver configures both engines directly rather than through presets.
        toolchain.variables["CMAKE_POSITION_INDEPENDENT_CODE"] = True
        toolchain.variables["CMAKE_POLICY_DEFAULT_CMP0077"] = "NEW"
        toolchain.variables["CMAKE_POLICY_DEFAULT_CMP0144"] = "NEW"
        toolchain.generate()
        dependencies = CMakeDeps(self)
        dependencies.generate()
        VirtualBuildEnv(self).generate()
        VirtualRunEnv(self).generate()

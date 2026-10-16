/* Exercise the pinned upstream C API against the released shared libraries. */
#include <cpuid.h>

#define main knowhere_upstream_test_main
#include "../../knowhere/tests/c_api/test_c_api.c"
#undef main

static void
check_cpu(const char* model) {
    unsigned int eax, ebx, ecx, edx;
    char vendor[13] = {0};
    unsigned int max_leaf = __get_cpuid_max(0, NULL);
    __cpuid(0, eax, ebx, ecx, edx);
    memcpy(vendor, &ebx, 4);
    memcpy(vendor + 4, &edx, 4);
    memcpy(vendor + 8, &ecx, 4);
    CHECK(max_leaf >= 1);
    __cpuid(1, eax, ebx, ecx, edx);
    unsigned int signature = eax;
    int avx = !!(ecx & bit_AVX);
    int fma = !!(ecx & bit_FMA);
    int avx2 = 0, avx512 = 0;
    if (max_leaf >= 7) {
        __cpuid_count(7, 0, eax, ebx, ecx, edx);
        avx2 = !!(ebx & bit_AVX2);
        avx512 = !!(ebx & bit_AVX512F);
    }
    printf("CPU contract: model=%s vendor=%s signature=0x%x AVX=%d AVX2=%d FMA=%d AVX512F=%d\n",
           model, vendor, signature, avx, avx2, fma, avx512);
    fflush(stdout);
    int nehalem = strcmp(model, "Nehalem") == 0;
    int haswell = strcmp(model, "Haswell") == 0;
    int epyc = strcmp(model, "EPYC") == 0;
    CHECK(nehalem || haswell || epyc);
    CHECK(strcmp(vendor, epyc ? "AuthenticAMD" : "GenuineIntel") == 0);
    CHECK(avx == !nehalem);
    CHECK(avx2 == !nehalem);
    CHECK(fma == !nehalem);
    CHECK(!avx512);
}

int
main(int argc, char** argv) {
    CHECK(argc == 2);
    check_cpu(argv[1]);
    CHECK(knowhere_c_abi_version() == 1);
    OK(knowhere_search_thread_pool_resize(1));
    OK(knowhere_build_thread_pool_resize(1));
    check_batched("L2", 64, 4995, 32, 10);
    puts("PASS native CPU probe: L2 nq=64 nb=4995 dim=32 top_k=10");
    return 0;
}

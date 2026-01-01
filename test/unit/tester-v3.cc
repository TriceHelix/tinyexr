/*
 * TinyEXR V3 API Test Suite
 *
 * Tests for the new Vulkan-style C API and C++17 wrapper.
 *
 * Copyright (c) 2024 TinyEXR authors
 * SPDX-License-Identifier: BSD-3-Clause
 */

/* Enable PIZ, PXR24, and B44 support in V3 (requires V1 implementation) */
#define TINYEXR_V3_ENABLE_PIZ 1
#define TINYEXR_V3_ENABLE_PXR24 1
#define TINYEXR_V3_ENABLE_B44 1

#include "../../tinyexr_v3.hh"
#include "../../tinyexr_c.h"

/* Include V1 API for comparison testing */
#define TINYEXR_IMPLEMENTATION
#include "../../tinyexr.h"

/* PIZ wrapper function - calls V1's DecompressPiz */
extern "C" bool tinyexr_v3_decompress_piz(
    unsigned char* outPtr, const unsigned char* inPtr,
    size_t tmpBufSizeInBytes, size_t inLen, int num_channels,
    const EXRChannelInfo* channels, int data_width, int num_lines) {
    return tinyexr::DecompressPiz(outPtr, inPtr, tmpBufSizeInBytes, inLen,
                                   num_channels, channels, data_width, num_lines);
}

/* PXR24 wrapper function - calls V1's DecompressPxr24 */
extern "C" bool tinyexr_v3_decompress_pxr24(
    unsigned char* outPtr, size_t outBufSize,
    const unsigned char* inPtr, size_t inLen,
    int data_width, int num_lines,
    size_t num_channels, const EXRChannelInfo* channels) {
    return tinyexr::DecompressPxr24(outPtr, outBufSize, inPtr, inLen,
                                     data_width, num_lines, num_channels, channels);
}

/* B44 wrapper function - calls V1's DecompressB44 */
extern "C" bool tinyexr_v3_decompress_b44(
    unsigned char* outPtr, size_t outBufSize,
    const unsigned char* inPtr, size_t inLen,
    int data_width, int num_lines,
    size_t num_channels, const EXRChannelInfo* channels, bool is_b44a) {
    return tinyexr::DecompressB44(outPtr, outBufSize, inPtr, inLen,
                                   data_width, num_lines, num_channels, channels, is_b44a);
}

#include <cstdio>
#include <cstring>
#include <cassert>
#include <stdexcept>
#include <vector>
#include <cmath>

/* ============================================================================
 * Test Utilities
 * ============================================================================ */

static int g_tests_passed = 0;
static int g_tests_failed = 0;

#define TEST(name) \
    static void test_##name(); \
    static void run_test_##name() { \
        printf("  [TEST] %s... ", #name); \
        fflush(stdout); \
        try { \
            test_##name(); \
            printf("PASSED\n"); \
            g_tests_passed++; \
        } catch (const std::exception& e) { \
            printf("FAILED: %s\n", e.what()); \
            g_tests_failed++; \
        } catch (...) { \
            printf("FAILED: unknown exception\n"); \
            g_tests_failed++; \
        } \
    } \
    static void test_##name()

#define REQUIRE(cond) \
    do { \
        if (!(cond)) { \
            throw std::runtime_error("Assertion failed: " #cond); \
        } \
    } while (0)

#define REQUIRE_EQ(a, b) \
    do { \
        if ((a) != (b)) { \
            throw std::runtime_error("Assertion failed: " #a " == " #b); \
        } \
    } while (0)

/* ============================================================================
 * C API Tests
 * ============================================================================ */

TEST(c_api_version) {
    int major, minor, patch;
    exr_get_version(&major, &minor, &patch);
    REQUIRE_EQ(major, TINYEXR_C_API_VERSION_MAJOR);
    REQUIRE_EQ(minor, TINYEXR_C_API_VERSION_MINOR);
    REQUIRE_EQ(patch, TINYEXR_C_API_VERSION_PATCH);
}

TEST(c_api_version_string) {
    const char* ver = exr_get_version_string();
    REQUIRE(ver != nullptr);
    REQUIRE(strlen(ver) > 0);
}

TEST(c_api_result_to_string) {
    const char* s = exr_result_to_string(EXR_SUCCESS);
    REQUIRE(strcmp(s, "Success") == 0);

    s = exr_result_to_string(EXR_ERROR_INVALID_HANDLE);
    REQUIRE(strcmp(s, "Invalid handle") == 0);

    s = exr_result_to_string(EXR_ERROR_OUT_OF_MEMORY);
    REQUIRE(strcmp(s, "Out of memory") == 0);
}

TEST(c_api_simd_info) {
    const char* info = exr_get_simd_info();
    REQUIRE(info != nullptr);
    printf("[%s] ", info);
}

TEST(c_api_context_create_destroy) {
    ExrContextCreateInfo info = {};
    info.api_version = TINYEXR_C_API_VERSION;

    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);
    REQUIRE(ctx != nullptr);

    exr_context_destroy(ctx);
}

TEST(c_api_context_invalid_version) {
    ExrContextCreateInfo info = {};
    info.api_version = (99 << 22);  // Invalid major version

    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&info, &ctx);
    REQUIRE_EQ(result, EXR_ERROR_INVALID_VERSION);
    REQUIRE(ctx == nullptr);
}

TEST(c_api_context_ref_counting) {
    ExrContextCreateInfo info = {};
    info.api_version = TINYEXR_C_API_VERSION;

    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);

    exr_context_add_ref(ctx);
    exr_context_release(ctx);  // Should not destroy
    exr_context_release(ctx);  // Should destroy
}

TEST(c_api_error_handling) {
    ExrContextCreateInfo info = {};
    info.api_version = TINYEXR_C_API_VERSION;

    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);

    // Initially no errors
    REQUIRE_EQ(exr_get_error_count(ctx), 0u);

    exr_clear_errors(ctx);
    REQUIRE_EQ(exr_get_error_count(ctx), 0u);

    exr_context_destroy(ctx);
}

TEST(c_api_memory_pool) {
    ExrContextCreateInfo ctx_info = {};
    ctx_info.api_version = TINYEXR_C_API_VERSION;

    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&ctx_info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrMemoryPoolCreateInfo pool_info = {};
    pool_info.initial_size = 1024;
    pool_info.max_size = 0;  // Unlimited

    ExrMemoryPool pool = nullptr;
    result = exr_memory_pool_create(ctx, &pool_info, &pool);
    REQUIRE_EQ(result, EXR_SUCCESS);
    REQUIRE(pool != nullptr);

    size_t used = exr_memory_pool_get_used(pool);
    REQUIRE_EQ(used, 0u);

    exr_memory_pool_reset(pool);
    exr_memory_pool_destroy(pool);
    exr_context_destroy(ctx);
}

TEST(c_api_data_source_from_memory) {
    uint8_t test_data[] = { 0x76, 0x2F, 0x31, 0x01 };  // EXR magic

    ExrDataSource source = {};
    ExrResult result = exr_data_source_from_memory(test_data, sizeof(test_data), &source);
    REQUIRE_EQ(result, EXR_SUCCESS);
    REQUIRE(source.fetch != nullptr);
    REQUIRE(source.total_size == sizeof(test_data));
    REQUIRE(source.flags & EXR_DATA_SOURCE_SEEKABLE);
}

TEST(c_api_decoder_create_destroy) {
    uint8_t test_data[] = { 0x76, 0x2F, 0x31, 0x01 };  // EXR magic

    ExrContextCreateInfo ctx_info = {};
    ctx_info.api_version = TINYEXR_C_API_VERSION;

    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&ctx_info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrDataSource source = {};
    result = exr_data_source_from_memory(test_data, sizeof(test_data), &source);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrDecoderCreateInfo dec_info = {};
    dec_info.source = source;

    ExrDecoder decoder = nullptr;
    result = exr_decoder_create(ctx, &dec_info, &decoder);
    REQUIRE_EQ(result, EXR_SUCCESS);
    REQUIRE(decoder != nullptr);

    exr_decoder_destroy(decoder);
    exr_context_destroy(ctx);
}

TEST(c_api_fence_create_destroy) {
    ExrContextCreateInfo ctx_info = {};
    ctx_info.api_version = TINYEXR_C_API_VERSION;

    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&ctx_info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrFenceCreateInfo fence_info = {};
    fence_info.flags = EXR_FENCE_SIGNALED;

    ExrFence fence = nullptr;
    result = exr_fence_create(ctx, &fence_info, &fence);
    REQUIRE_EQ(result, EXR_SUCCESS);
    REQUIRE(fence != nullptr);

    // Should already be signaled
    result = exr_fence_get_status(fence);
    REQUIRE_EQ(result, EXR_SUCCESS);

    // Wait should return immediately
    result = exr_fence_wait(fence, EXR_TIMEOUT_NONE);
    REQUIRE_EQ(result, EXR_SUCCESS);

    // Reset and check
    result = exr_fence_reset(fence);
    REQUIRE_EQ(result, EXR_SUCCESS);

    result = exr_fence_get_status(fence);
    REQUIRE_EQ(result, EXR_ERROR_NOT_READY);

    exr_fence_destroy(fence);
    exr_context_destroy(ctx);
}

TEST(c_api_command_buffer) {
    ExrContextCreateInfo ctx_info = {};
    ctx_info.api_version = TINYEXR_C_API_VERSION;

    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&ctx_info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrCommandBufferCreateInfo cmd_info = {};
    cmd_info.decoder = nullptr;  // No decoder for this test
    cmd_info.flags = 0;

    ExrCommandBuffer cmd = nullptr;
    result = exr_command_buffer_create(ctx, &cmd_info, &cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);
    REQUIRE(cmd != nullptr);

    result = exr_command_buffer_begin(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    result = exr_command_buffer_end(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    result = exr_command_buffer_reset(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    exr_command_buffer_destroy(cmd);
    exr_context_destroy(ctx);
}

TEST(c_api_half_conversion) {
    // Test half to float conversion
    uint16_t half_vals[] = { 0x0000, 0x3C00, 0x4000, 0x7C00 };  // 0, 1, 2, inf
    float float_vals[4];

    exr_convert_half_to_float(half_vals, float_vals, 4);

    REQUIRE(float_vals[0] == 0.0f);
    REQUIRE(float_vals[1] == 1.0f);
    REQUIRE(float_vals[2] == 2.0f);
    // float_vals[3] should be infinity

    // Test float to half conversion
    float src_floats[] = { 0.0f, 1.0f, 2.0f, 0.5f };
    uint16_t dst_halfs[4];

    exr_convert_float_to_half(src_floats, dst_halfs, 4);

    REQUIRE(dst_halfs[0] == 0x0000);  // 0
    REQUIRE(dst_halfs[1] == 0x3C00);  // 1
    REQUIRE(dst_halfs[2] == 0x4000);  // 2
    REQUIRE(dst_halfs[3] == 0x3800);  // 0.5
}

/* ============================================================================
 * C++ Wrapper Tests
 * ============================================================================ */

TEST(cpp_version_string) {
    std::string ver = tinyexr::v3::get_version_string();
    REQUIRE(!ver.empty());
}

TEST(cpp_simd_info) {
    std::string info = tinyexr::v3::get_simd_info();
    REQUIRE(!info.empty());
    printf("[%s] ", info.c_str());
}

TEST(cpp_context_create) {
    auto result = tinyexr::v3::Context::create();
    REQUIRE(result.success);
    REQUIRE(result.value);
}

TEST(cpp_context_with_options) {
    tinyexr::v3::Context::CreateInfo info;
    info.enable_validation = true;
    info.single_threaded = true;

    auto result = tinyexr::v3::Context::create(info);
    REQUIRE(result.success);
    REQUIRE(result.value);
}

TEST(cpp_result_ok) {
    auto result = tinyexr::v3::Result<int>::ok(42);
    REQUIRE(result.success);
    REQUIRE(result.value == 42);
    REQUIRE(static_cast<bool>(result));
}

TEST(cpp_result_error) {
    auto result = tinyexr::v3::Result<int>::error(EXR_ERROR_IO, "Test error");
    REQUIRE(!result.success);
    REQUIRE(result.first_error().code == EXR_ERROR_IO);
    REQUIRE(!static_cast<bool>(result));
}

TEST(cpp_result_value_or) {
    auto ok_result = tinyexr::v3::Result<int>::ok(42);
    REQUIRE(ok_result.value_or(0) == 42);

    auto err_result = tinyexr::v3::Result<int>::error(EXR_ERROR_IO);
    REQUIRE(err_result.value_or(99) == 99);
}

TEST(cpp_result_map) {
    auto result = tinyexr::v3::Result<int>::ok(21);
    auto mapped = result.map([](int x) { return x * 2; });
    REQUIRE(mapped.success);
    REQUIRE(mapped.value == 42);
}

TEST(cpp_result_map_error) {
    auto result = tinyexr::v3::Result<int>::error(EXR_ERROR_IO);
    auto mapped = result.map([](int x) { return x * 2; });
    REQUIRE(!mapped.success);
    REQUIRE(mapped.first_error().code == EXR_ERROR_IO);
}

TEST(cpp_result_and_then) {
    auto result = tinyexr::v3::Result<int>::ok(21);
    auto chained = result.and_then([](int x) {
        return tinyexr::v3::Result<std::string>::ok(std::to_string(x * 2));
    });
    REQUIRE(chained.success);
    REQUIRE(chained.value == "42");
}

TEST(cpp_result_void) {
    auto result = tinyexr::v3::Result<void>::ok();
    REQUIRE(result.success);
    REQUIRE(static_cast<bool>(result));

    auto err = tinyexr::v3::Result<void>::error(EXR_ERROR_IO);
    REQUIRE(!err.success);
}

TEST(cpp_error_info) {
    tinyexr::v3::ErrorInfo info(EXR_ERROR_IO, "Test message", "parsing", 1234);
    REQUIRE(info.code == EXR_ERROR_IO);
    REQUIRE(info.message == "Test message");
    REQUIRE(info.context == "parsing");
    REQUIRE(info.byte_position == 1234);

    std::string str = info.to_string();
    REQUIRE(!str.empty());
}

TEST(cpp_decoder_from_memory) {
    auto ctx_result = tinyexr::v3::Context::create();
    REQUIRE(ctx_result.success);

    uint8_t test_data[] = { 0x76, 0x2F, 0x31, 0x01, 0, 0, 0, 0 };
    auto decoder_result = tinyexr::v3::Decoder::from_memory(
        ctx_result.value, test_data, sizeof(test_data));
    REQUIRE(decoder_result.success);
    REQUIRE(decoder_result.value);
}

TEST(cpp_tile_coord) {
    tinyexr::v3::TileCoord a{0, 0, 1, 2};
    tinyexr::v3::TileCoord b{0, 0, 1, 2};
    tinyexr::v3::TileCoord c{0, 0, 1, 3};

    REQUIRE(a == b);
    REQUIRE(a != c);
}

/* ============================================================================
 * Header Parsing Tests
 * ============================================================================ */

/* Helper: Read file into vector */
static std::vector<uint8_t> read_file(const char* path) {
    FILE* f = fopen(path, "rb");
    if (!f) return {};
    fseek(f, 0, SEEK_END);
    size_t size = ftell(f);
    fseek(f, 0, SEEK_SET);
    std::vector<uint8_t> data(size);
    size_t read = fread(data.data(), 1, size, f);
    fclose(f);
    if (read != size) return {};
    return data;
}

TEST(c_api_parse_invalid_magic) {
    ExrContextCreateInfo ctx_info = {};
    ctx_info.api_version = TINYEXR_C_API_VERSION;

    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&ctx_info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);

    /* Invalid EXR data - wrong magic */
    uint8_t bad_data[] = { 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00 };

    ExrDataSource source = {};
    result = exr_data_source_from_memory(bad_data, sizeof(bad_data), &source);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrDecoderCreateInfo dec_info = {};
    dec_info.source = source;

    ExrDecoder decoder = nullptr;
    result = exr_decoder_create(ctx, &dec_info, &decoder);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrImage image = nullptr;
    result = exr_decoder_parse_header(decoder, &image);
    REQUIRE_EQ(result, EXR_ERROR_INVALID_MAGIC);
    REQUIRE(image == nullptr);

    exr_decoder_destroy(decoder);
    exr_context_destroy(ctx);
}

TEST(c_api_parse_invalid_version) {
    ExrContextCreateInfo ctx_info = {};
    ctx_info.api_version = TINYEXR_C_API_VERSION;

    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&ctx_info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);

    /* Invalid EXR data - correct magic but wrong version */
    uint8_t bad_data[] = { 0x76, 0x2f, 0x31, 0x01, 0x03, 0x00, 0x00, 0x00 };

    ExrDataSource source = {};
    result = exr_data_source_from_memory(bad_data, sizeof(bad_data), &source);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrDecoderCreateInfo dec_info = {};
    dec_info.source = source;

    ExrDecoder decoder = nullptr;
    result = exr_decoder_create(ctx, &dec_info, &decoder);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrImage image = nullptr;
    result = exr_decoder_parse_header(decoder, &image);
    REQUIRE_EQ(result, EXR_ERROR_INVALID_VERSION);
    REQUIRE(image == nullptr);

    exr_decoder_destroy(decoder);
    exr_context_destroy(ctx);
}

TEST(c_api_parse_real_file) {
    /* Try to load a real EXR file if available */
    std::vector<uint8_t> data = read_file("../../asakusa.exr");
    if (data.empty()) {
        printf("[SKIPPED - file not found] ");
        return;
    }

    ExrContextCreateInfo ctx_info = {};
    ctx_info.api_version = TINYEXR_C_API_VERSION;

    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&ctx_info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrDataSource source = {};
    result = exr_data_source_from_memory(data.data(), data.size(), &source);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrDecoderCreateInfo dec_info = {};
    dec_info.source = source;

    ExrDecoder decoder = nullptr;
    result = exr_decoder_create(ctx, &dec_info, &decoder);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrImage image = nullptr;
    result = exr_decoder_parse_header(decoder, &image);
    REQUIRE_EQ(result, EXR_SUCCESS);
    REQUIRE(image != nullptr);

    /* Query image info */
    ExrImageInfo info = {};
    result = exr_image_get_info(image, &info);
    REQUIRE_EQ(result, EXR_SUCCESS);

    printf("[%dx%d, %d ch, comp=%d] ", info.width, info.height,
           info.num_channels, info.compression);

    REQUIRE(info.width > 0);
    REQUIRE(info.height > 0);
    REQUIRE(info.num_channels > 0);

    /* Query channel info */
    uint32_t channel_count = 0;
    result = exr_image_get_channel_count(image, &channel_count);
    REQUIRE_EQ(result, EXR_SUCCESS);
    REQUIRE(channel_count > 0);

    for (uint32_t i = 0; i < channel_count; i++) {
        ExrChannelInfo ch_info = {};
        result = exr_image_get_channel(image, i, &ch_info);
        REQUIRE_EQ(result, EXR_SUCCESS);
        REQUIRE(ch_info.name != nullptr);
    }

    /* Query parts */
    uint32_t part_count = 0;
    result = exr_image_get_part_count(image, &part_count);
    REQUIRE_EQ(result, EXR_SUCCESS);
    REQUIRE(part_count >= 1);

    ExrPart part = nullptr;
    result = exr_image_get_part(image, 0, &part);
    REQUIRE_EQ(result, EXR_SUCCESS);
    REQUIRE(part != nullptr);

    ExrPartInfo part_info = {};
    result = exr_part_get_info(part, &part_info);
    REQUIRE_EQ(result, EXR_SUCCESS);
    REQUIRE(part_info.width > 0);
    REQUIRE(part_info.height > 0);

    uint32_t chunk_count = 0;
    result = exr_part_get_chunk_count(part, &chunk_count);
    REQUIRE_EQ(result, EXR_SUCCESS);
    REQUIRE(chunk_count > 0);

    /* Clean up - decoder owns the image, so just destroy decoder */
    exr_part_destroy(part);
    exr_decoder_destroy(decoder);
    exr_context_destroy(ctx);
}

/* ============================================================================
 * Async Data Source for Testing
 * ============================================================================ */

struct AsyncTestSource {
    const uint8_t* data;
    size_t size;
    int fetch_count;
    bool complete_immediately;  /* If false, returns WOULD_BLOCK first time */

    /* Pending fetch state */
    void* pending_dst;
    size_t pending_size;
    ExrFetchComplete pending_callback;
    void* pending_userdata;
};

static ExrResult async_test_fetch(void* userdata, uint64_t offset, uint64_t size,
                                   void* dst, ExrFetchComplete on_complete,
                                   void* complete_userdata) {
    AsyncTestSource* src = (AsyncTestSource*)userdata;
    src->fetch_count++;

    if (offset + size > src->size) {
        return EXR_ERROR_OUT_OF_BOUNDS;
    }

    /* Copy the data */
    memcpy(dst, src->data + offset, size);

    /* Simulate async behavior - first fetch returns WOULD_BLOCK */
    if (!src->complete_immediately && on_complete) {
        /* Store callback for later completion */
        src->pending_dst = dst;
        src->pending_size = size;
        src->pending_callback = on_complete;
        src->pending_userdata = complete_userdata;
        return EXR_WOULD_BLOCK;
    }

    return EXR_SUCCESS;
}

static void async_test_complete_pending(AsyncTestSource* src) {
    if (src->pending_callback) {
        src->pending_callback(src->pending_userdata, EXR_SUCCESS, src->pending_size);
        src->pending_callback = nullptr;
    }
}

TEST(c_api_async_header_parsing) {
    /* Test that async data sources work for header parsing.
     * Header parsing uses local stack buffers, so it falls back to sync
     * even for async sources. This test verifies that works correctly.
     */
    std::vector<uint8_t> data = read_file("../../asakusa.exr");
    if (data.empty()) {
        printf("[SKIPPED - file not found] ");
        return;
    }

    ExrContextCreateInfo ctx_info = {};
    ctx_info.api_version = TINYEXR_C_API_VERSION;

    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&ctx_info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);

    /* Create async data source */
    AsyncTestSource async_src = {};
    async_src.data = data.data();
    async_src.size = data.size();
    async_src.complete_immediately = true;  /* Header parsing uses sync fallback */

    ExrDataSource source = {};
    source.userdata = &async_src;
    source.fetch = async_test_fetch;
    source.total_size = data.size();
    source.flags = EXR_DATA_SOURCE_SEEKABLE | EXR_DATA_SOURCE_ASYNC | EXR_DATA_SOURCE_SIZE_KNOWN;

    ExrDecoderCreateInfo dec_info = {};
    dec_info.source = source;

    ExrDecoder decoder = nullptr;
    result = exr_decoder_create(ctx, &dec_info, &decoder);
    REQUIRE_EQ(result, EXR_SUCCESS);

    /* Header parsing should complete synchronously (sync fallback for local buffers) */
    ExrImage image = nullptr;
    result = exr_decoder_parse_header(decoder, &image);

    printf("[fetches=%d] ", async_src.fetch_count);

    REQUIRE_EQ(result, EXR_SUCCESS);
    REQUIRE(image != nullptr);

    /* Verify parsed header */
    ExrImageInfo info = {};
    result = exr_image_get_info(image, &info);
    REQUIRE_EQ(result, EXR_SUCCESS);
    REQUIRE(info.width > 0);
    REQUIRE(info.height > 0);

    exr_decoder_destroy(decoder);
    exr_context_destroy(ctx);
}

TEST(c_api_async_immediate_completion) {
    /* Test async source that completes immediately (no WOULD_BLOCK) */
    std::vector<uint8_t> data = read_file("../../asakusa.exr");
    if (data.empty()) {
        printf("[SKIPPED - file not found] ");
        return;
    }

    ExrContextCreateInfo ctx_info = {};
    ctx_info.api_version = TINYEXR_C_API_VERSION;

    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&ctx_info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);

    /* Create async data source that completes immediately */
    AsyncTestSource async_src = {};
    async_src.data = data.data();
    async_src.size = data.size();
    async_src.complete_immediately = true;  /* Complete immediately */

    ExrDataSource source = {};
    source.userdata = &async_src;
    source.fetch = async_test_fetch;
    source.total_size = data.size();
    source.flags = EXR_DATA_SOURCE_SEEKABLE | EXR_DATA_SOURCE_ASYNC | EXR_DATA_SOURCE_SIZE_KNOWN;

    ExrDecoderCreateInfo dec_info = {};
    dec_info.source = source;

    ExrDecoder decoder = nullptr;
    result = exr_decoder_create(ctx, &dec_info, &decoder);
    REQUIRE_EQ(result, EXR_SUCCESS);

    /* Parse header - should complete without WOULD_BLOCK */
    ExrImage image = nullptr;
    result = exr_decoder_parse_header(decoder, &image);
    REQUIRE_EQ(result, EXR_SUCCESS);
    REQUIRE(image != nullptr);

    printf("[fetches=%d] ", async_src.fetch_count);

    /* Verify parsed header */
    ExrImageInfo info = {};
    result = exr_image_get_info(image, &info);
    REQUIRE_EQ(result, EXR_SUCCESS);
    REQUIRE(info.width > 0);
    REQUIRE(info.height > 0);

    exr_decoder_destroy(decoder);
    exr_context_destroy(ctx);
}

TEST(c_api_command_buffer_recording) {
    ExrContextCreateInfo ctx_info = {};
    ctx_info.api_version = TINYEXR_C_API_VERSION;

    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&ctx_info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrCommandBufferCreateInfo cmd_info = {};
    cmd_info.decoder = nullptr;  /* Will test recording without decoder */
    cmd_info.max_commands = 64;

    ExrCommandBuffer cmd = nullptr;
    result = exr_command_buffer_create(ctx, &cmd_info, &cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);
    REQUIRE(cmd != nullptr);

    /* Begin recording */
    result = exr_command_buffer_begin(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    /* Can't begin again while recording */
    result = exr_command_buffer_begin(cmd);
    REQUIRE_EQ(result, EXR_ERROR_INVALID_STATE);

    /* End recording */
    result = exr_command_buffer_end(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    /* Can't end again */
    result = exr_command_buffer_end(cmd);
    REQUIRE_EQ(result, EXR_ERROR_INVALID_STATE);

    /* Reset and record again */
    result = exr_command_buffer_reset(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    result = exr_command_buffer_begin(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    result = exr_command_buffer_end(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    exr_command_buffer_destroy(cmd);
    exr_context_destroy(ctx);
}

TEST(c_api_submit_basic) {
    /* Try to load asakusa.exr (ZIP compressed) */
    std::vector<uint8_t> data = read_file("../../asakusa.exr");
    if (data.empty()) {
        printf("[SKIPPED - file not found] ");
        return;
    }

    ExrContextCreateInfo ctx_info = {};
    ctx_info.api_version = TINYEXR_C_API_VERSION;

    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&ctx_info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrDataSource source = {};
    result = exr_data_source_from_memory(data.data(), data.size(), &source);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrDecoderCreateInfo dec_info = {};
    dec_info.source = source;

    ExrDecoder decoder = nullptr;
    result = exr_decoder_create(ctx, &dec_info, &decoder);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrImage image = nullptr;
    result = exr_decoder_parse_header(decoder, &image);
    REQUIRE_EQ(result, EXR_SUCCESS);
    REQUIRE(image != nullptr);

    /* Create command buffer */
    ExrCommandBufferCreateInfo cmd_info = {};
    cmd_info.decoder = decoder;

    ExrCommandBuffer cmd = nullptr;
    result = exr_command_buffer_create(ctx, &cmd_info, &cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    /* Get part info */
    ExrPart part = nullptr;
    result = exr_image_get_part(image, 0, &part);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrPartInfo part_info = {};
    result = exr_part_get_info(part, &part_info);
    REQUIRE_EQ(result, EXR_SUCCESS);

    /* Allocate output buffer */
    size_t buffer_size = (size_t)part_info.width * part_info.height *
                         part_info.num_channels * sizeof(uint16_t);  /* HALF */
    std::vector<uint8_t> output_buffer(buffer_size, 0);

    /* Record full image read command */
    result = exr_command_buffer_begin(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrFullImageRequest full_req = {};
    full_req.part = part;
    full_req.output.data = output_buffer.data();
    full_req.output.size = buffer_size;
    full_req.channels_mask = 0;  /* All channels */
    full_req.output_pixel_type = EXR_PIXEL_HALF;

    result = exr_cmd_request_full_image(cmd, &full_req);
    REQUIRE_EQ(result, EXR_SUCCESS);

    result = exr_command_buffer_end(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    /* Submit - this will fail for ZIP compression (not implemented yet) */
    ExrSubmitInfo submit_info = {};
    ExrCommandBuffer cmd_buffers[] = { cmd };
    submit_info.command_buffer_count = 1;
    submit_info.command_buffers = cmd_buffers;

    result = exr_submit(decoder, &submit_info);
    /* Check result based on compression type */
    if (part_info.compression == EXR_COMPRESSION_NONE ||
        part_info.compression == EXR_COMPRESSION_RLE ||
        part_info.compression == EXR_COMPRESSION_ZIP ||
        part_info.compression == EXR_COMPRESSION_ZIPS) {
        REQUIRE_EQ(result, EXR_SUCCESS);
        printf("[loaded comp=%d] ", part_info.compression);
    } else {
        /* PIZ/PXR24/B44 compressions not implemented yet */
        REQUIRE_EQ(result, EXR_ERROR_UNSUPPORTED_FORMAT);
        printf("[comp=%d not impl] ", part_info.compression);
    }

    /* Clean up - decoder owns the image */
    exr_part_destroy(part);
    exr_command_buffer_destroy(cmd);
    exr_decoder_destroy(decoder);
    exr_context_destroy(ctx);
}

TEST(c_api_verify_zip_vs_v1) {
    /* Compare V3 ZIP decompression output with V1 API */
    std::vector<uint8_t> fileData = read_file("../../asakusa.exr");
    if (fileData.empty()) {
        printf("[SKIPPED - file not found] ");
        return;
    }

    /* Load with V1 API */
    EXRVersion exr_version;
    int ret = ParseEXRVersionFromMemory(&exr_version, fileData.data(), fileData.size());
    REQUIRE_EQ(ret, TINYEXR_SUCCESS);

    EXRHeader header;
    InitEXRHeader(&header);
    const char* err = nullptr;
    ret = ParseEXRHeaderFromMemory(&header, &exr_version, fileData.data(), fileData.size(), &err);
    REQUIRE_EQ(ret, TINYEXR_SUCCESS);

    EXRImage v1_image;
    InitEXRImage(&v1_image);
    ret = LoadEXRImageFromMemory(&v1_image, &header, fileData.data(), fileData.size(), &err);
    REQUIRE_EQ(ret, TINYEXR_SUCCESS);

    /* Load with V3 API */
    ExrContextCreateInfo ctx_info = {};
    ctx_info.api_version = TINYEXR_C_API_VERSION;
    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&ctx_info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrDataSource source = {};
    result = exr_data_source_from_memory(fileData.data(), fileData.size(), &source);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrDecoderCreateInfo dec_info = {};
    dec_info.source = source;
    ExrDecoder decoder = nullptr;
    result = exr_decoder_create(ctx, &dec_info, &decoder);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrImage v3_header = nullptr;
    result = exr_decoder_parse_header(decoder, &v3_header);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrPart part = nullptr;
    result = exr_image_get_part(v3_header, 0, &part);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrPartInfo part_info = {};
    result = exr_part_get_info(part, &part_info);
    REQUIRE_EQ(result, EXR_SUCCESS);

    /* Allocate V3 output buffer */
    size_t buffer_size = (size_t)part_info.width * part_info.height *
                         part_info.num_channels * sizeof(uint16_t);
    std::vector<uint8_t> v3_buffer(buffer_size, 0);

    ExrCommandBufferCreateInfo cmd_info = {};
    cmd_info.decoder = decoder;
    ExrCommandBuffer cmd = nullptr;
    result = exr_command_buffer_create(ctx, &cmd_info, &cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    result = exr_command_buffer_begin(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrFullImageRequest full_req = {};
    full_req.part = part;
    full_req.output.data = v3_buffer.data();
    full_req.output.size = buffer_size;
    full_req.channels_mask = 0;
    full_req.output_pixel_type = EXR_PIXEL_HALF;

    result = exr_cmd_request_full_image(cmd, &full_req);
    REQUIRE_EQ(result, EXR_SUCCESS);

    result = exr_command_buffer_end(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrSubmitInfo submit_info = {};
    ExrCommandBuffer cmd_buffers[] = { cmd };
    submit_info.command_buffer_count = 1;
    submit_info.command_buffers = cmd_buffers;

    result = exr_submit(decoder, &submit_info);
    REQUIRE_EQ(result, EXR_SUCCESS);

    /* Compare pixel data */
    /* V1 stores data per-channel in images[], V3 stores per-line (channel sequential) */
    /* EXR format: for each line: [ch0 data][ch1 data][ch2 data][ch3 data] */
    int width = part_info.width;
    int height = part_info.height;
    int num_channels = part_info.num_channels;

    int mismatch_count = 0;
    int total_pixels = width * height * num_channels;

    /* V3 data is per-line, channel sequential */
    const uint16_t* v3_data = reinterpret_cast<const uint16_t*>(v3_buffer.data());

    for (int y = 0; y < height && mismatch_count < 10; y++) {
        for (int c = 0; c < num_channels && mismatch_count < 10; c++) {
            for (int x = 0; x < width && mismatch_count < 10; x++) {
                /* V1: images[channel][(y * width + x)] */
                const uint16_t* v1_channel = reinterpret_cast<const uint16_t*>(v1_image.images[c]);
                uint16_t v1_val = v1_channel[y * width + x];

                /* V3: per-line [y * (width * num_channels) + c * width + x] */
                uint16_t v3_val = v3_data[y * (width * num_channels) + c * width + x];

                if (v1_val != v3_val) {
                    mismatch_count++;
                    printf("\n    Mismatch at (%d,%d) ch%d: V1=%04x V3=%04x", x, y, c, v1_val, v3_val);
                }
            }
        }
    }

    /* Clean up V3 - decoder owns the image */
    exr_part_destroy(part);
    exr_command_buffer_destroy(cmd);
    exr_decoder_destroy(decoder);
    exr_context_destroy(ctx);

    /* Clean up V1 */
    FreeEXRImage(&v1_image);
    FreeEXRHeader(&header);

    if (mismatch_count > 0) {
        printf("\n    Total mismatches: %d/%d ", mismatch_count, total_pixels);
    }
    REQUIRE_EQ(mismatch_count, 0);
    printf("[%dx%dx%d verified] ", width, height, num_channels);
}

TEST(c_api_verify_piz_vs_v1) {
    /* PIZ compression test - uses V2 optimized Huffman decoder */
    std::vector<uint8_t> fileData = read_file("../../test/unit/regression/issue-160-piz-decode.exr");
    if (fileData.empty()) {
        printf("[SKIPPED - PIZ test file not found] ");
        return;
    }

    /* Load with V1 API */
    EXRVersion exr_version;
    int ret = ParseEXRVersionFromMemory(&exr_version, fileData.data(), fileData.size());
    REQUIRE_EQ(ret, TINYEXR_SUCCESS);

    EXRHeader header;
    InitEXRHeader(&header);
    const char* err = nullptr;
    ret = ParseEXRHeaderFromMemory(&header, &exr_version, fileData.data(), fileData.size(), &err);
    REQUIRE_EQ(ret, TINYEXR_SUCCESS);

    /* Check compression is PIZ */
    REQUIRE_EQ(header.compression_type, TINYEXR_COMPRESSIONTYPE_PIZ);

    EXRImage v1_image;
    InitEXRImage(&v1_image);
    ret = LoadEXRImageFromMemory(&v1_image, &header, fileData.data(), fileData.size(), &err);
    REQUIRE_EQ(ret, TINYEXR_SUCCESS);

    /* Load with V3 API */
    ExrContextCreateInfo ctx_info = {};
    ctx_info.api_version = TINYEXR_C_API_VERSION;
    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&ctx_info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrDataSource source = {};
    result = exr_data_source_from_memory(fileData.data(), fileData.size(), &source);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrDecoderCreateInfo dec_info = {};
    dec_info.source = source;
    ExrDecoder decoder = nullptr;
    result = exr_decoder_create(ctx, &dec_info, &decoder);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrImage v3_header = nullptr;
    result = exr_decoder_parse_header(decoder, &v3_header);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrPart part = nullptr;
    result = exr_image_get_part(v3_header, 0, &part);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrPartInfo part_info = {};
    result = exr_part_get_info(part, &part_info);
    REQUIRE_EQ(result, EXR_SUCCESS);

    /* Verify compression type is PIZ */
    REQUIRE_EQ(part_info.compression, EXR_COMPRESSION_PIZ);

    /* Get actual pixel type from first channel */
    ExrChannelInfo ch_info = {};
    result = exr_part_get_channel(part, 0, &ch_info);
    REQUIRE_EQ(result, EXR_SUCCESS);

    int bytes_per_pixel = (ch_info.pixel_type == EXR_PIXEL_HALF) ? 2 : 4;
    printf("[ptype=%d] ", ch_info.pixel_type);

    /* Allocate V3 output buffer with correct size for actual pixel type */
    size_t buffer_size = (size_t)part_info.width * part_info.height *
                         part_info.num_channels * bytes_per_pixel;
    std::vector<uint8_t> v3_buffer(buffer_size, 0);

    ExrCommandBufferCreateInfo cmd_info = {};
    cmd_info.decoder = decoder;
    ExrCommandBuffer cmd = nullptr;
    result = exr_command_buffer_create(ctx, &cmd_info, &cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    result = exr_command_buffer_begin(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrFullImageRequest full_req = {};
    full_req.part = part;
    full_req.output.data = v3_buffer.data();
    full_req.output.size = buffer_size;
    full_req.channels_mask = 0;
    full_req.output_pixel_type = ch_info.pixel_type;  /* Use actual pixel type */

    result = exr_cmd_request_full_image(cmd, &full_req);
    REQUIRE_EQ(result, EXR_SUCCESS);

    result = exr_command_buffer_end(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    printf("[submit...] ");
    fflush(stdout);

    ExrSubmitInfo submit_info = {};
    ExrCommandBuffer cmd_buffers[] = { cmd };
    submit_info.command_buffer_count = 1;
    submit_info.command_buffers = cmd_buffers;

    result = exr_submit(decoder, &submit_info);
    printf("[result=%d] ", (int)result);
    fflush(stdout);
    REQUIRE_EQ(result, EXR_SUCCESS);

    printf("[compare...] ");
    fflush(stdout);

    /* Compare pixel data */
    int width = part_info.width;
    int height = part_info.height;
    int num_channels = part_info.num_channels;

    int mismatch_count = 0;
    int total_pixels = width * height * num_channels;

    /* Compare based on actual pixel type */
    if (ch_info.pixel_type == EXR_PIXEL_HALF) {
        const uint16_t* v3_data = reinterpret_cast<const uint16_t*>(v3_buffer.data());
        for (int y = 0; y < height && mismatch_count < 10; y++) {
            for (int c = 0; c < num_channels && mismatch_count < 10; c++) {
                for (int x = 0; x < width && mismatch_count < 10; x++) {
                    const uint16_t* v1_channel = reinterpret_cast<const uint16_t*>(v1_image.images[c]);
                    uint16_t v1_val = v1_channel[y * width + x];
                    uint16_t v3_val = v3_data[y * (width * num_channels) + c * width + x];
                    if (v1_val != v3_val) {
                        mismatch_count++;
                        printf("\n    Mismatch at (%d,%d) ch%d: V1=%04x V3=%04x", x, y, c, v1_val, v3_val);
                    }
                }
            }
        }
    } else {
        /* FLOAT or UINT - compare as 32-bit values */
        const uint32_t* v3_data = reinterpret_cast<const uint32_t*>(v3_buffer.data());
        for (int y = 0; y < height && mismatch_count < 10; y++) {
            for (int c = 0; c < num_channels && mismatch_count < 10; c++) {
                for (int x = 0; x < width && mismatch_count < 10; x++) {
                    const uint32_t* v1_channel = reinterpret_cast<const uint32_t*>(v1_image.images[c]);
                    uint32_t v1_val = v1_channel[y * width + x];
                    uint32_t v3_val = v3_data[y * (width * num_channels) + c * width + x];
                    if (v1_val != v3_val) {
                        mismatch_count++;
                        printf("\n    Mismatch at (%d,%d) ch%d: V1=%08x V3=%08x", x, y, c, v1_val, v3_val);
                    }
                }
            }
        }
    }

    /* Clean up V3 - decoder owns the image */
    exr_part_destroy(part);
    exr_command_buffer_destroy(cmd);
    exr_decoder_destroy(decoder);
    exr_context_destroy(ctx);

    /* Clean up V1 */
    FreeEXRImage(&v1_image);
    FreeEXRHeader(&header);

    if (mismatch_count > 0) {
        printf("\n    Total mismatches: %d/%d ", mismatch_count, total_pixels);
    }
    REQUIRE_EQ(mismatch_count, 0);
    printf("[%dx%dx%d verified] ", width, height, num_channels);
}

/* Test PXR24 decompression vs V1 */
TEST(c_api_verify_pxr24_vs_v1) {
    /* PXR24 compression test */
    std::vector<uint8_t> fileData = read_file("../../test/unit/regression/pxr24_test.exr");
    if (fileData.empty()) {
        printf("[SKIPPED - PXR24 test file not found] ");
        return;
    }

    /* Load with V1 API */
    EXRVersion exr_version;
    int ret = ParseEXRVersionFromMemory(&exr_version, fileData.data(), fileData.size());
    REQUIRE_EQ(ret, TINYEXR_SUCCESS);

    EXRHeader header;
    InitEXRHeader(&header);
    const char* err = nullptr;
    ret = ParseEXRHeaderFromMemory(&header, &exr_version, fileData.data(), fileData.size(), &err);
    REQUIRE_EQ(ret, TINYEXR_SUCCESS);

    /* Check compression is PXR24 */
    REQUIRE_EQ(header.compression_type, TINYEXR_COMPRESSIONTYPE_PXR24);

    EXRImage v1_image;
    InitEXRImage(&v1_image);
    ret = LoadEXRImageFromMemory(&v1_image, &header, fileData.data(), fileData.size(), &err);
    REQUIRE_EQ(ret, TINYEXR_SUCCESS);

    /* Load with V3 API */
    ExrContextCreateInfo ctx_info = {};
    ctx_info.api_version = TINYEXR_C_API_VERSION;
    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&ctx_info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrDataSource source = {};
    result = exr_data_source_from_memory(fileData.data(), fileData.size(), &source);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrDecoderCreateInfo dec_info = {};
    dec_info.source = source;
    ExrDecoder decoder = nullptr;
    result = exr_decoder_create(ctx, &dec_info, &decoder);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrImage v3_header = nullptr;
    result = exr_decoder_parse_header(decoder, &v3_header);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrPart part = nullptr;
    result = exr_image_get_part(v3_header, 0, &part);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrPartInfo part_info = {};
    result = exr_part_get_info(part, &part_info);
    REQUIRE_EQ(result, EXR_SUCCESS);

    /* Verify compression type is PXR24 */
    REQUIRE_EQ(part_info.compression, EXR_COMPRESSION_PXR24);

    /* Get actual pixel type from first channel */
    ExrChannelInfo ch_info = {};
    result = exr_part_get_channel(part, 0, &ch_info);
    REQUIRE_EQ(result, EXR_SUCCESS);

    int bytes_per_pixel = (ch_info.pixel_type == EXR_PIXEL_HALF) ? 2 : 4;
    printf("[ptype=%d] ", ch_info.pixel_type);

    /* Allocate V3 output buffer */
    size_t buffer_size = (size_t)part_info.width * part_info.height *
                         part_info.num_channels * bytes_per_pixel;
    std::vector<uint8_t> v3_buffer(buffer_size, 0);

    ExrCommandBufferCreateInfo cmd_info = {};
    cmd_info.decoder = decoder;
    ExrCommandBuffer cmd = nullptr;
    result = exr_command_buffer_create(ctx, &cmd_info, &cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    result = exr_command_buffer_begin(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrFullImageRequest full_req = {};
    full_req.part = part;
    full_req.output.data = v3_buffer.data();
    full_req.output.size = buffer_size;
    full_req.channels_mask = 0;
    full_req.output_pixel_type = ch_info.pixel_type;

    result = exr_cmd_request_full_image(cmd, &full_req);
    REQUIRE_EQ(result, EXR_SUCCESS);

    result = exr_command_buffer_end(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    printf("[submit...] ");
    fflush(stdout);

    ExrSubmitInfo submit_info = {};
    ExrCommandBuffer cmd_buffers[] = { cmd };
    submit_info.command_buffer_count = 1;
    submit_info.command_buffers = cmd_buffers;

    result = exr_submit(decoder, &submit_info);
    printf("[result=%d] ", (int)result);
    fflush(stdout);
    REQUIRE_EQ(result, EXR_SUCCESS);

    printf("[compare...] ");
    fflush(stdout);

    /* Compare pixel data */
    int width = part_info.width;
    int height = part_info.height;
    int num_channels = part_info.num_channels;

    int mismatch_count = 0;
    int total_pixels = width * height * num_channels;

    /* Compare based on actual pixel type (HALF for this test file) */
    if (ch_info.pixel_type == EXR_PIXEL_HALF) {
        const uint16_t* v3_data = reinterpret_cast<const uint16_t*>(v3_buffer.data());
        for (int y = 0; y < height && mismatch_count < 10; y++) {
            for (int c = 0; c < num_channels && mismatch_count < 10; c++) {
                for (int x = 0; x < width && mismatch_count < 10; x++) {
                    const uint16_t* v1_channel = reinterpret_cast<const uint16_t*>(v1_image.images[c]);
                    uint16_t v1_val = v1_channel[y * width + x];
                    uint16_t v3_val = v3_data[y * (width * num_channels) + c * width + x];
                    if (v1_val != v3_val) {
                        mismatch_count++;
                        printf("\n    Mismatch at (%d,%d) ch%d: V1=%04x V3=%04x", x, y, c, v1_val, v3_val);
                    }
                }
            }
        }
    } else {
        /* FLOAT or UINT */
        const uint32_t* v3_data = reinterpret_cast<const uint32_t*>(v3_buffer.data());
        for (int y = 0; y < height && mismatch_count < 10; y++) {
            for (int c = 0; c < num_channels && mismatch_count < 10; c++) {
                for (int x = 0; x < width && mismatch_count < 10; x++) {
                    const uint32_t* v1_channel = reinterpret_cast<const uint32_t*>(v1_image.images[c]);
                    uint32_t v1_val = v1_channel[y * width + x];
                    uint32_t v3_val = v3_data[y * (width * num_channels) + c * width + x];
                    if (v1_val != v3_val) {
                        mismatch_count++;
                        printf("\n    Mismatch at (%d,%d) ch%d: V1=%08x V3=%08x", x, y, c, v1_val, v3_val);
                    }
                }
            }
        }
    }

    /* Clean up V3 - decoder owns the image */
    exr_part_destroy(part);
    exr_command_buffer_destroy(cmd);
    exr_decoder_destroy(decoder);
    exr_context_destroy(ctx);

    /* Clean up V1 */
    FreeEXRImage(&v1_image);
    FreeEXRHeader(&header);

    if (mismatch_count > 0) {
        printf("\n    Total mismatches: %d/%d ", mismatch_count, total_pixels);
    }
    REQUIRE_EQ(mismatch_count, 0);
    printf("[%dx%dx%d verified] ", width, height, num_channels);
}

/* Test B44 decompression vs V1 */
TEST(c_api_verify_b44_vs_v1) {
    /* B44 compression test */
    std::vector<uint8_t> fileData = read_file("../../test/unit/regression/b44_test.exr");
    if (fileData.empty()) {
        printf("[SKIPPED - B44 test file not found] ");
        return;
    }

    /* Load with V1 API */
    EXRVersion exr_version;
    int ret = ParseEXRVersionFromMemory(&exr_version, fileData.data(), fileData.size());
    REQUIRE_EQ(ret, TINYEXR_SUCCESS);

    EXRHeader header;
    InitEXRHeader(&header);
    const char* err = nullptr;
    ret = ParseEXRHeaderFromMemory(&header, &exr_version, fileData.data(), fileData.size(), &err);
    REQUIRE_EQ(ret, TINYEXR_SUCCESS);

    /* Check compression is B44 */
    REQUIRE_EQ(header.compression_type, TINYEXR_COMPRESSIONTYPE_B44);

    EXRImage v1_image;
    InitEXRImage(&v1_image);
    ret = LoadEXRImageFromMemory(&v1_image, &header, fileData.data(), fileData.size(), &err);
    REQUIRE_EQ(ret, TINYEXR_SUCCESS);

    /* Load with V3 API */
    ExrContextCreateInfo ctx_info = {};
    ctx_info.api_version = TINYEXR_C_API_VERSION;
    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&ctx_info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrDataSource source = {};
    result = exr_data_source_from_memory(fileData.data(), fileData.size(), &source);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrDecoderCreateInfo dec_info = {};
    dec_info.source = source;
    ExrDecoder decoder = nullptr;
    result = exr_decoder_create(ctx, &dec_info, &decoder);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrImage v3_header = nullptr;
    result = exr_decoder_parse_header(decoder, &v3_header);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrPart part = nullptr;
    result = exr_image_get_part(v3_header, 0, &part);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrPartInfo part_info = {};
    result = exr_part_get_info(part, &part_info);
    REQUIRE_EQ(result, EXR_SUCCESS);

    /* Verify compression type is B44 */
    REQUIRE_EQ(part_info.compression, EXR_COMPRESSION_B44);

    /* Get actual pixel type from first channel */
    ExrChannelInfo ch_info = {};
    result = exr_part_get_channel(part, 0, &ch_info);
    REQUIRE_EQ(result, EXR_SUCCESS);

    int bytes_per_pixel = (ch_info.pixel_type == EXR_PIXEL_HALF) ? 2 : 4;
    printf("[ptype=%d] ", ch_info.pixel_type);

    /* Allocate V3 output buffer */
    size_t buffer_size = (size_t)part_info.width * part_info.height *
                         part_info.num_channels * bytes_per_pixel;
    std::vector<uint8_t> v3_buffer(buffer_size, 0);

    ExrCommandBufferCreateInfo cmd_info = {};
    cmd_info.decoder = decoder;
    ExrCommandBuffer cmd = nullptr;
    result = exr_command_buffer_create(ctx, &cmd_info, &cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    result = exr_command_buffer_begin(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrFullImageRequest full_req = {};
    full_req.part = part;
    full_req.output.data = v3_buffer.data();
    full_req.output.size = buffer_size;
    full_req.channels_mask = 0;
    full_req.output_pixel_type = ch_info.pixel_type;

    result = exr_cmd_request_full_image(cmd, &full_req);
    REQUIRE_EQ(result, EXR_SUCCESS);

    result = exr_command_buffer_end(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    printf("[submit...] ");
    fflush(stdout);

    ExrSubmitInfo submit_info = {};
    ExrCommandBuffer cmd_buffers[] = { cmd };
    submit_info.command_buffer_count = 1;
    submit_info.command_buffers = cmd_buffers;

    result = exr_submit(decoder, &submit_info);
    printf("[result=%d] ", (int)result);
    fflush(stdout);
    REQUIRE_EQ(result, EXR_SUCCESS);

    printf("[compare...] ");
    fflush(stdout);

    /* Compare pixel data */
    int width = part_info.width;
    int height = part_info.height;
    int num_channels = part_info.num_channels;

    int mismatch_count = 0;
    int total_pixels = width * height * num_channels;

    /* Compare based on actual pixel type (HALF for B44) */
    const uint16_t* v3_data = reinterpret_cast<const uint16_t*>(v3_buffer.data());
    for (int y = 0; y < height && mismatch_count < 10; y++) {
        for (int c = 0; c < num_channels && mismatch_count < 10; c++) {
            for (int x = 0; x < width && mismatch_count < 10; x++) {
                const uint16_t* v1_channel = reinterpret_cast<const uint16_t*>(v1_image.images[c]);
                uint16_t v1_val = v1_channel[y * width + x];
                uint16_t v3_val = v3_data[y * (width * num_channels) + c * width + x];
                if (v1_val != v3_val) {
                    mismatch_count++;
                    printf("\n    Mismatch at (%d,%d) ch%d: V1=%04x V3=%04x", x, y, c, v1_val, v3_val);
                }
            }
        }
    }

    /* Clean up V3 - decoder owns the image */
    exr_part_destroy(part);
    exr_command_buffer_destroy(cmd);
    exr_decoder_destroy(decoder);
    exr_context_destroy(ctx);

    /* Clean up V1 */
    FreeEXRImage(&v1_image);
    FreeEXRHeader(&header);

    if (mismatch_count > 0) {
        printf("\n    Total mismatches: %d/%d ", mismatch_count, total_pixels);
    }
    REQUIRE_EQ(mismatch_count, 0);
    printf("[%dx%dx%d verified] ", width, height, num_channels);
}

/* Test tiled image reading */
TEST(c_api_verify_tiled) {
    /* Try to find a tiled EXR file */
    const char* tiled_paths[] = {
        "../../openexr-images/Tiles/GoldenGate.exr",
        "../../openexr-images/Tiles/Spirals.exr",
        "../../openexr-images/Tiles/Ocean.exr",
        nullptr
    };

    std::vector<uint8_t> fileData;
    const char* used_path = nullptr;
    for (int i = 0; tiled_paths[i]; i++) {
        fileData = read_file(tiled_paths[i]);
        if (!fileData.empty()) {
            used_path = tiled_paths[i];
            break;
        }
    }

    if (fileData.empty()) {
        printf("[SKIPPED - no tiled test file found] ");
        return;
    }

    /* Parse with V3 API */
    ExrContextCreateInfo ctx_info = {};
    ctx_info.api_version = TINYEXR_C_API_VERSION;
    ExrContext ctx = nullptr;
    ExrResult result = exr_context_create(&ctx_info, &ctx);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrDataSource source = {};
    result = exr_data_source_from_memory(fileData.data(), fileData.size(), &source);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrDecoderCreateInfo dec_info = {};
    dec_info.source = source;
    ExrDecoder decoder = nullptr;
    result = exr_decoder_create(ctx, &dec_info, &decoder);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrImage v3_header = nullptr;
    result = exr_decoder_parse_header(decoder, &v3_header);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrPart part = nullptr;
    result = exr_image_get_part(v3_header, 0, &part);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrPartInfo part_info = {};
    result = exr_part_get_info(part, &part_info);
    REQUIRE_EQ(result, EXR_SUCCESS);

    /* Verify this is a tiled image */
    if (part_info.part_type != EXR_PART_TILED) {
        printf("[SKIPPED - not a tiled image] ");
        exr_part_destroy(part);
        exr_decoder_destroy(decoder);
        exr_context_destroy(ctx);
        return;
    }

    printf("[tiled %dx%d ch=%d] ", part_info.width, part_info.height,
           part_info.num_channels);

    /* Get actual pixel type from first channel */
    ExrChannelInfo ch_info = {};
    result = exr_part_get_channel(part, 0, &ch_info);
    REQUIRE_EQ(result, EXR_SUCCESS);

    int bytes_per_pixel = (ch_info.pixel_type == EXR_PIXEL_HALF) ? 2 : 4;

    /* Allocate V3 output buffer */
    size_t buffer_size = (size_t)part_info.width * part_info.height *
                         part_info.num_channels * bytes_per_pixel;
    std::vector<uint8_t> v3_buffer(buffer_size, 0);

    ExrCommandBufferCreateInfo cmd_info = {};
    cmd_info.decoder = decoder;
    ExrCommandBuffer cmd = nullptr;
    result = exr_command_buffer_create(ctx, &cmd_info, &cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    result = exr_command_buffer_begin(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrFullImageRequest full_req = {};
    full_req.part = part;
    full_req.output.data = v3_buffer.data();
    full_req.output.size = buffer_size;
    full_req.channels_mask = 0;
    full_req.output_pixel_type = ch_info.pixel_type;

    result = exr_cmd_request_full_image(cmd, &full_req);
    REQUIRE_EQ(result, EXR_SUCCESS);

    result = exr_command_buffer_end(cmd);
    REQUIRE_EQ(result, EXR_SUCCESS);

    ExrSubmitInfo submit_info = {};
    ExrCommandBuffer cmd_buffers[] = { cmd };
    submit_info.command_buffer_count = 1;
    submit_info.command_buffers = cmd_buffers;

    result = exr_submit(decoder, &submit_info);
    REQUIRE_EQ(result, EXR_SUCCESS);

    /* Verify data is non-zero (basic sanity check) */
    bool has_non_zero = false;
    for (size_t i = 0; i < v3_buffer.size() && !has_non_zero; i++) {
        if (v3_buffer[i] != 0) has_non_zero = true;
    }
    REQUIRE_EQ(has_non_zero, true);

    /* Clean up */
    exr_part_destroy(part);
    exr_command_buffer_destroy(cmd);
    exr_decoder_destroy(decoder);
    exr_context_destroy(ctx);

    printf("[loaded OK] ");
}

/* ============================================================================
 * Main
 * ============================================================================ */

int main() {
    printf("TinyEXR V3 API Tests\n");
    printf("====================\n\n");

    printf("[C API Tests]\n");
    run_test_c_api_version();
    run_test_c_api_version_string();
    run_test_c_api_result_to_string();
    run_test_c_api_simd_info();
    run_test_c_api_context_create_destroy();
    run_test_c_api_context_invalid_version();
    run_test_c_api_context_ref_counting();
    run_test_c_api_error_handling();
    run_test_c_api_memory_pool();
    run_test_c_api_data_source_from_memory();
    run_test_c_api_decoder_create_destroy();
    run_test_c_api_fence_create_destroy();
    run_test_c_api_command_buffer();
    run_test_c_api_half_conversion();

    printf("\n[Header Parsing Tests]\n");
    run_test_c_api_parse_invalid_magic();
    run_test_c_api_parse_invalid_version();
    run_test_c_api_parse_real_file();

    printf("\n[Async Parsing Tests]\n");
    run_test_c_api_async_header_parsing();
    run_test_c_api_async_immediate_completion();

    printf("\n[Command Buffer Tests]\n");
    run_test_c_api_command_buffer_recording();
    run_test_c_api_submit_basic();
    run_test_c_api_verify_zip_vs_v1();
    run_test_c_api_verify_piz_vs_v1();
    run_test_c_api_verify_pxr24_vs_v1();
    run_test_c_api_verify_b44_vs_v1();
    run_test_c_api_verify_tiled();

    printf("\n[C++ Wrapper Tests]\n");
    run_test_cpp_version_string();
    run_test_cpp_simd_info();
    run_test_cpp_context_create();
    run_test_cpp_context_with_options();
    run_test_cpp_result_ok();
    run_test_cpp_result_error();
    run_test_cpp_result_value_or();
    run_test_cpp_result_map();
    run_test_cpp_result_map_error();
    run_test_cpp_result_and_then();
    run_test_cpp_result_void();
    run_test_cpp_error_info();
    run_test_cpp_decoder_from_memory();
    run_test_cpp_tile_coord();

    printf("\n====================\n");
    printf("Results: %d passed, %d failed\n", g_tests_passed, g_tests_failed);

    return g_tests_failed > 0 ? 1 : 0;
}

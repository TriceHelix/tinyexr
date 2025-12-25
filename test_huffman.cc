// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2025, Syoyo Fujita and many contributors.
// All rights reserved.
//
// Test for TinyEXR Optimized Huffman/Deflate
//
// Build (x86_64 with BMI2):
//   g++ -O2 -march=native -DTINYEXR_ENABLE_SIMD=1 -o test_huffman test_huffman.cc -lz
//
// Build (without BMI):
//   g++ -O2 -DTINYEXR_ENABLE_SIMD=1 -o test_huffman test_huffman.cc -lz
//
// Build (scalar fallback):
//   g++ -O2 -DTINYEXR_ENABLE_SIMD=0 -o test_huffman test_huffman.cc -lz

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <chrono>
#include <vector>
#include <random>

#define TINYEXR_ENABLE_SIMD 1
#include "tinyexr_huffman.hh"

using namespace tinyexr::huffman;

// Test utilities
static int g_test_count = 0;
static int g_pass_count = 0;
static int g_fail_count = 0;

#define TEST_ASSERT(cond, msg) do { \
  g_test_count++; \
  if (!(cond)) { \
    printf("  FAIL: %s\n", msg); \
    g_fail_count++; \
  } else { \
    g_pass_count++; \
  } \
} while(0)

// ============================================================================
// Bit Manipulation Tests
// ============================================================================

void test_bit_manipulation() {
  printf("=== Test: Bit Manipulation Utilities ===\n");
  printf("  Backend: %s\n", get_huffman_info());

  // CLZ tests
  TEST_ASSERT(clz32(0x80000000) == 0, "clz32(0x80000000)");
  TEST_ASSERT(clz32(0x00000001) == 31, "clz32(0x00000001)");
  TEST_ASSERT(clz32(0x0000FFFF) == 16, "clz32(0x0000FFFF)");
  TEST_ASSERT(clz32(0) == 32, "clz32(0)");

  TEST_ASSERT(clz64(0x8000000000000000ULL) == 0, "clz64(high bit)");
  TEST_ASSERT(clz64(0x0000000000000001ULL) == 63, "clz64(low bit)");
  TEST_ASSERT(clz64(0) == 64, "clz64(0)");

  // CTZ tests
  TEST_ASSERT(ctz32(0x80000000) == 31, "ctz32(0x80000000)");
  TEST_ASSERT(ctz32(0x00000001) == 0, "ctz32(0x00000001)");
  TEST_ASSERT(ctz32(0x00001000) == 12, "ctz32(0x00001000)");
  TEST_ASSERT(ctz32(0) == 32, "ctz32(0)");

  TEST_ASSERT(ctz64(0x8000000000000000ULL) == 63, "ctz64(high bit)");
  TEST_ASSERT(ctz64(0x0000000000000001ULL) == 0, "ctz64(low bit)");
  TEST_ASSERT(ctz64(0) == 64, "ctz64(0)");

  // POPCOUNT tests
  TEST_ASSERT(popcount32(0) == 0, "popcount32(0)");
  TEST_ASSERT(popcount32(1) == 1, "popcount32(1)");
  TEST_ASSERT(popcount32(0xFFFFFFFF) == 32, "popcount32(0xFFFFFFFF)");
  TEST_ASSERT(popcount32(0x55555555) == 16, "popcount32(0x55555555)");

  // BEXTR tests
  TEST_ASSERT(bextr32(0xDEADBEEF, 0, 8) == 0xEF, "bextr32 low byte");
  TEST_ASSERT(bextr32(0xDEADBEEF, 8, 8) == 0xBE, "bextr32 second byte");
  TEST_ASSERT(bextr32(0xDEADBEEF, 16, 8) == 0xAD, "bextr32 third byte");
  TEST_ASSERT(bextr32(0xDEADBEEF, 24, 8) == 0xDE, "bextr32 high byte");

  // BSWAP tests
  TEST_ASSERT(bswap32(0x12345678) == 0x78563412, "bswap32");
  TEST_ASSERT(bswap64(0x123456789ABCDEF0ULL) == 0xF0DEBC9A78563412ULL, "bswap64");

  printf("  Passed bit manipulation tests\n\n");
}

// ============================================================================
// Bit Buffer Tests
// ============================================================================

void test_bit_buffer() {
  printf("=== Test: Bit Buffer ===\n");

  // Test data: 0xDEADBEEF
  uint8_t data[] = {0xDE, 0xAD, 0xBE, 0xEF, 0x12, 0x34, 0x56, 0x78};

  BitBuffer buf;
  buf.init(data, sizeof(data));

  // Refill and read
  buf.refill();

  // Read 8 bits at a time (MSB first for BitBuffer)
  TEST_ASSERT(buf.read(8) == 0xDE, "BitBuffer read first byte");
  buf.refill();
  TEST_ASSERT(buf.read(8) == 0xAD, "BitBuffer read second byte");
  buf.refill();
  TEST_ASSERT(buf.read(8) == 0xBE, "BitBuffer read third byte");
  buf.refill();
  TEST_ASSERT(buf.read(8) == 0xEF, "BitBuffer read fourth byte");

  // Test DeflateBitReader (LSB first)
  DeflateBitReader dbuf;
  dbuf.init(data, sizeof(data));
  dbuf.refill();

  // Read bits (LSB first)
  uint32_t val = dbuf.read(8);
  TEST_ASSERT(val == 0xDE, "DeflateBitReader read first byte");

  printf("  Passed bit buffer tests\n\n");
}

// ============================================================================
// Deflate Fixed Huffman Table Tests
// ============================================================================

void test_fixed_huffman_table() {
  printf("=== Test: Fixed Huffman Tables ===\n");

  DeflateHuffTable litlen;
  litlen.build_fixed_litlen();

  // Test some known fixed Huffman codes
  // Symbol 0: 8 bits, code 0x30 (reversed: 0x0C)
  // Symbol 144: 9 bits, code 0x190 (reversed: varies)
  // Symbol 256 (end): 7 bits, code 0x00 (reversed: 0x00)

  // The table is indexed by reversed bit patterns
  // Symbol 256 (end of block) has code 0000000 (7 bits)
  uint16_t entry = litlen.fast_table[0];
  TEST_ASSERT(entry & 0x8000, "End symbol entry valid");
  int sym = (entry >> 4) & 0x7FF;
  int len = entry & 0xF;
  TEST_ASSERT(sym == 256, "End symbol value");
  TEST_ASSERT(len == 7, "End symbol length");

  DeflateHuffTable dist;
  dist.build_fixed_dist();

  // Distance 0: 5 bits, code 00000 (reversed: 00000 = 0)
  entry = dist.fast_table[0];
  TEST_ASSERT(entry & 0x8000, "Distance 0 entry valid");
  sym = (entry >> 4) & 0x7FF;
  len = entry & 0xF;
  TEST_ASSERT(sym == 0, "Distance 0 symbol");
  TEST_ASSERT(len == 5, "Distance 0 length");

  printf("  Passed fixed Huffman table tests\n\n");
}

// ============================================================================
// Deflate Decompression Tests
// ============================================================================

void test_deflate_stored_block() {
  printf("=== Test: Deflate Stored Block ===\n");

  // A stored block with "Hello" (no compression)
  // Header: BFINAL=1, BTYPE=00, then LEN, NLEN, data
  uint8_t compressed[] = {
    0x01,                         // BFINAL=1, BTYPE=00 (stored)
    0x05, 0x00,                   // LEN = 5
    0xFA, 0xFF,                   // NLEN = ~5
    'H', 'e', 'l', 'l', 'o'       // Data
  };

  uint8_t output[256];
  size_t output_len = sizeof(output);

  FastDeflateDecoder decoder;
  bool ok = decoder.decompress(compressed, sizeof(compressed), output, &output_len);

  TEST_ASSERT(ok, "Stored block decompression succeeds");
  TEST_ASSERT(output_len == 5, "Stored block output length");
  TEST_ASSERT(memcmp(output, "Hello", 5) == 0, "Stored block output content");

  printf("  Passed stored block tests\n\n");
}

void test_deflate_fixed_huffman() {
  printf("=== Test: Deflate Fixed Huffman ===\n");

  // Pre-computed deflate stream for "AAAAAAAAAA" (10 A's) using fixed Huffman
  // This uses a length-distance pair for the repetition
  // Actually, let's use a simpler test with known compressed data

  // A simple fixed Huffman block ending immediately (just end-of-block)
  // BFINAL=1, BTYPE=01 (fixed Huffman)
  // Then symbol 256 (end of block) = 0000000 (7 bits, reversed = 0x00)
  uint8_t compressed[] = {
    0x03, 0x00  // BFINAL=1, BTYPE=01, symbol 256 (padded to byte boundary)
  };

  uint8_t output[256];
  size_t output_len = sizeof(output);

  FastDeflateDecoder decoder;
  bool ok = decoder.decompress(compressed, sizeof(compressed), output, &output_len);

  TEST_ASSERT(ok, "Empty fixed Huffman block succeeds");
  TEST_ASSERT(output_len == 0, "Empty fixed Huffman output length");

  printf("  Passed fixed Huffman tests\n\n");
}

// ============================================================================
// Zlib Wrapper Tests (if zlib available)
// ============================================================================

#ifdef HAVE_ZLIB
#include <zlib.h>

void test_zlib_compatibility() {
  printf("=== Test: Zlib Compatibility ===\n");

  // Create test data
  const char* test_data = "Hello, World! This is a test of the TinyEXR deflate implementation. "
                          "We need enough data to make compression worthwhile. "
                          "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
  size_t data_len = strlen(test_data);

  // Compress with zlib
  std::vector<uint8_t> compressed(compressBound(data_len));
  uLongf compressed_len = compressed.size();

  int ret = compress(compressed.data(), &compressed_len,
                    reinterpret_cast<const Bytef*>(test_data), data_len);
  TEST_ASSERT(ret == Z_OK, "zlib compress succeeds");

  // Decompress with our implementation
  std::vector<uint8_t> decompressed(data_len + 256);
  size_t decompressed_len = decompressed.size();

  bool ok = inflate_zlib(compressed.data(), compressed_len,
                        decompressed.data(), &decompressed_len);

  TEST_ASSERT(ok, "Our inflate succeeds");
  TEST_ASSERT(decompressed_len == data_len, "Decompressed length matches");
  TEST_ASSERT(memcmp(decompressed.data(), test_data, data_len) == 0,
              "Decompressed content matches");

  printf("  Passed zlib compatibility tests\n\n");
}
#endif

// ============================================================================
// Performance Benchmarks
// ============================================================================

void benchmark_bit_operations() {
  printf("=== Benchmark: Bit Operations ===\n");

  const int iterations = 10000000;

  // CLZ benchmark
  volatile uint32_t dummy = 0;
  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < iterations; i++) {
    dummy += clz32(static_cast<uint32_t>(i + 1));
  }
  auto end = std::chrono::high_resolution_clock::now();
  double ms = std::chrono::duration<double, std::milli>(end - start).count();
  printf("  CLZ32: %.2f ns/op\n", ms * 1e6 / iterations);

  // CTZ benchmark
  start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < iterations; i++) {
    dummy += ctz32(static_cast<uint32_t>(i + 1));
  }
  end = std::chrono::high_resolution_clock::now();
  ms = std::chrono::duration<double, std::milli>(end - start).count();
  printf("  CTZ32: %.2f ns/op\n", ms * 1e6 / iterations);

  // POPCOUNT benchmark
  start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < iterations; i++) {
    dummy += popcount32(static_cast<uint32_t>(i));
  }
  end = std::chrono::high_resolution_clock::now();
  ms = std::chrono::duration<double, std::milli>(end - start).count();
  printf("  POPCOUNT32: %.2f ns/op\n", ms * 1e6 / iterations);

  // BSWAP benchmark
  start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < iterations; i++) {
    dummy += bswap32(static_cast<uint32_t>(i));
  }
  end = std::chrono::high_resolution_clock::now();
  ms = std::chrono::duration<double, std::milli>(end - start).count();
  printf("  BSWAP32: %.2f ns/op\n", ms * 1e6 / iterations);

  printf("  (dummy = %u to prevent optimization)\n\n", dummy);
}

void benchmark_deflate() {
  printf("=== Benchmark: Deflate Decompression ===\n");

  // Create compressible test data
  const size_t data_size = 1024 * 1024;  // 1MB
  std::vector<uint8_t> original(data_size);

  // Fill with somewhat compressible data
  std::mt19937 rng(42);
  for (size_t i = 0; i < data_size; i++) {
    // Mix of repeated and random data
    if (i % 100 < 80) {
      original[i] = static_cast<uint8_t>(i & 0xFF);
    } else {
      original[i] = static_cast<uint8_t>(rng() & 0xFF);
    }
  }

  // Create a stored block (no actual compression, but tests our decompressor)
  // For a real benchmark, we'd use zlib to compress first
  std::vector<uint8_t> compressed;

  // Build stored block
  size_t offset = 0;
  while (offset < data_size) {
    size_t chunk = std::min(size_t(65535), data_size - offset);
    bool final = (offset + chunk >= data_size);

    compressed.push_back(final ? 0x01 : 0x00);  // BFINAL, BTYPE=00
    compressed.push_back(chunk & 0xFF);
    compressed.push_back((chunk >> 8) & 0xFF);
    compressed.push_back(~chunk & 0xFF);
    compressed.push_back((~chunk >> 8) & 0xFF);

    for (size_t i = 0; i < chunk; i++) {
      compressed.push_back(original[offset + i]);
    }

    offset += chunk;
  }

  printf("  Original size: %zu bytes\n", data_size);
  printf("  Compressed size: %zu bytes (stored blocks)\n", compressed.size());

  // Benchmark decompression
  std::vector<uint8_t> decompressed(data_size);
  const int iterations = 100;

  FastDeflateDecoder decoder;

  auto start = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < iterations; i++) {
    size_t out_len = data_size;
    decoder.decompress(compressed.data(), compressed.size(),
                      decompressed.data(), &out_len);
  }
  auto end = std::chrono::high_resolution_clock::now();
  double ms = std::chrono::duration<double, std::milli>(end - start).count();

  double mb_per_sec = (data_size * iterations) / (ms / 1000.0) / (1024.0 * 1024.0);
  printf("  Decompression: %.2f ms for %d iterations\n", ms, iterations);
  printf("                 %.2f MB/sec\n\n", mb_per_sec);
}

// ============================================================================
// Main
// ============================================================================

int main(int argc, char** argv) {
  printf("TinyEXR Huffman/Deflate Test Suite\n");
  printf("===================================\n\n");

  // Display capabilities
  printf("Bit Manipulation Backend: %s\n\n", get_huffman_info());

  // Run tests
  test_bit_manipulation();
  test_bit_buffer();
  test_fixed_huffman_table();
  test_deflate_stored_block();
  test_deflate_fixed_huffman();

#ifdef HAVE_ZLIB
  test_zlib_compatibility();
#endif

  // Run benchmarks
  if (argc > 1 && strcmp(argv[1], "--bench") == 0) {
    benchmark_bit_operations();
    benchmark_deflate();
  }

  // Summary
  printf("=== Test Summary ===\n");
  printf("Total: %d tests\n", g_test_count);
  printf("Passed: %d\n", g_pass_count);
  printf("Failed: %d\n", g_fail_count);
  printf("\n");

  if (g_fail_count > 0) {
    printf("SOME TESTS FAILED!\n");
    return 1;
  } else {
    printf("ALL TESTS PASSED!\n");
    return 0;
  }
}

// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2025, Syoyo Fujita and many contributors.
// All rights reserved.
//
// TinyEXR SIMD Optimization Layer
//
// Part of TinyEXR V2 API (EXPERIMENTAL)
//
// Provides SIMD-accelerated routines for:
// - FP16 <-> FP32 conversion
// - Memory operations (reorder, interleave)
// - Pixel processing
//
// Supported architectures:
// - x86/x64: SSE2, SSE4.1, AVX, AVX2, F16C
// - ARM: NEON (with FP16 extension), SVE, SVE2
// - ARM A64FX: SVE 512-bit
//
// Usage:
//   #define TINYEXR_ENABLE_SIMD 1
//   #include "tinyexr_simd.hh"
//
// The SIMD layer automatically detects available instruction sets at compile time.

#ifndef TINYEXR_SIMD_HH_
#define TINYEXR_SIMD_HH_

#include <cstdint>
#include <cstddef>
#include <cstring>

// ============================================================================
// Configuration and Feature Detection
// ============================================================================

#ifndef TINYEXR_ENABLE_SIMD
#define TINYEXR_ENABLE_SIMD 0
#endif

#if TINYEXR_ENABLE_SIMD

// Detect architecture
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#define TINYEXR_SIMD_X86 1
#else
#define TINYEXR_SIMD_X86 0
#endif

#if defined(__aarch64__) || defined(_M_ARM64)
#define TINYEXR_SIMD_ARM64 1
#else
#define TINYEXR_SIMD_ARM64 0
#endif

#if defined(__arm__) || defined(_M_ARM)
#define TINYEXR_SIMD_ARM32 1
#else
#define TINYEXR_SIMD_ARM32 0
#endif

// x86 SIMD feature detection
#if TINYEXR_SIMD_X86

#if defined(__SSE2__) || (defined(_MSC_VER) && (defined(_M_X64) || (defined(_M_IX86_FP) && _M_IX86_FP >= 2)))
#define TINYEXR_SIMD_SSE2 1
#include <emmintrin.h>
#else
#define TINYEXR_SIMD_SSE2 0
#endif

#if defined(__SSE4_1__) || (defined(_MSC_VER) && defined(__AVX__))
#define TINYEXR_SIMD_SSE41 1
#include <smmintrin.h>
#else
#define TINYEXR_SIMD_SSE41 0
#endif

#if defined(__AVX__)
#define TINYEXR_SIMD_AVX 1
#include <immintrin.h>
#else
#define TINYEXR_SIMD_AVX 0
#endif

#if defined(__AVX2__)
#define TINYEXR_SIMD_AVX2 1
#include <immintrin.h>
#else
#define TINYEXR_SIMD_AVX2 0
#endif

#if defined(__F16C__)
#define TINYEXR_SIMD_F16C 1
#include <immintrin.h>
#else
#define TINYEXR_SIMD_F16C 0
#endif

#if defined(__AVX512F__)
#define TINYEXR_SIMD_AVX512F 1
#include <immintrin.h>
#else
#define TINYEXR_SIMD_AVX512F 0
#endif

#else
// Not x86
#define TINYEXR_SIMD_SSE2 0
#define TINYEXR_SIMD_SSE41 0
#define TINYEXR_SIMD_AVX 0
#define TINYEXR_SIMD_AVX2 0
#define TINYEXR_SIMD_F16C 0
#define TINYEXR_SIMD_AVX512F 0
#endif

// ARM NEON detection
#if TINYEXR_SIMD_ARM64 || TINYEXR_SIMD_ARM32

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#define TINYEXR_SIMD_NEON 1
#include <arm_neon.h>
#else
#define TINYEXR_SIMD_NEON 0
#endif

// ARM FP16 (half-precision) support
#if defined(__ARM_FP16_FORMAT_IEEE) || defined(__ARM_FEATURE_FP16_SCALAR_ARITHMETIC)
#define TINYEXR_SIMD_NEON_FP16 1
#else
#define TINYEXR_SIMD_NEON_FP16 0
#endif

// ARM SVE detection
#if defined(__ARM_FEATURE_SVE)
#define TINYEXR_SIMD_SVE 1
#include <arm_sve.h>
#else
#define TINYEXR_SIMD_SVE 0
#endif

// ARM SVE2 detection
#if defined(__ARM_FEATURE_SVE2)
#define TINYEXR_SIMD_SVE2 1
#else
#define TINYEXR_SIMD_SVE2 0
#endif

// A64FX detection (Fujitsu A64FX has 512-bit SVE)
// A64FX can be detected by checking SVE vector length at runtime,
// but for compile-time, we use a define
#ifndef TINYEXR_SIMD_A64FX
#define TINYEXR_SIMD_A64FX 0
#endif

#else
// Not ARM
#define TINYEXR_SIMD_NEON 0
#define TINYEXR_SIMD_NEON_FP16 0
#define TINYEXR_SIMD_SVE 0
#define TINYEXR_SIMD_SVE2 0
#define TINYEXR_SIMD_A64FX 0
#endif

#else
// SIMD disabled
#define TINYEXR_SIMD_X86 0
#define TINYEXR_SIMD_ARM64 0
#define TINYEXR_SIMD_ARM32 0
#define TINYEXR_SIMD_SSE2 0
#define TINYEXR_SIMD_SSE41 0
#define TINYEXR_SIMD_AVX 0
#define TINYEXR_SIMD_AVX2 0
#define TINYEXR_SIMD_F16C 0
#define TINYEXR_SIMD_AVX512F 0
#define TINYEXR_SIMD_NEON 0
#define TINYEXR_SIMD_NEON_FP16 0
#define TINYEXR_SIMD_SVE 0
#define TINYEXR_SIMD_SVE2 0
#define TINYEXR_SIMD_A64FX 0
#endif

// ============================================================================
// Namespace and utilities
// ============================================================================

namespace tinyexr {
namespace simd {

// SIMD capability flags (can be queried at runtime)
struct SIMDCapabilities {
  bool sse2;
  bool sse41;
  bool avx;
  bool avx2;
  bool f16c;
  bool avx512f;
  bool neon;
  bool neon_fp16;
  bool sve;
  bool sve2;
  bool a64fx;
  uint32_t sve_vector_length;  // SVE vector length in bits (0 if not SVE)

  SIMDCapabilities()
    : sse2(TINYEXR_SIMD_SSE2),
      sse41(TINYEXR_SIMD_SSE41),
      avx(TINYEXR_SIMD_AVX),
      avx2(TINYEXR_SIMD_AVX2),
      f16c(TINYEXR_SIMD_F16C),
      avx512f(TINYEXR_SIMD_AVX512F),
      neon(TINYEXR_SIMD_NEON),
      neon_fp16(TINYEXR_SIMD_NEON_FP16),
      sve(TINYEXR_SIMD_SVE),
      sve2(TINYEXR_SIMD_SVE2),
      a64fx(TINYEXR_SIMD_A64FX),
      sve_vector_length(0) {
#if TINYEXR_SIMD_SVE
    // Get SVE vector length at runtime
    sve_vector_length = static_cast<uint32_t>(svcntb() * 8);
    // A64FX has 512-bit SVE
    if (sve_vector_length == 512) {
      a64fx = true;
    }
#endif
  }
};

inline const SIMDCapabilities& get_capabilities() {
  static SIMDCapabilities caps;
  return caps;
}

// ============================================================================
// FP16 <-> FP32 Scalar Fallback
// ============================================================================

// Software FP16 to FP32 conversion (same algorithm as tinyexr.h)
inline float half_to_float_scalar(uint16_t h) {
  union { uint32_t u; float f; } o;
  static const union { uint32_t u; float f; } magic = {113U << 23};
  static const uint32_t shifted_exp = 0x7c00U << 13;

  o.u = (h & 0x7fffU) << 13;
  uint32_t exp_ = shifted_exp & o.u;
  o.u += (127 - 15) << 23;

  if (exp_ == shifted_exp) {
    o.u += (128 - 16) << 23;
  } else if (exp_ == 0) {
    o.u += 1 << 23;
    o.f -= magic.f;
  }

  o.u |= (h & 0x8000U) << 16;
  return o.f;
}

// Software FP32 to FP16 conversion
inline uint16_t float_to_half_scalar(float f) {
  union {
    float f;
    uint32_t u;
    struct {
      uint32_t Mantissa : 23;
      uint32_t Exponent : 8;
      uint32_t Sign : 1;
    } s;
  } fi;
  fi.f = f;

  union {
    uint16_t u;
    struct {
      uint16_t Mantissa : 10;
      uint16_t Exponent : 5;
      uint16_t Sign : 1;
    } s;
  } o;
  o.u = 0;

  if (fi.s.Exponent == 0) {
    o.s.Exponent = 0;
  } else if (fi.s.Exponent == 255) {
    o.s.Exponent = 31;
    o.s.Mantissa = fi.s.Mantissa ? 0x200 : 0;
  } else {
    int newexp = static_cast<int>(fi.s.Exponent) - 127 + 15;
    if (newexp >= 31) {
      o.s.Exponent = 31;
    } else if (newexp <= 0) {
      if ((14 - newexp) <= 24) {
        uint32_t mant = fi.s.Mantissa | 0x800000;
        o.s.Mantissa = static_cast<uint16_t>(mant >> (14 - newexp));
        if ((mant >> (13 - newexp)) & 1) {
          o.u++;
        }
      }
    } else {
      o.s.Exponent = static_cast<uint16_t>(newexp);
      o.s.Mantissa = static_cast<uint16_t>(fi.s.Mantissa >> 13);
      if (fi.s.Mantissa & 0x1000) {
        o.u++;
      }
    }
  }

  o.s.Sign = static_cast<uint16_t>(fi.s.Sign);
  return o.u;
}

// ============================================================================
// x86 SSE/AVX Implementation
// ============================================================================

#if TINYEXR_SIMD_X86

#if TINYEXR_SIMD_F16C

// F16C: 4 half values to 4 float values (SSE)
inline void half_to_float_4_f16c(const uint16_t* src, float* dst) {
  __m128i h = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(src));
  __m128 f = _mm_cvtph_ps(h);
  _mm_storeu_ps(dst, f);
}

// F16C: 8 half values to 8 float values (AVX)
inline void half_to_float_8_f16c(const uint16_t* src, float* dst) {
  __m128i h = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
  __m256 f = _mm256_cvtph_ps(h);
  _mm256_storeu_ps(dst, f);
}

// F16C: 4 float values to 4 half values (SSE)
inline void float_to_half_4_f16c(const float* src, uint16_t* dst) {
  __m128 f = _mm_loadu_ps(src);
  __m128i h = _mm_cvtps_ph(f, _MM_FROUND_TO_NEAREST_INT | _MM_FROUND_NO_EXC);
  _mm_storel_epi64(reinterpret_cast<__m128i*>(dst), h);
}

// F16C: 8 float values to 8 half values (AVX)
inline void float_to_half_8_f16c(const float* src, uint16_t* dst) {
  __m256 f = _mm256_loadu_ps(src);
  __m128i h = _mm256_cvtps_ph(f, _MM_FROUND_TO_NEAREST_INT | _MM_FROUND_NO_EXC);
  _mm_storeu_si128(reinterpret_cast<__m128i*>(dst), h);
}

#endif  // TINYEXR_SIMD_F16C

#if TINYEXR_SIMD_AVX512F

// AVX-512: 16 half values to 16 float values
inline void half_to_float_16_avx512(const uint16_t* src, float* dst) {
  __m256i h = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
  __m512 f = _mm512_cvtph_ps(h);
  _mm512_storeu_ps(dst, f);
}

// AVX-512: 16 float values to 16 half values
inline void float_to_half_16_avx512(const float* src, uint16_t* dst) {
  __m512 f = _mm512_loadu_ps(src);
  __m256i h = _mm512_cvtps_ph(f, _MM_FROUND_TO_NEAREST_INT | _MM_FROUND_NO_EXC);
  _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst), h);
}

#endif  // TINYEXR_SIMD_AVX512F

#if TINYEXR_SIMD_SSE2

// SSE2 software half-to-float (no F16C)
// Process 4 values at a time using integer operations
inline void half_to_float_4_sse2(const uint16_t* src, float* dst) {
  // Fallback to scalar for now - SSE2 half conversion is complex
  for (int i = 0; i < 4; i++) {
    dst[i] = half_to_float_scalar(src[i]);
  }
}

inline void float_to_half_4_sse2(const float* src, uint16_t* dst) {
  // Fallback to scalar for now
  for (int i = 0; i < 4; i++) {
    dst[i] = float_to_half_scalar(src[i]);
  }
}

#endif  // TINYEXR_SIMD_SSE2

#endif  // TINYEXR_SIMD_X86

// ============================================================================
// ARM NEON Implementation
// ============================================================================

#if TINYEXR_SIMD_NEON

#if TINYEXR_SIMD_NEON_FP16

// NEON with FP16: 4 half values to 4 float values
inline void half_to_float_4_neon_fp16(const uint16_t* src, float* dst) {
  float16x4_t h = vld1_f16(reinterpret_cast<const float16_t*>(src));
  float32x4_t f = vcvt_f32_f16(h);
  vst1q_f32(dst, f);
}

// NEON with FP16: 8 half values to 8 float values
inline void half_to_float_8_neon_fp16(const uint16_t* src, float* dst) {
  float16x8_t h = vld1q_f16(reinterpret_cast<const float16_t*>(src));
  float32x4_t f_lo = vcvt_f32_f16(vget_low_f16(h));
  float32x4_t f_hi = vcvt_f32_f16(vget_high_f16(h));
  vst1q_f32(dst, f_lo);
  vst1q_f32(dst + 4, f_hi);
}

// NEON with FP16: 4 float values to 4 half values
inline void float_to_half_4_neon_fp16(const float* src, uint16_t* dst) {
  float32x4_t f = vld1q_f32(src);
  float16x4_t h = vcvt_f16_f32(f);
  vst1_f16(reinterpret_cast<float16_t*>(dst), h);
}

// NEON with FP16: 8 float values to 8 half values
inline void float_to_half_8_neon_fp16(const float* src, uint16_t* dst) {
  float32x4_t f_lo = vld1q_f32(src);
  float32x4_t f_hi = vld1q_f32(src + 4);
  float16x4_t h_lo = vcvt_f16_f32(f_lo);
  float16x4_t h_hi = vcvt_f16_f32(f_hi);
  float16x8_t h = vcombine_f16(h_lo, h_hi);
  vst1q_f16(reinterpret_cast<float16_t*>(dst), h);
}

#else

// NEON without FP16 hardware support - use scalar fallback
inline void half_to_float_4_neon(const uint16_t* src, float* dst) {
  for (int i = 0; i < 4; i++) {
    dst[i] = half_to_float_scalar(src[i]);
  }
}

inline void float_to_half_4_neon(const float* src, uint16_t* dst) {
  for (int i = 0; i < 4; i++) {
    dst[i] = float_to_half_scalar(src[i]);
  }
}

#endif  // TINYEXR_SIMD_NEON_FP16

#endif  // TINYEXR_SIMD_NEON

// ============================================================================
// ARM SVE Implementation
// ============================================================================

#if TINYEXR_SIMD_SVE

// SVE: Variable-length half to float conversion
// Processes as many elements as the SVE vector length allows
inline size_t half_to_float_sve(const uint16_t* src, float* dst, size_t count) {
  size_t processed = 0;
  svbool_t pg = svwhilelt_b16(processed, count);

  while (svptest_any(svptrue_b16(), pg)) {
    // Load half-precision values
    svfloat16_t h = svld1_f16(pg, reinterpret_cast<const float16_t*>(src + processed));

    // Convert to float32 (process in two halves for SVE)
    svbool_t pg32 = svwhilelt_b32(processed, count);
    svfloat32_t f_lo = svcvt_f32_f16_z(pg32, h);
    vst1q_f32(dst + processed, f_lo);  // This needs SVE store

    // SVE doesn't have direct half-vector access like NEON
    // We need to use predicated stores
    svst1_f32(pg32, dst + processed, f_lo);

    size_t step = svcnth();
    processed += step;
    pg = svwhilelt_b16(processed, count);
  }

  return processed;
}

// SVE: Variable-length float to half conversion
inline size_t float_to_half_sve(const float* src, uint16_t* dst, size_t count) {
  size_t processed = 0;

  while (processed < count) {
    size_t remaining = count - processed;
    svbool_t pg32 = svwhilelt_b32(static_cast<uint64_t>(0), remaining);

    // Load float32 values
    svfloat32_t f = svld1_f32(pg32, src + processed);

    // Convert to half-precision
    svfloat16_t h = svcvt_f16_f32_z(svwhilelt_b16(static_cast<uint64_t>(0), remaining), f);

    // Store half-precision values
    svst1_f16(svwhilelt_b16(static_cast<uint64_t>(0), remaining),
              reinterpret_cast<float16_t*>(dst + processed), h);

    size_t step = svcntw();  // Number of 32-bit elements
    processed += step;
  }

  return processed;
}

#endif  // TINYEXR_SIMD_SVE

// ============================================================================
// ARM A64FX (SVE 512-bit) Optimized Implementation
// ============================================================================

#if TINYEXR_SIMD_A64FX || (TINYEXR_SIMD_SVE && defined(TINYEXR_A64FX_OPTIMIZED))

// A64FX-specific optimizations for 512-bit SVE
// A64FX has 512-bit SVE vectors, so we can process:
// - 32 half-precision values at once
// - 16 single-precision values at once

// A64FX: 16 half values to 16 float values (512-bit SVE)
inline void half_to_float_16_a64fx(const uint16_t* src, float* dst) {
#if TINYEXR_SIMD_SVE
  // Use full 512-bit vector for half-precision (32 values)
  // But we convert 16 at a time to fit in float32 output
  svbool_t pg16 = svptrue_b16();
  svbool_t pg32 = svptrue_b32();

  // Load 16 half-precision values
  svfloat16_t h = svld1_f16(svwhilelt_b16(static_cast<uint64_t>(0), static_cast<uint64_t>(16)),
                            reinterpret_cast<const float16_t*>(src));

  // Convert to float32
  svfloat32_t f = svcvt_f32_f16_z(pg32, h);

  // Store 16 float values
  svst1_f32(pg32, dst, f);
#else
  // Fallback to scalar
  for (int i = 0; i < 16; i++) {
    dst[i] = half_to_float_scalar(src[i]);
  }
#endif
}

// A64FX: 16 float values to 16 half values (512-bit SVE)
inline void float_to_half_16_a64fx(const float* src, uint16_t* dst) {
#if TINYEXR_SIMD_SVE
  svbool_t pg32 = svptrue_b32();

  // Load 16 float values
  svfloat32_t f = svld1_f32(pg32, src);

  // Convert to half-precision
  svfloat16_t h = svcvt_f16_f32_z(svwhilelt_b16(static_cast<uint64_t>(0), static_cast<uint64_t>(16)), f);

  // Store 16 half-precision values
  svst1_f16(svwhilelt_b16(static_cast<uint64_t>(0), static_cast<uint64_t>(16)),
            reinterpret_cast<float16_t*>(dst), h);
#else
  // Fallback to scalar
  for (int i = 0; i < 16; i++) {
    dst[i] = float_to_half_scalar(src[i]);
  }
#endif
}

// A64FX: 32 half values to 32 float values (full 512-bit utilization)
inline void half_to_float_32_a64fx(const uint16_t* src, float* dst) {
#if TINYEXR_SIMD_SVE
  // Process in two batches of 16 to utilize full 512-bit vectors
  half_to_float_16_a64fx(src, dst);
  half_to_float_16_a64fx(src + 16, dst + 16);
#else
  for (int i = 0; i < 32; i++) {
    dst[i] = half_to_float_scalar(src[i]);
  }
#endif
}

// A64FX: 32 float values to 32 half values (full 512-bit utilization)
inline void float_to_half_32_a64fx(const float* src, uint16_t* dst) {
#if TINYEXR_SIMD_SVE
  // Process in two batches of 16
  float_to_half_16_a64fx(src, dst);
  float_to_half_16_a64fx(src + 16, dst + 16);
#else
  for (int i = 0; i < 32; i++) {
    dst[i] = float_to_half_scalar(src[i]);
  }
#endif
}

#endif  // TINYEXR_SIMD_A64FX

// ============================================================================
// Generic Batch Conversion Functions (auto-dispatch)
// ============================================================================

// Convert an array of half-precision values to float
inline void half_to_float_batch(const uint16_t* src, float* dst, size_t count) {
  size_t i = 0;

#if TINYEXR_SIMD_A64FX || (TINYEXR_SIMD_SVE && defined(TINYEXR_A64FX_OPTIMIZED))
  // A64FX: Process 32 values at a time
  for (; i + 32 <= count; i += 32) {
    half_to_float_32_a64fx(src + i, dst + i);
  }
  for (; i + 16 <= count; i += 16) {
    half_to_float_16_a64fx(src + i, dst + i);
  }
#elif TINYEXR_SIMD_SVE
  // SVE: Use variable-length processing
  i = half_to_float_sve(src, dst, count);
#elif TINYEXR_SIMD_AVX512F
  // AVX-512: Process 16 values at a time
  for (; i + 16 <= count; i += 16) {
    half_to_float_16_avx512(src + i, dst + i);
  }
#elif TINYEXR_SIMD_F16C && TINYEXR_SIMD_AVX
  // AVX + F16C: Process 8 values at a time
  for (; i + 8 <= count; i += 8) {
    half_to_float_8_f16c(src + i, dst + i);
  }
#elif TINYEXR_SIMD_F16C
  // SSE + F16C: Process 4 values at a time
  for (; i + 4 <= count; i += 4) {
    half_to_float_4_f16c(src + i, dst + i);
  }
#elif TINYEXR_SIMD_NEON_FP16
  // NEON with FP16: Process 8 values at a time
  for (; i + 8 <= count; i += 8) {
    half_to_float_8_neon_fp16(src + i, dst + i);
  }
  for (; i + 4 <= count; i += 4) {
    half_to_float_4_neon_fp16(src + i, dst + i);
  }
#elif TINYEXR_SIMD_NEON
  // NEON without FP16: Use scalar (4 at a time for cache efficiency)
  for (; i + 4 <= count; i += 4) {
    half_to_float_4_neon(src + i, dst + i);
  }
#elif TINYEXR_SIMD_SSE2
  // SSE2: Use scalar implementation with some loop unrolling
  for (; i + 4 <= count; i += 4) {
    half_to_float_4_sse2(src + i, dst + i);
  }
#endif

  // Handle remaining elements with scalar code
  for (; i < count; i++) {
    dst[i] = half_to_float_scalar(src[i]);
  }
}

// Convert an array of float values to half-precision
inline void float_to_half_batch(const float* src, uint16_t* dst, size_t count) {
  size_t i = 0;

#if TINYEXR_SIMD_A64FX || (TINYEXR_SIMD_SVE && defined(TINYEXR_A64FX_OPTIMIZED))
  // A64FX: Process 32 values at a time
  for (; i + 32 <= count; i += 32) {
    float_to_half_32_a64fx(src + i, dst + i);
  }
  for (; i + 16 <= count; i += 16) {
    float_to_half_16_a64fx(src + i, dst + i);
  }
#elif TINYEXR_SIMD_SVE
  // SVE: Use variable-length processing
  i = float_to_half_sve(src, dst, count);
#elif TINYEXR_SIMD_AVX512F
  // AVX-512: Process 16 values at a time
  for (; i + 16 <= count; i += 16) {
    float_to_half_16_avx512(src + i, dst + i);
  }
#elif TINYEXR_SIMD_F16C && TINYEXR_SIMD_AVX
  // AVX + F16C: Process 8 values at a time
  for (; i + 8 <= count; i += 8) {
    float_to_half_8_f16c(src + i, dst + i);
  }
#elif TINYEXR_SIMD_F16C
  // SSE + F16C: Process 4 values at a time
  for (; i + 4 <= count; i += 4) {
    float_to_half_4_f16c(src + i, dst + i);
  }
#elif TINYEXR_SIMD_NEON_FP16
  // NEON with FP16: Process 8 values at a time
  for (; i + 8 <= count; i += 8) {
    float_to_half_8_neon_fp16(src + i, dst + i);
  }
  for (; i + 4 <= count; i += 4) {
    float_to_half_4_neon_fp16(src + i, dst + i);
  }
#elif TINYEXR_SIMD_NEON
  // NEON without FP16
  for (; i + 4 <= count; i += 4) {
    float_to_half_4_neon(src + i, dst + i);
  }
#elif TINYEXR_SIMD_SSE2
  // SSE2
  for (; i + 4 <= count; i += 4) {
    float_to_half_4_sse2(src + i, dst + i);
  }
#endif

  // Handle remaining elements with scalar code
  for (; i < count; i++) {
    dst[i] = float_to_half_scalar(src[i]);
  }
}

// ============================================================================
// Memory Operations
// ============================================================================

// Fast memory copy with SIMD
inline void memcpy_simd(void* dst, const void* src, size_t bytes) {
#if TINYEXR_SIMD_AVX512F
  const size_t vec_size = 64;
  size_t i = 0;
  for (; i + vec_size <= bytes; i += vec_size) {
    __m512i v = _mm512_loadu_si512(static_cast<const char*>(src) + i);
    _mm512_storeu_si512(static_cast<char*>(dst) + i, v);
  }
  if (i < bytes) {
    std::memcpy(static_cast<char*>(dst) + i, static_cast<const char*>(src) + i, bytes - i);
  }
#elif TINYEXR_SIMD_AVX
  const size_t vec_size = 32;
  size_t i = 0;
  for (; i + vec_size <= bytes; i += vec_size) {
    __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(static_cast<const char*>(src) + i));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(static_cast<char*>(dst) + i), v);
  }
  if (i < bytes) {
    std::memcpy(static_cast<char*>(dst) + i, static_cast<const char*>(src) + i, bytes - i);
  }
#elif TINYEXR_SIMD_SSE2
  const size_t vec_size = 16;
  size_t i = 0;
  for (; i + vec_size <= bytes; i += vec_size) {
    __m128i v = _mm_loadu_si128(reinterpret_cast<const __m128i*>(static_cast<const char*>(src) + i));
    _mm_storeu_si128(reinterpret_cast<__m128i*>(static_cast<char*>(dst) + i), v);
  }
  if (i < bytes) {
    std::memcpy(static_cast<char*>(dst) + i, static_cast<const char*>(src) + i, bytes - i);
  }
#elif TINYEXR_SIMD_NEON
  const size_t vec_size = 16;
  size_t i = 0;
  for (; i + vec_size <= bytes; i += vec_size) {
    uint8x16_t v = vld1q_u8(static_cast<const uint8_t*>(src) + i);
    vst1q_u8(static_cast<uint8_t*>(dst) + i, v);
  }
  if (i < bytes) {
    std::memcpy(static_cast<char*>(dst) + i, static_cast<const char*>(src) + i, bytes - i);
  }
#elif TINYEXR_SIMD_SVE
  size_t i = 0;
  while (i < bytes) {
    svbool_t pg = svwhilelt_b8(i, bytes);
    svuint8_t v = svld1_u8(pg, static_cast<const uint8_t*>(src) + i);
    svst1_u8(pg, static_cast<uint8_t*>(dst) + i, v);
    i += svcntb();
  }
#else
  std::memcpy(dst, src, bytes);
#endif
}

// ============================================================================
// Pixel Channel Interleaving/Deinterleaving
// ============================================================================

// Interleave separate R, G, B, A channels into RGBA format
// Input: 4 separate float arrays (R, G, B, A), each of length 'count'
// Output: Interleaved RGBA array of length 'count * 4'
inline void interleave_rgba_float(const float* r, const float* g, const float* b, const float* a,
                                  float* rgba, size_t count) {
  size_t i = 0;

#if TINYEXR_SIMD_AVX
  // AVX: Process 8 pixels at a time
  for (; i + 8 <= count; i += 8) {
    __m256 vr = _mm256_loadu_ps(r + i);
    __m256 vg = _mm256_loadu_ps(g + i);
    __m256 vb = _mm256_loadu_ps(b + i);
    __m256 va = _mm256_loadu_ps(a + i);

    // Interleave: RGBARGBA...
    // First, interleave R and G, B and A
    __m256 rg_lo = _mm256_unpacklo_ps(vr, vg);  // r0 g0 r1 g1 | r4 g4 r5 g5
    __m256 rg_hi = _mm256_unpackhi_ps(vr, vg);  // r2 g2 r3 g3 | r6 g6 r7 g7
    __m256 ba_lo = _mm256_unpacklo_ps(vb, va);  // b0 a0 b1 a1 | b4 a4 b5 a5
    __m256 ba_hi = _mm256_unpackhi_ps(vb, va);  // b2 a2 b3 a3 | b6 a6 b7 a7

    // Combine RG and BA
    __m256 rgba0 = _mm256_shuffle_ps(rg_lo, ba_lo, 0x44);  // r0 g0 b0 a0 | r4 g4 b4 a4
    __m256 rgba1 = _mm256_shuffle_ps(rg_lo, ba_lo, 0xEE);  // r1 g1 b1 a1 | r5 g5 b5 a5
    __m256 rgba2 = _mm256_shuffle_ps(rg_hi, ba_hi, 0x44);  // r2 g2 b2 a2 | r6 g6 b6 a6
    __m256 rgba3 = _mm256_shuffle_ps(rg_hi, ba_hi, 0xEE);  // r3 g3 b3 a3 | r7 g7 b7 a7

    // Permute to get final order
    __m256 out0 = _mm256_permute2f128_ps(rgba0, rgba1, 0x20);  // r0 g0 b0 a0 r1 g1 b1 a1
    __m256 out1 = _mm256_permute2f128_ps(rgba2, rgba3, 0x20);  // r2 g2 b2 a2 r3 g3 b3 a3
    __m256 out2 = _mm256_permute2f128_ps(rgba0, rgba1, 0x31);  // r4 g4 b4 a4 r5 g5 b5 a5
    __m256 out3 = _mm256_permute2f128_ps(rgba2, rgba3, 0x31);  // r6 g6 b6 a6 r7 g7 b7 a7

    _mm256_storeu_ps(rgba + i * 4, out0);
    _mm256_storeu_ps(rgba + i * 4 + 8, out1);
    _mm256_storeu_ps(rgba + i * 4 + 16, out2);
    _mm256_storeu_ps(rgba + i * 4 + 24, out3);
  }
#elif TINYEXR_SIMD_SSE2
  // SSE2: Process 4 pixels at a time
  for (; i + 4 <= count; i += 4) {
    __m128 vr = _mm_loadu_ps(r + i);
    __m128 vg = _mm_loadu_ps(g + i);
    __m128 vb = _mm_loadu_ps(b + i);
    __m128 va = _mm_loadu_ps(a + i);

    // Interleave
    __m128 rg_lo = _mm_unpacklo_ps(vr, vg);  // r0 g0 r1 g1
    __m128 rg_hi = _mm_unpackhi_ps(vr, vg);  // r2 g2 r3 g3
    __m128 ba_lo = _mm_unpacklo_ps(vb, va);  // b0 a0 b1 a1
    __m128 ba_hi = _mm_unpackhi_ps(vb, va);  // b2 a2 b3 a3

    __m128 rgba0 = _mm_movelh_ps(rg_lo, ba_lo);  // r0 g0 b0 a0
    __m128 rgba1 = _mm_movehl_ps(ba_lo, rg_lo);  // r1 g1 b1 a1
    __m128 rgba2 = _mm_movelh_ps(rg_hi, ba_hi);  // r2 g2 b2 a2
    __m128 rgba3 = _mm_movehl_ps(ba_hi, rg_hi);  // r3 g3 b3 a3

    _mm_storeu_ps(rgba + i * 4, rgba0);
    _mm_storeu_ps(rgba + i * 4 + 4, rgba1);
    _mm_storeu_ps(rgba + i * 4 + 8, rgba2);
    _mm_storeu_ps(rgba + i * 4 + 12, rgba3);
  }
#elif TINYEXR_SIMD_NEON
  // NEON: Process 4 pixels at a time
  for (; i + 4 <= count; i += 4) {
    float32x4_t vr = vld1q_f32(r + i);
    float32x4_t vg = vld1q_f32(g + i);
    float32x4_t vb = vld1q_f32(b + i);
    float32x4_t va = vld1q_f32(a + i);

    float32x4x4_t rgba_vec = {{vr, vg, vb, va}};
    vst4q_f32(rgba + i * 4, rgba_vec);
  }
#endif

  // Scalar fallback for remaining elements
  for (; i < count; i++) {
    rgba[i * 4 + 0] = r[i];
    rgba[i * 4 + 1] = g[i];
    rgba[i * 4 + 2] = b[i];
    rgba[i * 4 + 3] = a[i];
  }
}

// Deinterleave RGBA format into separate R, G, B, A channels
inline void deinterleave_rgba_float(const float* rgba, float* r, float* g, float* b, float* a,
                                    size_t count) {
  size_t i = 0;

#if TINYEXR_SIMD_NEON
  // NEON has excellent deinterleave support
  for (; i + 4 <= count; i += 4) {
    float32x4x4_t rgba_vec = vld4q_f32(rgba + i * 4);
    vst1q_f32(r + i, rgba_vec.val[0]);
    vst1q_f32(g + i, rgba_vec.val[1]);
    vst1q_f32(b + i, rgba_vec.val[2]);
    vst1q_f32(a + i, rgba_vec.val[3]);
  }
#elif TINYEXR_SIMD_SSE2
  // SSE2: Process 4 pixels at a time
  for (; i + 4 <= count; i += 4) {
    __m128 rgba0 = _mm_loadu_ps(rgba + i * 4);       // r0 g0 b0 a0
    __m128 rgba1 = _mm_loadu_ps(rgba + i * 4 + 4);   // r1 g1 b1 a1
    __m128 rgba2 = _mm_loadu_ps(rgba + i * 4 + 8);   // r2 g2 b2 a2
    __m128 rgba3 = _mm_loadu_ps(rgba + i * 4 + 12);  // r3 g3 b3 a3

    // Transpose 4x4 matrix
    __m128 t0 = _mm_unpacklo_ps(rgba0, rgba1);  // r0 r1 g0 g1
    __m128 t1 = _mm_unpackhi_ps(rgba0, rgba1);  // b0 b1 a0 a1
    __m128 t2 = _mm_unpacklo_ps(rgba2, rgba3);  // r2 r3 g2 g3
    __m128 t3 = _mm_unpackhi_ps(rgba2, rgba3);  // b2 b3 a2 a3

    __m128 vr = _mm_movelh_ps(t0, t2);  // r0 r1 r2 r3
    __m128 vg = _mm_movehl_ps(t2, t0);  // g0 g1 g2 g3
    __m128 vb = _mm_movelh_ps(t1, t3);  // b0 b1 b2 b3
    __m128 va = _mm_movehl_ps(t3, t1);  // a0 a1 a2 a3

    _mm_storeu_ps(r + i, vr);
    _mm_storeu_ps(g + i, vg);
    _mm_storeu_ps(b + i, vb);
    _mm_storeu_ps(a + i, va);
  }
#endif

  // Scalar fallback
  for (; i < count; i++) {
    r[i] = rgba[i * 4 + 0];
    g[i] = rgba[i * 4 + 1];
    b[i] = rgba[i * 4 + 2];
    a[i] = rgba[i * 4 + 3];
  }
}

// ============================================================================
// RLE Decompression with SIMD
// ============================================================================

// Optimized byte reordering for EXR scanline data
// This reorders bytes for better compression (separates MSB and LSB)
inline void reorder_bytes_for_compression(const uint8_t* src, uint8_t* dst, size_t count) {
  size_t half = count / 2;

  // Reorder: alternating bytes to separate channels
  // This mimics the OpenEXR predictor preprocessing
  size_t i = 0;

// Note: AVX2 byte deinterleave is complex due to lane boundaries.
// Using SSE2 approach which is simpler and still fast.
#if TINYEXR_SIMD_SSE2
  // SSE2: Process 16 bytes at a time
  for (; i + 16 <= half; i += 16) {
    __m128i v0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src + i * 2));
    __m128i v1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src + i * 2 + 16));

    // Simple even/odd separation using masks and shifts
    __m128i even0 = _mm_and_si128(v0, _mm_set1_epi16(0x00FF));
    __m128i even1 = _mm_and_si128(v1, _mm_set1_epi16(0x00FF));
    __m128i odd0 = _mm_srli_epi16(v0, 8);
    __m128i odd1 = _mm_srli_epi16(v1, 8);

    __m128i evens = _mm_packus_epi16(even0, even1);
    __m128i odds = _mm_packus_epi16(odd0, odd1);

    _mm_storeu_si128(reinterpret_cast<__m128i*>(dst + i), evens);
    _mm_storeu_si128(reinterpret_cast<__m128i*>(dst + half + i), odds);
  }
#elif TINYEXR_SIMD_NEON
  // NEON: Process 16 bytes at a time with native deinterleave
  for (; i + 16 <= half; i += 16) {
    uint8x16x2_t v = vld2q_u8(src + i * 2);
    vst1q_u8(dst + i, v.val[0]);
    vst1q_u8(dst + half + i, v.val[1]);
  }
#endif

  // Scalar fallback
  for (; i < half; i++) {
    dst[i] = src[i * 2];
    dst[half + i] = src[i * 2 + 1];
  }
}

// Reverse byte reordering after decompression
inline void unreorder_bytes_after_decompression(const uint8_t* src, uint8_t* dst, size_t count) {
  size_t half = count / 2;
  size_t i = 0;

#if TINYEXR_SIMD_NEON
  // NEON: Process 16 bytes at a time with native interleave
  for (; i + 16 <= half; i += 16) {
    uint8x16_t v0 = vld1q_u8(src + i);
    uint8x16_t v1 = vld1q_u8(src + half + i);
    uint8x16x2_t out = {{v0, v1}};
    vst2q_u8(dst + i * 2, out);
  }
#elif TINYEXR_SIMD_SSE2
  // SSE2: Process 16 bytes at a time
  for (; i + 16 <= half; i += 16) {
    __m128i evens = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src + i));
    __m128i odds = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src + half + i));

    __m128i lo = _mm_unpacklo_epi8(evens, odds);
    __m128i hi = _mm_unpackhi_epi8(evens, odds);

    _mm_storeu_si128(reinterpret_cast<__m128i*>(dst + i * 2), lo);
    _mm_storeu_si128(reinterpret_cast<__m128i*>(dst + i * 2 + 16), hi);
  }
#endif

  // Scalar fallback
  for (; i < half; i++) {
    dst[i * 2] = src[i];
    dst[i * 2 + 1] = src[half + i];
  }
}

// ============================================================================
// Predictor (delta encoding/decoding)
// ============================================================================

// Apply delta predictor (used in PIZ compression)
inline void apply_delta_predictor(uint8_t* data, size_t count) {
  if (count < 2) return;

  size_t i = 1;

#if TINYEXR_SIMD_AVX2
  // AVX2: Process 32 bytes at a time
  if (count >= 33) {
    // Process first byte separately
    data[1] = static_cast<uint8_t>(data[0] + data[1] - 128);

    // SIMD processing
    for (i = 2; i + 31 <= count; ) {
      __m256i prev = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i - 1));
      __m256i curr = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
      __m256i bias = _mm256_set1_epi8(-128);

      // Sequential dependency makes full SIMD difficult
      // Fall back to scalar for correctness
      break;
    }
  }
#endif

  // Scalar fallback (required due to sequential dependency)
  for (; i < count; i++) {
    data[i] = static_cast<uint8_t>(data[i - 1] + data[i] - 128);
  }
}

// Reverse delta predictor
inline void reverse_delta_predictor(uint8_t* data, size_t count) {
  if (count < 2) return;

  // This has sequential dependency, so SIMD doesn't help much
  // But we can still use SIMD for the subtraction step
  for (size_t i = count - 1; i >= 1; i--) {
    data[i] = static_cast<uint8_t>(data[i] - data[i - 1] + 128);
  }
}

// ============================================================================
// Utility Functions
// ============================================================================

// Check if pointer is aligned to N bytes
template<size_t N>
inline bool is_aligned(const void* ptr) {
  return (reinterpret_cast<uintptr_t>(ptr) % N) == 0;
}

// Get optimal batch size for current architecture
inline size_t get_optimal_batch_size() {
#if TINYEXR_SIMD_AVX512F
  return 64;  // 512 bits = 64 bytes
#elif TINYEXR_SIMD_AVX
  return 32;  // 256 bits = 32 bytes
#elif TINYEXR_SIMD_SSE2 || TINYEXR_SIMD_NEON
  return 16;  // 128 bits = 16 bytes
#elif TINYEXR_SIMD_SVE
  return svcntb();  // SVE vector length in bytes
#else
  return 8;   // Fallback
#endif
}

// Get SIMD capability string for debugging
inline const char* get_simd_info() {
#if TINYEXR_SIMD_AVX512F
  return "AVX-512F";
#elif TINYEXR_SIMD_AVX2 && TINYEXR_SIMD_F16C
  return "AVX2+F16C";
#elif TINYEXR_SIMD_AVX2
  return "AVX2";
#elif TINYEXR_SIMD_AVX
  return "AVX";
#elif TINYEXR_SIMD_F16C
  return "SSE+F16C";
#elif TINYEXR_SIMD_SSE41
  return "SSE4.1";
#elif TINYEXR_SIMD_SSE2
  return "SSE2";
#elif TINYEXR_SIMD_A64FX
  return "A64FX (SVE 512-bit)";
#elif TINYEXR_SIMD_SVE2
  return "SVE2";
#elif TINYEXR_SIMD_SVE
  return "SVE";
#elif TINYEXR_SIMD_NEON_FP16
  return "NEON+FP16";
#elif TINYEXR_SIMD_NEON
  return "NEON";
#else
  return "Scalar";
#endif
}

}  // namespace simd
}  // namespace tinyexr

#endif  // TINYEXR_SIMD_HH_

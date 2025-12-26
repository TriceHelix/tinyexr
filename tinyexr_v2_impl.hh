// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2025, Syoyo Fujita and many contributors.
// All rights reserved.
//
// TinyEXR V2 Implementation

#ifndef TINYEXR_V2_IMPL_HH_
#define TINYEXR_V2_IMPL_HH_

#include "tinyexr_v2.hh"
#include <cstring>
#include <algorithm>

// ============================================================================
// Decompression backend configuration
// ============================================================================
//
// V2 API decompression backend priority:
//   1. TINYEXR_V2_USE_CUSTOM_DEFLATE (default=1) - Custom SIMD-optimized deflate
//   2. TINYEXR_USE_ZLIB - System zlib
//   3. TINYEXR_USE_MINIZ - Miniz (bundled)
//
// To use system zlib instead of custom deflate:
//   #define TINYEXR_V2_USE_CUSTOM_DEFLATE 0
//   #define TINYEXR_USE_ZLIB 1
//
// To use miniz instead of custom deflate:
//   #define TINYEXR_V2_USE_CUSTOM_DEFLATE 0
//   #define TINYEXR_USE_MINIZ 1

// Default: use custom SIMD-optimized deflate
#ifndef TINYEXR_V2_USE_CUSTOM_DEFLATE
#define TINYEXR_V2_USE_CUSTOM_DEFLATE 1
#endif

// Custom deflate uses tinyexr_huffman.hh
#if TINYEXR_V2_USE_CUSTOM_DEFLATE
#include "tinyexr_huffman.hh"
#include "tinyexr_piz.hh"
#endif

// Fallback: miniz or zlib for when custom deflate is disabled
#if !TINYEXR_V2_USE_CUSTOM_DEFLATE
#if !defined(TINYEXR_USE_MINIZ) && !defined(TINYEXR_USE_ZLIB)
// Default to zlib if available, otherwise miniz
#if defined(__has_include)
#if __has_include(<zlib.h>)
#define TINYEXR_USE_ZLIB 1
#define TINYEXR_USE_MINIZ 0
#else
#define TINYEXR_USE_ZLIB 0
#define TINYEXR_USE_MINIZ 1
#endif
#else
#define TINYEXR_USE_ZLIB 1
#define TINYEXR_USE_MINIZ 0
#endif
#endif

#if TINYEXR_USE_MINIZ
#include "miniz.h"
#elif TINYEXR_USE_ZLIB
#include <zlib.h>
#endif
#endif

namespace tinyexr {
namespace v2 {

// ============================================================================
// Compression constants
// ============================================================================

enum CompressionType {
  COMPRESSION_NONE = 0,
  COMPRESSION_RLE = 1,
  COMPRESSION_ZIPS = 2,  // ZIP single scanline
  COMPRESSION_ZIP = 3,   // ZIP 16 scanlines
  COMPRESSION_PIZ = 4,
  COMPRESSION_PXR24 = 5,
  COMPRESSION_B44 = 6,
  COMPRESSION_B44A = 7,
  COMPRESSION_DWAA = 8,
  COMPRESSION_DWAB = 9
};

// Pixel types
enum PixelType {
  PIXEL_TYPE_UINT = 0,
  PIXEL_TYPE_HALF = 1,
  PIXEL_TYPE_FLOAT = 2
};

// ============================================================================
// Helper: RLE decompression (from OpenEXR)
// ============================================================================

static int rleUncompress(int inLength, int maxLength,
                         const signed char* in, char* out) {
  char* outStart = out;
  const char* outEnd = out + maxLength;
  const signed char* inEnd = in + inLength;

  while (in < inEnd) {
    if (*in < 0) {
      int count = -static_cast<int>(*in++);
      if (out + count > outEnd) return 0;
      if (in + count > inEnd) return 0;
      std::memcpy(out, in, static_cast<size_t>(count));
      out += count;
      in += count;
    } else {
      int count = static_cast<int>(*in++) + 1;
      if (out + count > outEnd) return 0;
      if (in >= inEnd) return 0;
      std::memset(out, *in++, static_cast<size_t>(count));
      out += count;
    }
  }
  return static_cast<int>(out - outStart);
}

// ============================================================================
// Helper: Decompression functions
// ============================================================================

static bool DecompressZipV2(uint8_t* dst, size_t* uncompressed_size,
                            const uint8_t* src, size_t src_size,
                            ScratchPool& pool) {
  if (*uncompressed_size == src_size) {
    // Not compressed
    std::memcpy(dst, src, src_size);
    return true;
  }

  uint8_t* tmpBuf = pool.get_buffer(*uncompressed_size);

#if TINYEXR_V2_USE_CUSTOM_DEFLATE
  // Use custom SIMD-optimized deflate decoder
  tinyexr::huffman::dfl::DeflateOptions opts;
  opts.max_output_size = *uncompressed_size;
  auto result = tinyexr::huffman::dfl::inflate_zlib_safe(
      src, src_size, tmpBuf, *uncompressed_size, opts);
  if (!result.success) {
    return false;
  }
  *uncompressed_size = result.bytes_written;
#elif TINYEXR_USE_MINIZ
  mz_ulong dest_len = static_cast<mz_ulong>(*uncompressed_size);
  int ret = mz_uncompress(tmpBuf, &dest_len, src, static_cast<mz_ulong>(src_size));
  if (ret != MZ_OK) {
    return false;
  }
  *uncompressed_size = static_cast<size_t>(dest_len);
#elif TINYEXR_USE_ZLIB
  uLongf dest_len = static_cast<uLongf>(*uncompressed_size);
  int ret = uncompress(tmpBuf, &dest_len, src, static_cast<uLong>(src_size));
  if (ret != Z_OK) {
    return false;
  }
  *uncompressed_size = static_cast<size_t>(dest_len);
#else
  // Fallback - no decompression backend available
  (void)tmpBuf; (void)src; (void)src_size;
  return false;
#endif

  // Predictor (using optimized version if available)
#if defined(TINYEXR_ENABLE_SIMD) && TINYEXR_ENABLE_SIMD
  tinyexr::simd::apply_delta_predictor_fast(tmpBuf, *uncompressed_size);
#else
  if (*uncompressed_size > 1) {
    uint8_t* t = tmpBuf + 1;
    uint8_t* stop = tmpBuf + *uncompressed_size;
    while (t < stop) {
      int d = static_cast<int>(t[-1]) + static_cast<int>(t[0]) - 128;
      t[0] = static_cast<uint8_t>(d);
      ++t;
    }
  }
#endif

  // Reorder (using optimized version if available)
#if defined(TINYEXR_ENABLE_SIMD) && TINYEXR_ENABLE_SIMD
  tinyexr::simd::unreorder_bytes_after_decompression(tmpBuf, dst, *uncompressed_size);
#else
  {
    const uint8_t* t1 = tmpBuf;
    const uint8_t* t2 = tmpBuf + (*uncompressed_size + 1) / 2;
    uint8_t* s = dst;
    uint8_t* stop = s + *uncompressed_size;
    while (s < stop) {
      if (s < stop) *s++ = *t1++;
      if (s < stop) *s++ = *t2++;
    }
  }
#endif

  return true;
}

static bool DecompressRleV2(uint8_t* dst, size_t uncompressed_size,
                            const uint8_t* src, size_t src_size,
                            ScratchPool& pool) {
  if (uncompressed_size == src_size) {
    std::memcpy(dst, src, src_size);
    return true;
  }

  if (src_size <= 2) {
    return false;
  }

  uint8_t* tmpBuf = pool.get_buffer(uncompressed_size);

  int ret = rleUncompress(static_cast<int>(src_size),
                          static_cast<int>(uncompressed_size),
                          reinterpret_cast<const signed char*>(src),
                          reinterpret_cast<char*>(tmpBuf));
  if (ret != static_cast<int>(uncompressed_size)) {
    return false;
  }

  // Predictor
#if defined(TINYEXR_ENABLE_SIMD) && TINYEXR_ENABLE_SIMD
  tinyexr::simd::apply_delta_predictor_fast(tmpBuf, uncompressed_size);
#else
  if (uncompressed_size > 1) {
    uint8_t* t = tmpBuf + 1;
    uint8_t* stop = tmpBuf + uncompressed_size;
    while (t < stop) {
      int d = static_cast<int>(t[-1]) + static_cast<int>(t[0]) - 128;
      t[0] = static_cast<uint8_t>(d);
      ++t;
    }
  }
#endif

  // Reorder
#if defined(TINYEXR_ENABLE_SIMD) && TINYEXR_ENABLE_SIMD
  tinyexr::simd::unreorder_bytes_after_decompression(tmpBuf, dst, uncompressed_size);
#else
  {
    const uint8_t* t1 = tmpBuf;
    const uint8_t* t2 = tmpBuf + (uncompressed_size + 1) / 2;
    uint8_t* s = dst;
    uint8_t* stop = s + uncompressed_size;
    while (s < stop) {
      if (s < stop) *s++ = *t1++;
      if (s < stop) *s++ = *t2++;
    }
  }
#endif

  return true;
}

// ============================================================================
// Helper: FP16 to FP32 conversion
// ============================================================================

static float HalfToFloat(uint16_t h) {
#if defined(TINYEXR_ENABLE_SIMD) && TINYEXR_ENABLE_SIMD
  return tinyexr::simd::half_to_float_scalar(h);
#else
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
#endif
}

// ============================================================================
// Helper: Get scanlines per block for compression type
// ============================================================================

static int GetScanlinesPerBlock(int compression) {
  switch (compression) {
    case COMPRESSION_NONE:
    case COMPRESSION_RLE:
    case COMPRESSION_ZIPS:
      return 1;
    case COMPRESSION_ZIP:
      return 16;
    case COMPRESSION_PIZ:
      return 32;
    case COMPRESSION_PXR24:
      return 16;
    case COMPRESSION_B44:
    case COMPRESSION_B44A:
      return 32;
    case COMPRESSION_DWAA:
      return 32;
    case COMPRESSION_DWAB:
      return 256;
    default:
      return 1;
  }
}

// ============================================================================
// Implementation of parser functions
// ============================================================================

Result<Version> ParseVersion(Reader& reader) {
  reader.set_context("Parsing EXR version header");

  Version version;

  // Check minimum size
  if (reader.length() < 8) {
    return Result<Version>::error(
      ErrorInfo(ErrorCode::InvalidData,
                "File too small to contain EXR version header (need 8 bytes, got " +
                std::to_string(reader.length()) + " bytes)",
                reader.context(),
                0));
  }

  // Read and check magic number: 0x76 0x2f 0x31 0x01
  uint8_t magic[4];
  if (!reader.read(4, magic)) {
    return Result<Version>::error(reader.last_error());
  }

  const uint8_t expected_magic[] = {0x76, 0x2f, 0x31, 0x01};
  for (int i = 0; i < 4; i++) {
    if (magic[i] != expected_magic[i]) {
      char buf[256];
      snprintf(buf, sizeof(buf),
               "Invalid EXR magic number. Expected [0x76 0x2f 0x31 0x01], "
               "got [0x%02x 0x%02x 0x%02x 0x%02x]. "
               "This is not a valid OpenEXR file.",
               magic[0], magic[1], magic[2], magic[3]);
      return Result<Version>::error(
        ErrorInfo(ErrorCode::InvalidMagicNumber, buf, reader.context(), 0));
    }
  }

  // Read version byte (must be 2)
  uint8_t version_byte;
  if (!reader.read1(&version_byte)) {
    return Result<Version>::error(reader.last_error());
  }

  if (version_byte != 2) {
    char buf[128];
    snprintf(buf, sizeof(buf),
             "Unsupported EXR version %d. Only version 2 is supported.",
             version_byte);
    return Result<Version>::error(
      ErrorInfo(ErrorCode::InvalidVersion, buf, reader.context(), 4));
  }

  version.version = 2;

  // Read flags byte
  uint8_t flags;
  if (!reader.read1(&flags)) {
    return Result<Version>::error(reader.last_error());
  }

  version.tiled = (flags & 0x02) != 0;       // bit 1 (9th bit of file)
  version.long_name = (flags & 0x04) != 0;   // bit 2 (10th bit of file)
  version.non_image = (flags & 0x08) != 0;   // bit 3 (11th bit of file, deep data)
  version.multipart = (flags & 0x10) != 0;   // bit 4 (12th bit of file)

  // Read remaining 2 bytes to complete 8-byte header
  uint8_t padding[2];
  if (!reader.read(2, padding)) {
    return Result<Version>::error(reader.last_error());
  }

  // Create result with informational warnings
  Result<Version> result = Result<Version>::ok(version);

  if (version.tiled) {
    result.add_warning("File uses tiled format");
  }
  if (version.long_name) {
    result.add_warning("File uses long attribute names (>255 chars)");
  }
  if (version.non_image) {
    result.add_warning("File contains deep/non-image data");
  }
  if (version.multipart) {
    result.add_warning("File is multipart format");
  }

  return result;
}

Result<Header> ParseHeader(Reader& reader, const Version& version) {
  reader.set_context("Parsing EXR header attributes");

  Header header;
  header.tiled = version.tiled;

  // Required attributes according to OpenEXR spec
  bool has_channels = false;
  bool has_compression = false;
  bool has_data_window = false;
  bool has_display_window = false;
  bool has_line_order = false;
  bool has_pixel_aspect_ratio = false;
  bool has_screen_window_center = false;
  bool has_screen_window_width = false;

  size_t header_start = reader.tell();

  // Read attributes until we hit null terminator
  for (int attr_count = 0; attr_count < 1024; attr_count++) {  // Safety limit
    // Check for end of header (null byte)
    size_t attr_start = reader.tell();
    uint8_t first_byte;
    if (!reader.read1(&first_byte)) {
      return Result<Header>::error(reader.last_error());
    }

    if (first_byte == 0) {
      // End of header
      break;
    }

    // Rewind to read full attribute name
    reader.seek(attr_start);

    // Read attribute name
    std::string attr_name;
    if (!reader.read_string(&attr_name, 256)) {
      return Result<Header>::error(
        ErrorInfo(ErrorCode::InvalidData,
                  "Failed to read attribute name at position " +
                  std::to_string(attr_start),
                  reader.context(),
                  attr_start));
    }

    // Read attribute type
    std::string attr_type;
    if (!reader.read_string(&attr_type, 256)) {
      return Result<Header>::error(
        ErrorInfo(ErrorCode::InvalidData,
                  "Failed to read attribute type for '" + attr_name + "'",
                  reader.context(),
                  reader.tell()));
    }

    // Read attribute data size
    uint32_t data_size;
    if (!reader.read4(&data_size)) {
      return Result<Header>::error(reader.last_error());
    }

    // Sanity check on data size (max 10MB per attribute)
    if (data_size > 10 * 1024 * 1024) {
      char buf[256];
      snprintf(buf, sizeof(buf),
               "Attribute '%s' has unreasonably large size %u bytes. "
               "Possible file corruption.",
               attr_name.c_str(), data_size);
      return Result<Header>::error(
        ErrorInfo(ErrorCode::InvalidData, buf, reader.context(), reader.tell() - 4));
    }

    size_t data_start = reader.tell();

    // Parse specific attributes we care about
    if (attr_name == "channels" && attr_type == "chlist") {
      has_channels = true;

      // Parse channel list
      size_t chlist_end = reader.tell() + data_size;

      while (reader.tell() < chlist_end) {
        // Check for null terminator (end of channel list)
        uint8_t name_first;
        size_t name_start = reader.tell();
        if (!reader.read1(&name_first)) {
          return Result<Header>::error(reader.last_error());
        }
        if (name_first == 0) {
          break;  // End of channel list
        }
        reader.seek(name_start);

        // Read channel name
        std::string channel_name;
        if (!reader.read_string(&channel_name, 256)) {
          return Result<Header>::error(reader.last_error());
        }

        Channel ch;
        ch.name = channel_name;

        // Read pixel type (4 bytes)
        uint32_t pixel_type;
        if (!reader.read4(&pixel_type)) {
          return Result<Header>::error(reader.last_error());
        }
        ch.pixel_type = static_cast<int>(pixel_type);

        // Read pLinear (1 byte) + reserved (3 bytes)
        uint8_t plinear;
        if (!reader.read1(&plinear)) {
          return Result<Header>::error(reader.last_error());
        }
        ch.p_linear = (plinear != 0);

        // Skip reserved (3 bytes)
        uint8_t reserved[3];
        if (!reader.read(3, reserved)) {
          return Result<Header>::error(reader.last_error());
        }

        // Read x sampling (4 bytes)
        uint32_t x_sampling;
        if (!reader.read4(&x_sampling)) {
          return Result<Header>::error(reader.last_error());
        }
        ch.x_sampling = static_cast<int>(x_sampling);

        // Read y sampling (4 bytes)
        uint32_t y_sampling;
        if (!reader.read4(&y_sampling)) {
          return Result<Header>::error(reader.last_error());
        }
        ch.y_sampling = static_cast<int>(y_sampling);

        header.channels.push_back(ch);
      }

      // Sort channels by name for consistent ordering
      std::sort(header.channels.begin(), header.channels.end(),
                [](const Channel& a, const Channel& b) {
                  return a.name < b.name;
                });

      // Ensure we're at the end of the attribute data
      reader.seek(data_start + data_size);
    }
    else if (attr_name == "compression" && attr_type == "compression") {
      has_compression = true;
      if (data_size != 1) {
        return Result<Header>::error(
          ErrorInfo(ErrorCode::InvalidData,
                    "Compression attribute must be 1 byte",
                    reader.context(),
                    data_start));
      }
      uint8_t comp;
      if (!reader.read1(&comp)) {
        return Result<Header>::error(reader.last_error());
      }
      header.compression = comp;
    }
    else if (attr_name == "dataWindow" && attr_type == "box2i") {
      has_data_window = true;
      if (data_size != 16) {
        return Result<Header>::error(
          ErrorInfo(ErrorCode::InvalidData,
                    "dataWindow attribute must be 16 bytes (4 ints)",
                    reader.context(),
                    data_start));
      }
      uint32_t vals[4];
      for (int i = 0; i < 4; i++) {
        if (!reader.read4(&vals[i])) {
          return Result<Header>::error(reader.last_error());
        }
      }
      header.data_window.min_x = static_cast<int>(vals[0]);
      header.data_window.min_y = static_cast<int>(vals[1]);
      header.data_window.max_x = static_cast<int>(vals[2]);
      header.data_window.max_y = static_cast<int>(vals[3]);
    }
    else if (attr_name == "displayWindow" && attr_type == "box2i") {
      has_display_window = true;
      if (data_size != 16) {
        return Result<Header>::error(
          ErrorInfo(ErrorCode::InvalidData,
                    "displayWindow attribute must be 16 bytes",
                    reader.context(),
                    data_start));
      }
      uint32_t vals[4];
      for (int i = 0; i < 4; i++) {
        if (!reader.read4(&vals[i])) {
          return Result<Header>::error(reader.last_error());
        }
      }
      header.display_window.min_x = static_cast<int>(vals[0]);
      header.display_window.min_y = static_cast<int>(vals[1]);
      header.display_window.max_x = static_cast<int>(vals[2]);
      header.display_window.max_y = static_cast<int>(vals[3]);
    }
    else if (attr_name == "lineOrder" && attr_type == "lineOrder") {
      has_line_order = true;
      if (data_size != 1) {
        return Result<Header>::error(
          ErrorInfo(ErrorCode::InvalidData,
                    "lineOrder attribute must be 1 byte",
                    reader.context(),
                    data_start));
      }
      uint8_t lo;
      if (!reader.read1(&lo)) {
        return Result<Header>::error(reader.last_error());
      }
      header.line_order = lo;
    }
    else if (attr_name == "pixelAspectRatio" && attr_type == "float") {
      has_pixel_aspect_ratio = true;
      if (data_size != 4) {
        return Result<Header>::error(
          ErrorInfo(ErrorCode::InvalidData,
                    "pixelAspectRatio must be 4 bytes (float)",
                    reader.context(),
                    data_start));
      }
      uint32_t bits;
      if (!reader.read4(&bits)) {
        return Result<Header>::error(reader.last_error());
      }
      std::memcpy(&header.pixel_aspect_ratio, &bits, 4);
    }
    else if (attr_name == "screenWindowCenter" && attr_type == "v2f") {
      has_screen_window_center = true;
      if (data_size != 8) {
        return Result<Header>::error(
          ErrorInfo(ErrorCode::InvalidData,
                    "screenWindowCenter must be 8 bytes (2 floats)",
                    reader.context(),
                    data_start));
      }
      for (int i = 0; i < 2; i++) {
        uint32_t bits;
        if (!reader.read4(&bits)) {
          return Result<Header>::error(reader.last_error());
        }
        std::memcpy(&header.screen_window_center[i], &bits, 4);
      }
    }
    else if (attr_name == "screenWindowWidth" && attr_type == "float") {
      has_screen_window_width = true;
      if (data_size != 4) {
        return Result<Header>::error(
          ErrorInfo(ErrorCode::InvalidData,
                    "screenWindowWidth must be 4 bytes (float)",
                    reader.context(),
                    data_start));
      }
      uint32_t bits;
      if (!reader.read4(&bits)) {
        return Result<Header>::error(reader.last_error());
      }
      std::memcpy(&header.screen_window_width, &bits, 4);
    }
    else {
      // Unknown attribute - skip it
      if (!reader.seek_relative(data_size)) {
        return Result<Header>::error(reader.last_error());
      }
    }
  }

  // Check for required attributes
  Result<Header> result = Result<Header>::ok(header);

  if (!has_channels) {
    result.add_error(ErrorInfo(ErrorCode::MissingRequiredAttribute,
                               "Required attribute 'channels' not found",
                               reader.context(), header_start));
  }
  if (!has_compression) {
    result.add_error(ErrorInfo(ErrorCode::MissingRequiredAttribute,
                               "Required attribute 'compression' not found",
                               reader.context(), header_start));
  }
  if (!has_data_window) {
    result.add_error(ErrorInfo(ErrorCode::MissingRequiredAttribute,
                               "Required attribute 'dataWindow' not found",
                               reader.context(), header_start));
  }
  if (!has_display_window) {
    result.add_error(ErrorInfo(ErrorCode::MissingRequiredAttribute,
                               "Required attribute 'displayWindow' not found",
                               reader.context(), header_start));
  }
  if (!has_line_order) {
    result.add_error(ErrorInfo(ErrorCode::MissingRequiredAttribute,
                               "Required attribute 'lineOrder' not found",
                               reader.context(), header_start));
  }
  if (!has_pixel_aspect_ratio) {
    result.add_error(ErrorInfo(ErrorCode::MissingRequiredAttribute,
                               "Required attribute 'pixelAspectRatio' not found",
                               reader.context(), header_start));
  }
  if (!has_screen_window_center) {
    result.add_error(ErrorInfo(ErrorCode::MissingRequiredAttribute,
                               "Required attribute 'screenWindowCenter' not found",
                               reader.context(), header_start));
  }
  if (!has_screen_window_width) {
    result.add_error(ErrorInfo(ErrorCode::MissingRequiredAttribute,
                               "Required attribute 'screenWindowWidth' not found",
                               reader.context(), header_start));
  }

  if (!result.success) {
    return result;
  }

  header.header_len = reader.tell() - header_start;
  return result;
}

Result<ImageData> LoadFromMemory(const uint8_t* data, size_t size) {
  if (!data) {
    return Result<ImageData>::error(
      ErrorInfo(ErrorCode::InvalidArgument,
                "Null data pointer passed to LoadFromMemory",
                "LoadFromMemory", 0));
  }

  if (size == 0) {
    return Result<ImageData>::error(
      ErrorInfo(ErrorCode::InvalidArgument,
                "Zero size passed to LoadFromMemory",
                "LoadFromMemory", 0));
  }

  Reader reader(data, size, Endian::Little);

  // Parse version
  Result<Version> version_result = ParseVersion(reader);
  if (!version_result.success) {
    Result<ImageData> result;
    result.success = false;
    result.errors = version_result.errors;
    result.warnings = version_result.warnings;
    return result;
  }

  // Parse header
  Result<Header> header_result = ParseHeader(reader, version_result.value);
  if (!header_result.success) {
    Result<ImageData> result;
    result.success = false;
    result.errors = header_result.errors;
    result.warnings = header_result.warnings;
    return result;
  }

  // Setup image data
  ImageData img_data;
  img_data.header = header_result.value;
  img_data.width = header_result.value.data_window.width();
  img_data.height = header_result.value.data_window.height();
  img_data.num_channels = static_cast<int>(header_result.value.channels.size());

  const Header& hdr = header_result.value;
  int width = img_data.width;
  int height = img_data.height;

  // Allocate RGBA output buffer
  img_data.rgba.resize(static_cast<size_t>(width) * static_cast<size_t>(height) * 4, 0.0f);

  // Calculate bytes per pixel for each channel and total
  size_t bytes_per_pixel = 0;
  std::vector<size_t> channel_offsets;
  std::vector<int> channel_sizes;  // bytes per pixel for each channel

  for (size_t i = 0; i < hdr.channels.size(); i++) {
    channel_offsets.push_back(bytes_per_pixel);
    int sz = 0;
    switch (hdr.channels[i].pixel_type) {
      case PIXEL_TYPE_UINT:  sz = 4; break;
      case PIXEL_TYPE_HALF:  sz = 2; break;
      case PIXEL_TYPE_FLOAT: sz = 4; break;
      default: sz = 4; break;
    }
    channel_sizes.push_back(sz);
    bytes_per_pixel += static_cast<size_t>(sz);
  }

  size_t pixel_data_size = bytes_per_pixel * static_cast<size_t>(width);
  int scanlines_per_block = GetScanlinesPerBlock(hdr.compression);
  int num_blocks = (height + scanlines_per_block - 1) / scanlines_per_block;

  // Check compression type support
  if (hdr.compression != COMPRESSION_NONE &&
      hdr.compression != COMPRESSION_RLE &&
      hdr.compression != COMPRESSION_ZIPS &&
      hdr.compression != COMPRESSION_ZIP &&
      hdr.compression != COMPRESSION_PIZ) {
    Result<ImageData> result = Result<ImageData>::ok(img_data);
    result.warnings = version_result.warnings;
    for (size_t i = 0; i < header_result.warnings.size(); i++) {
      result.warnings.push_back(header_result.warnings[i]);
    }
    result.add_warning("Compression type " + std::to_string(hdr.compression) +
                       " not yet supported in V2 API. Pixel data not loaded.");
    return result;
  }

  // Read offset table
  reader.set_context("Reading offset table");
  std::vector<uint64_t> offsets(static_cast<size_t>(num_blocks));
  for (int i = 0; i < num_blocks; i++) {
    uint64_t offset;
    if (!reader.read8(&offset)) {
      Result<ImageData> result;
      result.success = false;
      result.errors.push_back(ErrorInfo(ErrorCode::InvalidData,
                                        "Failed to read offset table entry " + std::to_string(i),
                                        reader.context(), reader.tell()));
      return result;
    }
    offsets[static_cast<size_t>(i)] = offset;
  }

  // Get scratch pool for decompression
  ScratchPool& pool = get_scratch_pool();

  // Map channel names to output indices (RGBA)
  auto GetOutputIndex = [&](const std::string& name) -> int {
    if (name == "R" || name == "r") return 0;
    if (name == "G" || name == "g") return 1;
    if (name == "B" || name == "b") return 2;
    if (name == "A" || name == "a") return 3;
    if (name == "Y" || name == "y") return 0;  // Luminance -> R
    return -1;  // Unknown channel
  };

  // Build channel mapping
  std::vector<int> channel_output_idx;
  for (size_t i = 0; i < hdr.channels.size(); i++) {
    channel_output_idx.push_back(GetOutputIndex(hdr.channels[i].name));
  }

  // Decompress buffer
  std::vector<uint8_t> decomp_buf(pixel_data_size * static_cast<size_t>(scanlines_per_block));

  // Process each scanline block
  reader.set_context("Decoding scanline data");

  for (int block = 0; block < num_blocks; block++) {
    // Seek to block
    if (!reader.seek(static_cast<size_t>(offsets[static_cast<size_t>(block)]))) {
      Result<ImageData> result;
      result.success = false;
      result.errors.push_back(ErrorInfo(ErrorCode::OutOfBounds,
                                        "Failed to seek to block " + std::to_string(block),
                                        reader.context(), reader.tell()));
      return result;
    }

    // Read y coordinate (4 bytes)
    uint32_t y_coord;
    if (!reader.read4(&y_coord)) {
      Result<ImageData> result;
      result.success = false;
      result.errors.push_back(reader.last_error());
      return result;
    }

    // Read data size (4 bytes)
    uint32_t data_size;
    if (!reader.read4(&data_size)) {
      Result<ImageData> result;
      result.success = false;
      result.errors.push_back(reader.last_error());
      return result;
    }

    // Calculate number of scanlines in this block
    int y_start = static_cast<int>(y_coord) - hdr.data_window.min_y;
    int num_lines = std::min(scanlines_per_block, height - y_start);
    if (num_lines <= 0) continue;

    size_t expected_size = pixel_data_size * static_cast<size_t>(num_lines);

    // Read compressed data
    const uint8_t* block_data = data + reader.tell();
    if (reader.tell() + data_size > size) {
      Result<ImageData> result;
      result.success = false;
      result.errors.push_back(ErrorInfo(ErrorCode::OutOfBounds,
                                        "Block data exceeds file size",
                                        reader.context(), reader.tell()));
      return result;
    }

    // Decompress
    bool decomp_ok = false;
    switch (hdr.compression) {
      case COMPRESSION_NONE:
        if (data_size == expected_size) {
          std::memcpy(decomp_buf.data(), block_data, expected_size);
          decomp_ok = true;
        }
        break;

      case COMPRESSION_RLE:
        decomp_ok = DecompressRleV2(decomp_buf.data(), expected_size,
                                     block_data, data_size, pool);
        break;

      case COMPRESSION_ZIPS:
      case COMPRESSION_ZIP: {
        size_t uncomp_size = expected_size;
        decomp_ok = DecompressZipV2(decomp_buf.data(), &uncomp_size,
                                     block_data, data_size, pool);
        break;
      }

#if TINYEXR_V2_USE_CUSTOM_DEFLATE
      case COMPRESSION_PIZ: {
        auto piz_result = tinyexr::piz::DecompressPizV2(
            decomp_buf.data(), expected_size,
            block_data, data_size,
            static_cast<int>(hdr.channels.size()), hdr.channels.data(),
            width, num_lines);
        decomp_ok = piz_result.success;
        break;
      }
#endif

      default:
        decomp_ok = false;
        break;
    }

    if (!decomp_ok) {
      Result<ImageData> result;
      result.success = false;
      result.errors.push_back(ErrorInfo(ErrorCode::CompressionError,
                                        "Failed to decompress block " + std::to_string(block),
                                        reader.context(), reader.tell()));
      return result;
    }

    // Convert pixel data to RGBA float
    for (int line = 0; line < num_lines; line++) {
      int y = y_start + line;
      if (y < 0 || y >= height) continue;

      const uint8_t* line_data = decomp_buf.data() + static_cast<size_t>(line) * pixel_data_size;
      float* out_line = img_data.rgba.data() + static_cast<size_t>(y) * static_cast<size_t>(width) * 4;

      for (int x = 0; x < width; x++) {
        const uint8_t* pixel = line_data + static_cast<size_t>(x) * bytes_per_pixel;

        // Process each channel
        for (size_t c = 0; c < hdr.channels.size(); c++) {
          int out_idx = channel_output_idx[c];
          if (out_idx < 0 || out_idx > 3) continue;

          const uint8_t* ch_data = pixel + channel_offsets[c];
          float val = 0.0f;

          switch (hdr.channels[c].pixel_type) {
            case PIXEL_TYPE_UINT: {
              uint32_t u;
              std::memcpy(&u, ch_data, 4);
              val = static_cast<float>(u) / 4294967295.0f;  // Normalize to [0,1]
              break;
            }
            case PIXEL_TYPE_HALF: {
              uint16_t h;
              std::memcpy(&h, ch_data, 2);
              val = HalfToFloat(h);
              break;
            }
            case PIXEL_TYPE_FLOAT: {
              std::memcpy(&val, ch_data, 4);
              break;
            }
          }

          out_line[x * 4 + out_idx] = val;
        }

        // Set alpha to 1.0 if not present
        bool has_alpha = false;
        for (size_t c = 0; c < hdr.channels.size(); c++) {
          if (channel_output_idx[c] == 3) {
            has_alpha = true;
            break;
          }
        }
        if (!has_alpha) {
          out_line[x * 4 + 3] = 1.0f;
        }
      }
    }

    reader.seek_relative(static_cast<int64_t>(data_size));
  }

  Result<ImageData> result = Result<ImageData>::ok(img_data);

  // Carry forward warnings
  result.warnings = version_result.warnings;
  for (size_t i = 0; i < header_result.warnings.size(); i++) {
    result.warnings.push_back(header_result.warnings[i]);
  }

  return result;
}

// ============================================================================
// Writer implementations
// ============================================================================

Result<void> WriteVersion(Writer& writer, const Version& version) {
  writer.set_context("Writing EXR version header");

  // Write magic number: 0x76 0x2f 0x31 0x01
  const uint8_t magic[] = {0x76, 0x2f, 0x31, 0x01};
  if (!writer.write(4, magic)) {
    return Result<void>::error(writer.last_error());
  }

  // Write version byte (must be 2)
  if (!writer.write1(static_cast<uint8_t>(version.version))) {
    return Result<void>::error(writer.last_error());
  }

  // Build flags byte
  uint8_t flags = 0;
  if (version.tiled) flags |= 0x02;       // bit 1
  if (version.long_name) flags |= 0x04;   // bit 2
  if (version.non_image) flags |= 0x08;   // bit 3
  if (version.multipart) flags |= 0x10;   // bit 4

  if (!writer.write1(flags)) {
    return Result<void>::error(writer.last_error());
  }

  // Write 2 bytes padding to complete 8-byte header
  const uint8_t padding[2] = {0, 0};
  if (!writer.write(2, padding)) {
    return Result<void>::error(writer.last_error());
  }

  return Result<void>::ok();
}

Result<void> WriteHeader(Writer& writer, const Header& header) {
  writer.set_context("Writing EXR header attributes");

  Result<void> result = Result<void>::ok();

  // Write compression attribute
  if (!writer.write_string("compression")) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write_string("compression")) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write4(1)) {  // data size = 1 byte
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write1(static_cast<uint8_t>(header.compression))) {
    return Result<void>::error(writer.last_error());
  }

  // Write dataWindow attribute
  if (!writer.write_string("dataWindow")) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write_string("box2i")) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write4(16)) {  // data size = 16 bytes (4 ints)
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write4(static_cast<uint32_t>(header.data_window.min_x))) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write4(static_cast<uint32_t>(header.data_window.min_y))) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write4(static_cast<uint32_t>(header.data_window.max_x))) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write4(static_cast<uint32_t>(header.data_window.max_y))) {
    return Result<void>::error(writer.last_error());
  }

  // Write displayWindow attribute
  if (!writer.write_string("displayWindow")) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write_string("box2i")) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write4(16)) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write4(static_cast<uint32_t>(header.display_window.min_x))) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write4(static_cast<uint32_t>(header.display_window.min_y))) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write4(static_cast<uint32_t>(header.display_window.max_x))) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write4(static_cast<uint32_t>(header.display_window.max_y))) {
    return Result<void>::error(writer.last_error());
  }

  // Write lineOrder attribute
  if (!writer.write_string("lineOrder")) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write_string("lineOrder")) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write4(1)) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write1(static_cast<uint8_t>(header.line_order))) {
    return Result<void>::error(writer.last_error());
  }

  // Write pixelAspectRatio attribute
  if (!writer.write_string("pixelAspectRatio")) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write_string("float")) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write4(4)) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write_float(header.pixel_aspect_ratio)) {
    return Result<void>::error(writer.last_error());
  }

  // Write screenWindowCenter attribute
  if (!writer.write_string("screenWindowCenter")) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write_string("v2f")) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write4(8)) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write_float(header.screen_window_center[0])) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write_float(header.screen_window_center[1])) {
    return Result<void>::error(writer.last_error());
  }

  // Write screenWindowWidth attribute
  if (!writer.write_string("screenWindowWidth")) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write_string("float")) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write4(4)) {
    return Result<void>::error(writer.last_error());
  }
  if (!writer.write_float(header.screen_window_width)) {
    return Result<void>::error(writer.last_error());
  }

  // Write end-of-header marker (null byte)
  if (!writer.write1(0)) {
    return Result<void>::error(writer.last_error());
  }

  result.add_warning("Minimal header written - channels attribute not yet implemented");

  return result;
}

Result<std::vector<uint8_t>> SaveToMemory(const ImageData& image) {
  Writer writer;
  writer.set_context("Saving EXR to memory");

  // Create version
  Version version;
  version.version = 2;
  version.tiled = image.header.tiled;
  version.long_name = false;
  version.non_image = false;
  version.multipart = false;

  // Write version
  Result<void> version_result = WriteVersion(writer, version);
  if (!version_result.success) {
    Result<std::vector<uint8_t>> result;
    result.success = false;
    result.errors = version_result.errors;
    return result;
  }

  // Write header
  Result<void> header_result = WriteHeader(writer, image.header);
  if (!header_result.success) {
    Result<std::vector<uint8_t>> result;
    result.success = false;
    result.errors = header_result.errors;
    result.warnings = version_result.warnings;
    return result;
  }

  // Return data
  Result<std::vector<uint8_t>> result = Result<std::vector<uint8_t>>::ok(writer.data());

  // Carry forward warnings
  for (size_t i = 0; i < version_result.warnings.size(); i++) {
    result.warnings.push_back(version_result.warnings[i]);
  }
  for (size_t i = 0; i < header_result.warnings.size(); i++) {
    result.warnings.push_back(header_result.warnings[i]);
  }

  result.add_warning("Image pixel data writing not yet implemented in v2 API");

  return result;
}

}  // namespace v2
}  // namespace tinyexr

#endif  // TINYEXR_V2_IMPL_HH_

// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2025, Syoyo Fujita and many contributors.
// All rights reserved.
//
// TinyEXR V2 API: Refactored loader with enhanced error reporting
//
// ============================================================================
// WARNING: EXPERIMENTAL API - NOT FOR PRODUCTION USE
// ============================================================================
// This is an experimental API that may change without notice.
// For production use, please use the stable V1 API in tinyexr.h
//
// This is a new API that allows breaking changes from the original tinyexr.h
// for better safety, error handling, and human-readable error messages.
// ============================================================================

#ifndef TINYEXR_V2_HH_
#define TINYEXR_V2_HH_

#include <cstdint>
#include <string>
#include <vector>
#include "streamreader.hh"
#include "streamwriter.hh"

// SIMD optimizations (optional, define TINYEXR_ENABLE_SIMD=1 to enable)
#if defined(TINYEXR_ENABLE_SIMD) && TINYEXR_ENABLE_SIMD
#include "tinyexr_simd.hh"
#include "tinyexr_huffman.hh"
#endif

namespace tinyexr {
namespace v2 {

// ============================================================================
// Error reporting
// ============================================================================

enum class ErrorCode {
  Success = 0,
  InvalidArgument,
  InvalidMagicNumber,
  InvalidVersion,
  InvalidData,
  OutOfBounds,
  UnsupportedFormat,
  CompressionError,
  MissingRequiredAttribute,
  AllocationError,
  IOError
};

// Human-readable error information
struct ErrorInfo {
  ErrorCode code;
  std::string message;       // Human-readable description
  std::string context;       // Where/what was being processed
  size_t byte_position;      // Position in stream where error occurred
  int line_number;           // For code context (optional)

  ErrorInfo()
    : code(ErrorCode::Success), byte_position(0), line_number(0) {}

  ErrorInfo(ErrorCode c, const std::string& msg, const std::string& ctx = "",
            size_t pos = 0)
    : code(c), message(msg), context(ctx), byte_position(pos), line_number(0) {}

  // Format as human-readable string
  std::string to_string() const {
    std::string result;

    // Error type
    result += "[ERROR] ";
    switch (code) {
      case ErrorCode::Success:
        result += "Success";
        break;
      case ErrorCode::InvalidArgument:
        result += "Invalid Argument";
        break;
      case ErrorCode::InvalidMagicNumber:
        result += "Invalid Magic Number";
        break;
      case ErrorCode::InvalidVersion:
        result += "Invalid Version";
        break;
      case ErrorCode::InvalidData:
        result += "Invalid Data";
        break;
      case ErrorCode::OutOfBounds:
        result += "Out of Bounds";
        break;
      case ErrorCode::UnsupportedFormat:
        result += "Unsupported Format";
        break;
      case ErrorCode::CompressionError:
        result += "Compression Error";
        break;
      case ErrorCode::MissingRequiredAttribute:
        result += "Missing Required Attribute";
        break;
      case ErrorCode::AllocationError:
        result += "Allocation Error";
        break;
      case ErrorCode::IOError:
        result += "I/O Error";
        break;
    }
    result += "\n";

    // Message
    if (!message.empty()) {
      result += "  Message: " + message + "\n";
    }

    // Context
    if (!context.empty()) {
      result += "  Context: " + context + "\n";
    }

    // Position
    if (byte_position > 0) {
      result += "  Position: byte " + std::to_string(byte_position) +
                " (0x" + to_hex(byte_position) + ")\n";
    }

    return result;
  }

private:
  static std::string to_hex(size_t val) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%zx", val);
    return std::string(buf);
  }
};

// Result type that contains either success or error information
template<typename T>
struct Result {
  bool success;
  T value;
  std::vector<ErrorInfo> errors;  // Can have multiple errors
  std::vector<std::string> warnings;

  Result() : success(false) {}

  // Success constructor
  static Result<T> ok(const T& val) {
    Result<T> r;
    r.success = true;
    r.value = val;
    return r;
  }

  // Error constructor
  static Result<T> error(const ErrorInfo& err) {
    Result<T> r;
    r.success = false;
    r.errors.push_back(err);
    return r;
  }

  // Add error
  void add_error(const ErrorInfo& err) {
    errors.push_back(err);
    success = false;
  }

  // Add warning
  void add_warning(const std::string& msg) {
    warnings.push_back(msg);
  }

  // Get first error
  ErrorInfo first_error() const {
    return errors.empty() ? ErrorInfo() : errors[0];
  }

  // Get all errors as human-readable string
  std::string error_string() const {
    std::string result;
    for (size_t i = 0; i < errors.size(); i++) {
      if (i > 0) result += "\n";
      result += errors[i].to_string();
    }
    return result;
  }

  // Get all warnings as string
  std::string warnings_string() const {
    std::string result;
    for (size_t i = 0; i < warnings.size(); i++) {
      if (i > 0) result += "\n";
      result += "[WARNING] " + warnings[i];
    }
    return result;
  }
};

// Specialization for void
template<>
struct Result<void> {
  bool success;
  std::vector<ErrorInfo> errors;
  std::vector<std::string> warnings;

  Result() : success(true) {}

  static Result<void> ok() {
    Result<void> r;
    r.success = true;
    return r;
  }

  static Result<void> error(const ErrorInfo& err) {
    Result<void> r;
    r.success = false;
    r.errors.push_back(err);
    return r;
  }

  void add_error(const ErrorInfo& err) {
    errors.push_back(err);
    success = false;
  }

  void add_warning(const std::string& msg) {
    warnings.push_back(msg);
  }

  ErrorInfo first_error() const {
    return errors.empty() ? ErrorInfo() : errors[0];
  }

  std::string error_string() const {
    std::string result;
    for (size_t i = 0; i < errors.size(); i++) {
      if (i > 0) result += "\n";
      result += errors[i].to_string();
    }
    return result;
  }

  std::string warnings_string() const {
    std::string result;
    for (size_t i = 0; i < warnings.size(); i++) {
      if (i > 0) result += "\n";
      result += "[WARNING] " + warnings[i];
    }
    return result;
  }
};

// ============================================================================
// Reader with enhanced error reporting
// ============================================================================

class Reader {
public:
  Reader(const uint8_t* data, size_t length, Endian endian = Endian::Little)
      : stream_(data, length, endian), context_("") {}

  // Set context for better error messages (e.g., "parsing header", "reading pixels")
  void set_context(const std::string& ctx) {
    context_ = ctx;
  }

  const std::string& context() const {
    return context_;
  }

  // Get accumulated errors
  const std::vector<ErrorInfo>& errors() const { return errors_; }

  bool has_error() const { return !errors_.empty(); }

  ErrorInfo last_error() const {
    return errors_.empty() ? ErrorInfo() : errors_.back();
  }

  std::string error_string() const {
    std::string result;
    for (size_t i = 0; i < errors_.size(); i++) {
      if (i > 0) result += "\n";
      result += errors_[i].to_string();
    }
    return result;
  }

  void clear_errors() {
    errors_.clear();
  }

  // Read n bytes
  bool read(size_t n, uint8_t* dst) {
    if (!dst) {
      add_error(ErrorCode::InvalidArgument, "Null destination pointer", n);
      return false;
    }
    if (!stream_.read(n, dst)) {
      add_error(ErrorCode::OutOfBounds,
                "Failed to read " + std::to_string(n) + " bytes",
                stream_.tell());
      return false;
    }
    return true;
  }

  // Read 1 byte
  bool read1(uint8_t* dst) {
    if (!stream_.read1(dst)) {
      add_error(ErrorCode::OutOfBounds, "Failed to read 1 byte", stream_.tell());
      return false;
    }
    return true;
  }

  // Read 2 bytes with endian swap
  bool read2(uint16_t* dst) {
    if (!stream_.read2(dst)) {
      add_error(ErrorCode::OutOfBounds, "Failed to read 2 bytes", stream_.tell());
      return false;
    }
    return true;
  }

  // Read 4 bytes with endian swap
  bool read4(uint32_t* dst) {
    if (!stream_.read4(dst)) {
      add_error(ErrorCode::OutOfBounds, "Failed to read 4 bytes", stream_.tell());
      return false;
    }
    return true;
  }

  // Read 8 bytes with endian swap
  bool read8(uint64_t* dst) {
    if (!stream_.read8(dst)) {
      add_error(ErrorCode::OutOfBounds, "Failed to read 8 bytes", stream_.tell());
      return false;
    }
    return true;
  }

  // Read null-terminated string with length limit
  bool read_string(std::string* str, size_t max_len = 256) {
    if (!str) {
      add_error(ErrorCode::InvalidArgument, "Null string pointer", stream_.tell());
      return false;
    }

    str->clear();
    size_t start_pos = stream_.tell();

    for (size_t i = 0; i < max_len; i++) {
      uint8_t c;
      if (!stream_.read1(&c)) {
        add_error(ErrorCode::OutOfBounds,
                  "Failed to read string (reached end at " + std::to_string(i) + " bytes)",
                  start_pos);
        return false;
      }
      if (c == '\0') {
        return true;
      }
      str->push_back(static_cast<char>(c));
    }

    add_error(ErrorCode::InvalidData,
              "String not null-terminated within " + std::to_string(max_len) + " bytes",
              start_pos);
    return false;
  }

  // Read fixed-length string (not null-terminated)
  bool read_fixed_string(std::string* str, size_t len) {
    if (!str) {
      add_error(ErrorCode::InvalidArgument, "Null string pointer", stream_.tell());
      return false;
    }

    str->resize(len);
    if (!stream_.read(len, reinterpret_cast<uint8_t*>(&(*str)[0]))) {
      add_error(ErrorCode::OutOfBounds,
                "Failed to read fixed string of length " + std::to_string(len),
                stream_.tell());
      return false;
    }
    return true;
  }

  // Seek operations
  bool seek(size_t pos) {
    if (!stream_.seek(pos)) {
      add_error(ErrorCode::OutOfBounds,
                "Failed to seek to position " + std::to_string(pos),
                stream_.tell());
      return false;
    }
    return true;
  }

  bool seek_relative(int64_t offset) {
    size_t current = stream_.tell();
    int64_t new_pos = static_cast<int64_t>(current) + offset;

    if (new_pos < 0) {
      add_error(ErrorCode::OutOfBounds, "Seek would move before start of stream", current);
      return false;
    }

    return seek(static_cast<size_t>(new_pos));
  }

  void rewind() {
    stream_.rewind();
  }

  // Position and size queries
  size_t tell() const { return stream_.tell(); }
  size_t remaining() const { return stream_.remaining(); }
  bool eof() const { return stream_.eof(); }
  size_t length() const { return stream_.length(); }

  // Get underlying stream
  const StreamReader& stream() const { return stream_; }

private:
  StreamReader stream_;
  std::vector<ErrorInfo> errors_;
  std::string context_;

  void add_error(ErrorCode code, const std::string& msg, size_t pos) {
    ErrorInfo err(code, msg, context_, pos);
    errors_.push_back(err);
  }
};

// ============================================================================
// Writer with enhanced error reporting
// ============================================================================

class Writer {
public:
  // Constructor for dynamic mode (grows automatically)
  Writer(Endian endian = Endian::Little)
      : stream_(endian), context_("") {}

  // Constructor for bounded mode (fixed-size buffer)
  Writer(uint8_t* data, size_t capacity, Endian endian = Endian::Little)
      : stream_(data, capacity, endian), context_("") {}

  // Set context for better error messages
  void set_context(const std::string& ctx) {
    context_ = ctx;
  }

  const std::string& context() const {
    return context_;
  }

  // Get accumulated errors
  const std::vector<ErrorInfo>& errors() const { return errors_; }

  bool has_error() const { return !errors_.empty(); }

  ErrorInfo last_error() const {
    return errors_.empty() ? ErrorInfo() : errors_.back();
  }

  std::string error_string() const {
    std::string result;
    for (size_t i = 0; i < errors_.size(); i++) {
      if (i > 0) result += "\n";
      result += errors_[i].to_string();
    }
    return result;
  }

  void clear_errors() {
    errors_.clear();
  }

  // Write n bytes
  bool write(size_t n, const uint8_t* src) {
    if (!src) {
      add_error(ErrorCode::InvalidArgument, "Null source pointer", n);
      return false;
    }
    if (!stream_.write(n, src)) {
      add_error(ErrorCode::OutOfBounds,
                "Failed to write " + std::to_string(n) + " bytes",
                stream_.tell());
      return false;
    }
    return true;
  }

  // Write 1 byte
  bool write1(uint8_t val) {
    if (!stream_.write1(val)) {
      add_error(ErrorCode::OutOfBounds, "Failed to write 1 byte", stream_.tell());
      return false;
    }
    return true;
  }

  // Write 2 bytes with endian swap
  bool write2(uint16_t val) {
    if (!stream_.write2(val)) {
      add_error(ErrorCode::OutOfBounds, "Failed to write 2 bytes", stream_.tell());
      return false;
    }
    return true;
  }

  // Write 4 bytes with endian swap
  bool write4(uint32_t val) {
    if (!stream_.write4(val)) {
      add_error(ErrorCode::OutOfBounds, "Failed to write 4 bytes", stream_.tell());
      return false;
    }
    return true;
  }

  // Write 8 bytes with endian swap
  bool write8(uint64_t val) {
    if (!stream_.write8(val)) {
      add_error(ErrorCode::OutOfBounds, "Failed to write 8 bytes", stream_.tell());
      return false;
    }
    return true;
  }

  // Write float
  bool write_float(float val) {
    if (!stream_.write_float(val)) {
      add_error(ErrorCode::OutOfBounds, "Failed to write float", stream_.tell());
      return false;
    }
    return true;
  }

  // Write double
  bool write_double(double val) {
    if (!stream_.write_double(val)) {
      add_error(ErrorCode::OutOfBounds, "Failed to write double", stream_.tell());
      return false;
    }
    return true;
  }

  // Write null-terminated string
  bool write_string(const char* str) {
    if (!str) {
      add_error(ErrorCode::InvalidArgument, "Null string pointer", stream_.tell());
      return false;
    }
    if (!stream_.write_string(str)) {
      add_error(ErrorCode::OutOfBounds,
                "Failed to write string",
                stream_.tell());
      return false;
    }
    return true;
  }

  // Write fixed-length string (no null terminator)
  bool write_fixed_string(const char* str, size_t len) {
    if (!str) {
      add_error(ErrorCode::InvalidArgument, "Null string pointer", stream_.tell());
      return false;
    }
    if (!stream_.write_fixed_string(str, len)) {
      add_error(ErrorCode::OutOfBounds,
                "Failed to write fixed string of length " + std::to_string(len),
                stream_.tell());
      return false;
    }
    return true;
  }

  // Seek operations
  bool seek(size_t pos) {
    if (!stream_.seek(pos)) {
      add_error(ErrorCode::OutOfBounds,
                "Failed to seek to position " + std::to_string(pos),
                stream_.tell());
      return false;
    }
    return true;
  }

  bool seek_relative(int64_t offset) {
    if (!stream_.seek_relative(offset)) {
      add_error(ErrorCode::OutOfBounds,
                "Failed to seek relative by " + std::to_string(offset),
                stream_.tell());
      return false;
    }
    return true;
  }

  void rewind() {
    stream_.rewind();
  }

  // Position and size queries
  size_t tell() const { return stream_.tell(); }
  size_t size() const { return stream_.size(); }
  size_t capacity() const { return stream_.capacity(); }
  size_t remaining() const { return stream_.remaining(); }
  bool at_capacity() const { return stream_.at_capacity(); }

  // Mode and data access
  WriterMode mode() const { return stream_.mode(); }

  // Get data (dynamic mode)
  const std::vector<uint8_t>& data() const { return stream_.data(); }
  std::vector<uint8_t>& data() { return stream_.data(); }

  // Get data pointer (both modes)
  const uint8_t* data_ptr() const { return stream_.data_ptr(); }

  // Reserve capacity (dynamic mode)
  void reserve(size_t cap) { stream_.reserve(cap); }

  // Clear (dynamic mode)
  void clear() {
    stream_.clear();
    errors_.clear();
  }

  // Get underlying stream
  const StreamWriter& stream() const { return stream_; }
  StreamWriter& stream() { return stream_; }

private:
  StreamWriter stream_;
  std::vector<ErrorInfo> errors_;
  std::string context_;

  void add_error(ErrorCode code, const std::string& msg, size_t pos) {
    ErrorInfo err(code, msg, context_, pos);
    errors_.push_back(err);
  }
};

// ============================================================================
// EXR structures (same as v1 but in v2 namespace)
// ============================================================================

struct Version {
  int version;        // Must be 2
  bool tiled;         // Tiled format
  bool long_name;     // Long attribute names (> 255 chars)
  bool non_image;     // Deep/non-image data
  bool multipart;     // Multipart file

  Version() : version(0), tiled(false), long_name(false),
              non_image(false), multipart(false) {}
};

struct Channel {
  std::string name;
  int pixel_type;     // 0=UINT, 1=HALF, 2=FLOAT
  bool p_linear;      // Perceptually linear
  int x_sampling;
  int y_sampling;

  Channel() : pixel_type(0), p_linear(false), x_sampling(1), y_sampling(1) {}
};

struct Box2i {
  int min_x;
  int min_y;
  int max_x;
  int max_y;

  Box2i() : min_x(0), min_y(0), max_x(0), max_y(0) {}

  int width() const { return max_x - min_x + 1; }
  int height() const { return max_y - min_y + 1; }
};

struct Header {
  std::vector<Channel> channels;
  Box2i data_window;
  Box2i display_window;
  int compression;
  int line_order;
  float pixel_aspect_ratio;
  float screen_window_center[2];
  float screen_window_width;

  // Tiled format
  bool tiled;
  int tile_size_x;
  int tile_size_y;
  int tile_level_mode;
  int tile_rounding_mode;

  // Multipart / Deep
  std::string name;           // Part name (required for multipart)
  std::string type;           // "scanlineimage", "tiledimage", "deepscanline", "deeptile"
  std::string view;           // View name for stereo (e.g., "left", "right")
  int chunk_count;            // Number of chunks (for multipart files)
  int deep_data_version;      // Version of deep data format (1 = current)
  bool is_deep;               // True if this is a deep image part

  size_t header_len;  // Length of header in bytes

  Header()
    : compression(0), line_order(0), pixel_aspect_ratio(1.0f),
      screen_window_width(1.0f), tiled(false), tile_size_x(0), tile_size_y(0),
      tile_level_mode(0), tile_rounding_mode(0), chunk_count(0),
      deep_data_version(0), is_deep(false), header_len(0) {
    screen_window_center[0] = 0.0f;
    screen_window_center[1] = 0.0f;
  }
};

// ============================================================================
// Parser functions
// ============================================================================

// Parse EXR version header
Result<Version> ParseVersion(Reader& reader);

// Parse EXR header (after version)
Result<Header> ParseHeader(Reader& reader, const Version& version);

// Load full EXR from memory (simplified API)
struct ImageData {
  int width;
  int height;
  int num_channels;
  std::vector<float> rgba;  // Always RGBA float for simplicity in v2
  Header header;

  ImageData() : width(0), height(0), num_channels(0) {}
};

// Deep image data - variable samples per pixel
struct DeepImageData {
  int width;
  int height;
  int num_channels;
  Header header;

  // Sample counts per pixel (width * height elements)
  std::vector<uint32_t> sample_counts;

  // Total number of samples across all pixels
  size_t total_samples;

  // Per-channel deep data (flattened, total_samples elements each)
  // channel_data[channel_index][sample_index] = sample value
  std::vector<std::vector<float>> channel_data;

  DeepImageData() : width(0), height(0), num_channels(0), total_samples(0) {}
};

// Multipart image data - multiple parts/layers
struct MultipartImageData {
  std::vector<ImageData> parts;          // Regular image parts
  std::vector<DeepImageData> deep_parts; // Deep image parts
  std::vector<Header> headers;           // All part headers

  // Get part by name
  const ImageData* get_part(const std::string& name) const {
    for (size_t i = 0; i < parts.size(); i++) {
      if (parts[i].header.name == name) return &parts[i];
    }
    return nullptr;
  }

  // Get part by view name (for stereo)
  const ImageData* get_view(const std::string& view) const {
    for (size_t i = 0; i < parts.size(); i++) {
      if (parts[i].header.view == view) return &parts[i];
    }
    return nullptr;
  }
};

Result<ImageData> LoadFromMemory(const uint8_t* data, size_t size);

// Load multipart/deep EXR from memory
Result<MultipartImageData> LoadMultipartFromMemory(const uint8_t* data, size_t size);

// ============================================================================
// Writer functions
// ============================================================================

// Write EXR version header
Result<void> WriteVersion(Writer& writer, const Version& version);

// Write EXR header
Result<void> WriteHeader(Writer& writer, const Header& header);

// Save image data to memory (simplified API)
// compression_level: 1-9 for ZIP compression (6 = default)
Result<std::vector<uint8_t>> SaveToMemory(const ImageData& image, int compression_level);
Result<std::vector<uint8_t>> SaveToMemory(const ImageData& image);

// Save image data to file
Result<void> SaveToFile(const char* filename, const ImageData& image, int compression_level = 6);

// Save tiled EXR to memory
// The image.header must have tiled=true, tile_size_x>0, and tile_size_y>0
// compression_level: 1-9 for ZIP compression (6 = default)
Result<std::vector<uint8_t>> SaveTiledToMemory(const ImageData& image, int compression_level);
Result<std::vector<uint8_t>> SaveTiledToMemory(const ImageData& image);

// Save tiled EXR to file
Result<void> SaveTiledToFile(const char* filename, const ImageData& image, int compression_level = 6);

// Save multipart image data to memory
// compression_level: 1-9 for ZIP compression (6 = default)
Result<std::vector<uint8_t>> SaveMultipartToMemory(const MultipartImageData& multipart, int compression_level);
Result<std::vector<uint8_t>> SaveMultipartToMemory(const MultipartImageData& multipart);

// Save multipart image data to file
Result<void> SaveMultipartToFile(const char* filename, const MultipartImageData& multipart, int compression_level = 6);

// ============================================================================
// Pixel Processing Utilities (with SIMD optimization when enabled)
// ============================================================================

// Convert half-precision (FP16) array to single-precision (FP32)
// Uses SIMD when TINYEXR_ENABLE_SIMD is defined
inline void ConvertHalfToFloat(const uint16_t* half_data, float* float_data, size_t count) {
#if defined(TINYEXR_ENABLE_SIMD) && TINYEXR_ENABLE_SIMD
  tinyexr::simd::half_to_float_batch(half_data, float_data, count);
#else
  // Scalar fallback (same algorithm as tinyexr.h)
  for (size_t i = 0; i < count; i++) {
    uint16_t h = half_data[i];
    union { uint32_t u; float f; } o;
    static const union { uint32_t u; float f; } magic = {113U << 23};
    static const uint32_t shifted_exp = 0x7c00U << 13;

    o.u = (h & 0x7fffU) << 13U;
    uint32_t exp_ = shifted_exp & o.u;
    o.u += (127 - 15) << 23;

    if (exp_ == shifted_exp) {
      o.u += (128 - 16) << 23;
    } else if (exp_ == 0) {
      o.u += 1 << 23;
      o.f -= magic.f;
    }

    o.u |= (h & 0x8000U) << 16U;
    float_data[i] = o.f;
  }
#endif
}

// Convert single-precision (FP32) array to half-precision (FP16)
// Uses SIMD when TINYEXR_ENABLE_SIMD is defined
inline void ConvertFloatToHalf(const float* float_data, uint16_t* half_data, size_t count) {
#if defined(TINYEXR_ENABLE_SIMD) && TINYEXR_ENABLE_SIMD
  tinyexr::simd::float_to_half_batch(float_data, half_data, count);
#else
  // Scalar fallback
  for (size_t i = 0; i < count; i++) {
    float f = float_data[i];
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
    half_data[i] = o.u;
  }
#endif
}

// Interleave separate channel arrays into interleaved RGBA format
// Input: 4 separate float arrays (R, G, B, A), each of length 'pixel_count'
// Output: Interleaved RGBA array of length 'pixel_count * 4'
inline void InterleaveRGBA(const float* r, const float* g, const float* b, const float* a,
                           float* rgba, size_t pixel_count) {
#if defined(TINYEXR_ENABLE_SIMD) && TINYEXR_ENABLE_SIMD
  tinyexr::simd::interleave_rgba_float(r, g, b, a, rgba, pixel_count);
#else
  for (size_t i = 0; i < pixel_count; i++) {
    rgba[i * 4 + 0] = r[i];
    rgba[i * 4 + 1] = g[i];
    rgba[i * 4 + 2] = b[i];
    rgba[i * 4 + 3] = a[i];
  }
#endif
}

// Deinterleave RGBA format into separate channel arrays
inline void DeinterleaveRGBA(const float* rgba, float* r, float* g, float* b, float* a,
                             size_t pixel_count) {
#if defined(TINYEXR_ENABLE_SIMD) && TINYEXR_ENABLE_SIMD
  tinyexr::simd::deinterleave_rgba_float(rgba, r, g, b, a, pixel_count);
#else
  for (size_t i = 0; i < pixel_count; i++) {
    r[i] = rgba[i * 4 + 0];
    g[i] = rgba[i * 4 + 1];
    b[i] = rgba[i * 4 + 2];
    a[i] = rgba[i * 4 + 3];
  }
#endif
}

// Get SIMD capability information string
inline const char* GetSIMDInfo() {
#if defined(TINYEXR_ENABLE_SIMD) && TINYEXR_ENABLE_SIMD
  return tinyexr::simd::get_simd_info();
#else
  return "Scalar (SIMD disabled)";
#endif
}

// Check if SIMD is enabled at compile time
inline bool IsSIMDEnabled() {
#if defined(TINYEXR_ENABLE_SIMD) && TINYEXR_ENABLE_SIMD
  return true;
#else
  return false;
#endif
}

// ============================================================================
// Compression/Decompression Utilities (with optimizations when enabled)
// ============================================================================

// Get Huffman/bit manipulation backend info
inline const char* GetHuffmanInfo() {
#if defined(TINYEXR_ENABLE_SIMD) && TINYEXR_ENABLE_SIMD
  return tinyexr::huffman::get_huffman_info();
#else
  return "Scalar (SIMD disabled)";
#endif
}

// Decompress deflate data (raw deflate, no zlib header)
// Returns true on success, false on error
// dst_len should be set to the output buffer size on input,
// and will be set to the actual decompressed size on output
inline bool DecompressDeflate(const uint8_t* src, size_t src_len,
                              uint8_t* dst, size_t* dst_len) {
#if defined(TINYEXR_ENABLE_SIMD) && TINYEXR_ENABLE_SIMD
  return tinyexr::huffman::inflate(src, src_len, dst, dst_len);
#else
  // Fallback: not implemented without SIMD
  // In production, this would call miniz or zlib
  (void)src; (void)src_len; (void)dst; (void)dst_len;
  return false;
#endif
}

// Decompress zlib data (with 2-byte header)
inline bool DecompressZlib(const uint8_t* src, size_t src_len,
                          uint8_t* dst, size_t* dst_len) {
#if defined(TINYEXR_ENABLE_SIMD) && TINYEXR_ENABLE_SIMD
  return tinyexr::huffman::inflate_zlib(src, src_len, dst, dst_len);
#else
  // Fallback: not implemented without SIMD
  (void)src; (void)src_len; (void)dst; (void)dst_len;
  return false;
#endif
}

// ============================================================================
// Memory Pool for Temporary Buffers
// ============================================================================

// Thread-local scratch buffer pool to avoid repeated allocations
// during decompression operations
class ScratchPool {
public:
  static constexpr size_t SMALL_SIZE = 64 * 1024;     // 64KB
  static constexpr size_t MEDIUM_SIZE = 256 * 1024;   // 256KB
  static constexpr size_t LARGE_SIZE = 1024 * 1024;   // 1MB

  ScratchPool() {
    small_.resize(SMALL_SIZE);
    medium_.resize(MEDIUM_SIZE);
    large_.resize(LARGE_SIZE);
  }

  // Get pre-allocated small buffer (64KB)
  uint8_t* get_small() { return small_.data(); }
  size_t small_size() const { return SMALL_SIZE; }

  // Get pre-allocated medium buffer (256KB)
  uint8_t* get_medium() { return medium_.data(); }
  size_t medium_size() const { return MEDIUM_SIZE; }

  // Get pre-allocated large buffer (1MB)
  uint8_t* get_large() { return large_.data(); }
  size_t large_size() const { return LARGE_SIZE; }

  // Get dynamic buffer, resizing if needed
  uint8_t* get_dynamic(size_t needed) {
    if (dynamic_.size() < needed) {
      dynamic_.resize(needed);
    }
    return dynamic_.data();
  }
  size_t dynamic_size() const { return dynamic_.size(); }

  // Get best-fit buffer for given size
  // Returns pointer to appropriate pre-allocated buffer if size fits,
  // otherwise allocates dynamic buffer
  uint8_t* get_buffer(size_t needed) {
    if (needed <= SMALL_SIZE) {
      return get_small();
    } else if (needed <= MEDIUM_SIZE) {
      return get_medium();
    } else if (needed <= LARGE_SIZE) {
      return get_large();
    } else {
      return get_dynamic(needed);
    }
  }

  // Clear dynamic buffer to release memory
  void clear_dynamic() {
    dynamic_.clear();
    dynamic_.shrink_to_fit();
  }

private:
  std::vector<uint8_t> small_;
  std::vector<uint8_t> medium_;
  std::vector<uint8_t> large_;
  std::vector<uint8_t> dynamic_;
};

// Thread-local scratch pool instance
// Each thread gets its own pool to avoid contention
inline ScratchPool& get_scratch_pool() {
  static thread_local ScratchPool pool;
  return pool;
}

}  // namespace v2
}  // namespace tinyexr

#endif  // TINYEXR_V2_HH_

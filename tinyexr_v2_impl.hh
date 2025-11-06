// SPDX-License-Identifier: BSD-3-Clause
// Copyright (c) 2025, Syoyo Fujita and many contributors.
// All rights reserved.
//
// TinyEXR V2 Implementation

#ifndef TINYEXR_V2_IMPL_HH_
#define TINYEXR_V2_IMPL_HH_

#include "tinyexr_v2.hh"
#include <cstring>

namespace tinyexr {
namespace v2 {

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
      // For now, skip detailed parsing - just consume the bytes
      // TODO: Implement full channel list parsing
      if (!reader.seek_relative(data_size)) {
        return Result<Header>::error(reader.last_error());
      }
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

  // For now, return partial result with header info
  // TODO: Implement full image data loading
  ImageData img_data;
  img_data.header = header_result.value;
  img_data.width = header_result.value.data_window.width();
  img_data.height = header_result.value.data_window.height();

  Result<ImageData> result = Result<ImageData>::ok(img_data);

  // Carry forward warnings
  result.warnings = version_result.warnings;
  for (size_t i = 0; i < header_result.warnings.size(); i++) {
    result.warnings.push_back(header_result.warnings[i]);
  }

  result.add_warning("Image pixel data loading not yet implemented in v2 API");

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

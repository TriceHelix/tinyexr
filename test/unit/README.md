# Build&Test

## Prepare

Clone `https://github.com/openexr/openexr-images` to `../../../` directory.
(Or edit path to openexr-images in `tester.cc`)

## V1 API (Stable Release)

The original TinyEXR API - stable and production-ready.

### Use makefile

    $ make check

### Use ninja + kuroga

Assume

* ninja 1.4+
* python 2.6+

Are installed.

#### Linux/MacOSX

    $ python kuroga.py config-posix.py
    $ ninja

#### Windows

    > python kuroga.py config-msvc.py
    > vcbuild.bat

## V2 API (Experimental)

**Status: EXPERIMENTAL - API may change**

The V2 API is a refactored version with enhanced error reporting and safer memory access. It is currently experimental and not recommended for production use.

### Features

- **StreamReader/StreamWriter**: Low-level safe memory access with endian support
- **Error Accumulation**: Comprehensive error stack with context and positions
- **Human-Readable Errors**: Detailed error messages designed for developers
- **Result<T> Pattern**: Type-safe error handling without exceptions
- **Dual Buffer Modes**: Dynamic (auto-growing) and bounded (fixed-size) buffers
- **Exception-Free**: Compatible with `-fno-exceptions -fno-rtti`

### Build V2 Tester

    $ g++ -std=c++11 -Wall -Wextra -I../.. tester-v2.cc -o tester-v2

### Run V2 Tests

    $ ./tester-v2                    # Run all tests
    $ ./tester-v2 --list-tests       # List available tests
    $ ./tester-v2 -s                 # Run with detailed output
    $ ./tester-v2 "[StreamReader]"   # Run specific category

### V2 Test Coverage

- **20 test cases** with 3+ million assertions
- StreamReader/StreamWriter unit tests
- v2::Reader/v2::Writer with error accumulation
- Version parsing and writing
- Round-trip tests
- Error handling and regression tests
- Performance/stress tests

### V2 API Files

- `streamreader.hh` - Low-level stream reader
- `streamwriter.hh` - Low-level stream writer
- `exr_reader.hh` - V1-compatible reader wrapper
- `tinyexr_v2.hh` - V2 API declarations
- `tinyexr_v2_impl.hh` - V2 API implementations
- `test/unit/tester-v2.cc` - V2 unit tests

### Migration Status

✅ StreamReader/Writer fully implemented
✅ Error reporting and Result<T> complete
✅ Version parsing and writing complete
✅ Basic header parsing implemented
⚠️ Pixel data loading/saving TODO
⚠️ Compression formats TODO
⚠️ Full channel list parsing TODO

See `TINYEXR_V2_README.md` for complete V2 API documentation.

## Recommendation

**Use V1 API for production.** The V2 API is experimental and provided for:
- Testing new error handling approaches
- Evaluating safer memory access patterns
- Gathering feedback before potential future stable release


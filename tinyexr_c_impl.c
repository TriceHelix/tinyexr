/* Feature test macros for POSIX functions (must be before any includes) */
#if !defined(_WIN32)
#define _POSIX_C_SOURCE 199309L
#endif

/*
 * TinyEXR V3 - Pure C API Implementation
 *
 * Copyright (c) 2024 TinyEXR authors
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "tinyexr_c.h"

#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdbool.h>

/* C11 atomics for C, std::atomic for C++ */
#ifdef __cplusplus
#include <atomic>
#define ATOMIC_INT std::atomic<int>
#define ATOMIC_INIT(var, val) var = (val)
#define ATOMIC_LOAD(var) var.load()
#define ATOMIC_STORE(var, val) var.store(val)
#define ATOMIC_FETCH_ADD(var, val) var.fetch_add(val)
#define ATOMIC_FETCH_SUB(var, val) var.fetch_sub(val)
#else
#include <stdatomic.h>
#define ATOMIC_INT atomic_int
#define ATOMIC_INIT(var, val) atomic_init(&(var), (val))
#define ATOMIC_LOAD(var) atomic_load(&(var))
#define ATOMIC_STORE(var, val) atomic_store(&(var), (val))
#define ATOMIC_FETCH_ADD(var, val) atomic_fetch_add(&(var), (val))
#define ATOMIC_FETCH_SUB(var, val) atomic_fetch_sub(&(var), (val))
#endif

/* Platform-specific includes */
#if defined(_WIN32)
#include <windows.h>
#else
#include <pthread.h>
#include <time.h>
#include <errno.h>
#endif

/* CPUID for x86 */
#if (defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)) && \
    (defined(__GNUC__) || defined(__clang__))
#include <cpuid.h>
#endif

/* Thread-local storage */
#if defined(_MSC_VER)
#define EXR_THREAD_LOCAL __declspec(thread)
#elif defined(__GNUC__) || defined(__clang__)
#define EXR_THREAD_LOCAL __thread
#else
#define EXR_THREAD_LOCAL _Thread_local
#endif

/* Alignment macros */
#define EXR_ALIGN(x, a) (((x) + (a) - 1) & ~((a) - 1))
#define EXR_DEFAULT_ALIGNMENT 16

/* Maximum errors to store per context */
#define EXR_MAX_ERRORS 16

/* Maximum error message length */
#define EXR_MAX_ERROR_MESSAGE 256

/* ============================================================================
 * Version Information
 * ============================================================================ */

static const char* g_version_string = "TinyEXR 3.0.0";

void exr_get_version(int* major, int* minor, int* patch) {
    if (major) *major = TINYEXR_C_API_VERSION_MAJOR;
    if (minor) *minor = TINYEXR_C_API_VERSION_MINOR;
    if (patch) *patch = TINYEXR_C_API_VERSION_PATCH;
}

const char* exr_get_version_string(void) {
    return g_version_string;
}

/* ============================================================================
 * Result to String
 * ============================================================================ */

static const char* g_result_strings[] = {
    "Success",                          /* EXR_SUCCESS = 0 */
    "Incomplete",                       /* EXR_INCOMPLETE = 1 */
    "Would block",                      /* EXR_WOULD_BLOCK = 2 */
    "Suspended",                        /* EXR_SUSPENDED = 3 */
};

static const char* g_error_strings[] = {
    "Invalid handle",                   /* EXR_ERROR_INVALID_HANDLE = -1 */
    "Invalid argument",                 /* EXR_ERROR_INVALID_ARGUMENT = -2 */
    "Out of memory",                    /* EXR_ERROR_OUT_OF_MEMORY = -3 */
    "Invalid magic number",             /* EXR_ERROR_INVALID_MAGIC = -4 */
    "Invalid version",                  /* EXR_ERROR_INVALID_VERSION = -5 */
    "Invalid data",                     /* EXR_ERROR_INVALID_DATA = -6 */
    "Unsupported format",               /* EXR_ERROR_UNSUPPORTED_FORMAT = -7 */
    "Unsupported compression",          /* EXR_ERROR_UNSUPPORTED_COMPRESSION = -8 */
    "Decompression failed",             /* EXR_ERROR_DECOMPRESSION_FAILED = -9 */
    "Compression failed",               /* EXR_ERROR_COMPRESSION_FAILED = -10 */
    "I/O error",                        /* EXR_ERROR_IO = -11 */
    "Buffer too small",                 /* EXR_ERROR_BUFFER_TOO_SMALL = -12 */
    "Timeout",                          /* EXR_ERROR_TIMEOUT = -13 */
    "Cancelled",                        /* EXR_ERROR_CANCELLED = -14 */
    "Not ready",                        /* EXR_ERROR_NOT_READY = -15 */
    "Missing attribute",                /* EXR_ERROR_MISSING_ATTRIBUTE = -16 */
    "Fetch failed",                     /* EXR_ERROR_FETCH_FAILED = -17 */
    "Out of bounds",                    /* EXR_ERROR_OUT_OF_BOUNDS = -18 */
    "Already initialized",              /* EXR_ERROR_ALREADY_INITIALIZED = -19 */
    "Not initialized",                  /* EXR_ERROR_NOT_INITIALIZED = -20 */
    "Invalid state",                    /* EXR_ERROR_INVALID_STATE = -21 */
};

const char* exr_result_to_string(ExrResult result) {
    if (result >= 0 && result < (int)(sizeof(g_result_strings) / sizeof(g_result_strings[0]))) {
        return g_result_strings[result];
    }
    if (result < 0) {
        int index = -result - 1;
        if (index < (int)(sizeof(g_error_strings) / sizeof(g_error_strings[0]))) {
            return g_error_strings[index];
        }
    }
    return "Unknown error";
}

/* ============================================================================
 * Default Allocator
 * ============================================================================ */

/* Forward declaration */
static void default_free(void* userdata, void* ptr, size_t size);

static void* default_alloc(void* userdata, size_t size, size_t alignment) {
    (void)userdata;
    (void)alignment;
#if defined(_MSC_VER)
    return _aligned_malloc(size, alignment < 16 ? 16 : alignment);
#elif defined(__APPLE__) || defined(__ANDROID__)
    void* ptr = NULL;
    if (posix_memalign(&ptr, alignment < 16 ? 16 : alignment, size) != 0) {
        return NULL;
    }
    return ptr;
#else
    return aligned_alloc(alignment < sizeof(void*) ? sizeof(void*) : alignment,
                         EXR_ALIGN(size, alignment));
#endif
}

static void* default_realloc(void* userdata, void* ptr, size_t old_size,
                              size_t new_size, size_t alignment) {
    (void)userdata;
    (void)old_size;
    (void)alignment;
    /* Note: aligned realloc is tricky, so we allocate new and copy */
    void* new_ptr = default_alloc(userdata, new_size, alignment);
    if (new_ptr && ptr) {
        memcpy(new_ptr, ptr, old_size < new_size ? old_size : new_size);
        default_free(userdata, ptr, old_size);
    }
    return new_ptr;
}

static void default_free(void* userdata, void* ptr, size_t size) {
    (void)userdata;
    (void)size;
#if defined(_MSC_VER)
    _aligned_free(ptr);
#else
    free(ptr);
#endif
}

static const ExrAllocator g_default_allocator = {
    NULL,
    default_alloc,
    default_realloc,
    default_free
};

const ExrAllocator* exr_get_default_allocator(void) {
    return &g_default_allocator;
}

/* ============================================================================
 * Error Entry
 * ============================================================================ */

typedef struct ExrErrorEntry {
    ExrResult code;
    char message[EXR_MAX_ERROR_MESSAGE];
    char context[64];
    uint64_t byte_position;
    int32_t line_number;
    const char* source_file;  /* Static string, not owned */
} ExrErrorEntry;

/* ============================================================================
 * Context Internal Structure
 * ============================================================================ */

struct ExrContext_T {
    /* Reference count */
    ATOMIC_INT ref_count;

    /* Allocator */
    ExrAllocator allocator;

    /* Error handling */
    ExrErrorCallback error_callback;
    void* error_userdata;
    ExrErrorEntry errors[EXR_MAX_ERRORS];
    uint32_t error_count;
    uint32_t error_index;  /* Ring buffer index */

    /* Flags */
    uint32_t flags;
    uint32_t max_threads;

    /* Magic for validation */
    uint32_t magic;
};

#define EXR_CONTEXT_MAGIC 0x45585243  /* 'EXRC' */

/* ============================================================================
 * Context Validation
 * ============================================================================ */

static int exr_context_is_valid(ExrContext ctx) {
    return ctx != NULL && ctx->magic == EXR_CONTEXT_MAGIC;
}

/* ============================================================================
 * Context Error Management
 * ============================================================================ */

static void exr_context_add_error(ExrContext ctx, ExrResult code,
                                   const char* message, const char* context_str,
                                   uint64_t byte_pos) {
    if (!exr_context_is_valid(ctx)) return;

    uint32_t index = ctx->error_index % EXR_MAX_ERRORS;
    ExrErrorEntry* entry = &ctx->errors[index];

    entry->code = code;
    if (message) {
        strncpy(entry->message, message, EXR_MAX_ERROR_MESSAGE - 1);
        entry->message[EXR_MAX_ERROR_MESSAGE - 1] = '\0';
    } else {
        entry->message[0] = '\0';
    }
    if (context_str) {
        strncpy(entry->context, context_str, sizeof(entry->context) - 1);
        entry->context[sizeof(entry->context) - 1] = '\0';
    } else {
        entry->context[0] = '\0';
    }
    entry->byte_position = byte_pos;
    entry->line_number = 0;
    entry->source_file = NULL;

    ctx->error_index++;
    if (ctx->error_count < EXR_MAX_ERRORS) {
        ctx->error_count++;
    }

    /* Call error callback if set */
    if (ctx->error_callback) {
        ExrErrorInfo info = {
            .code = code,
            .message = entry->message,
            .context = entry->context,
            .byte_position = byte_pos,
            .line_number = 0,
            .source_file = NULL
        };
        ctx->error_callback(ctx->error_userdata, &info);
    }
}

static void exr_context_add_error_fmt(ExrContext ctx, ExrResult code,
                                       const char* context_str, uint64_t byte_pos,
                                       const char* fmt, ...) {
    char message[EXR_MAX_ERROR_MESSAGE];
    va_list args;
    va_start(args, fmt);
    vsnprintf(message, sizeof(message), fmt, args);
    va_end(args);
    exr_context_add_error(ctx, code, message, context_str, byte_pos);
}

/* ============================================================================
 * Context Creation/Destruction
 * ============================================================================ */

ExrResult exr_context_create(const ExrContextCreateInfo* create_info,
                              ExrContext* out_ctx) {
    if (!out_ctx) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }
    *out_ctx = NULL;

    if (!create_info) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    /* Check API version compatibility */
    uint32_t major = (create_info->api_version >> 22) & 0x3FF;
    if (major != TINYEXR_C_API_VERSION_MAJOR) {
        return EXR_ERROR_INVALID_VERSION;
    }

    /* Use provided allocator or default */
    const ExrAllocator* alloc = create_info->allocator;
    if (!alloc) {
        alloc = &g_default_allocator;
    }

    /* Allocate context */
    ExrContext ctx = (ExrContext)alloc->alloc(
        alloc->userdata,
        sizeof(struct ExrContext_T),
        EXR_DEFAULT_ALIGNMENT
    );
    if (!ctx) {
        return EXR_ERROR_OUT_OF_MEMORY;
    }

    /* Initialize */
    memset(ctx, 0, sizeof(struct ExrContext_T));
    ctx->magic = EXR_CONTEXT_MAGIC;
    ATOMIC_INIT(ctx->ref_count, 1);
    ctx->allocator = *alloc;
    ctx->error_callback = create_info->error_callback;
    ctx->error_userdata = create_info->error_userdata;
    ctx->flags = create_info->flags;
    ctx->max_threads = create_info->max_threads;

    *out_ctx = ctx;
    return EXR_SUCCESS;
}

void exr_context_destroy(ExrContext ctx) {
    if (!exr_context_is_valid(ctx)) return;

    /* Check reference count */
    int refs = ATOMIC_LOAD(ctx->ref_count);
    if (refs > 1) {
        ATOMIC_FETCH_SUB(ctx->ref_count, 1);
        return;
    }

    /* Invalidate magic before freeing */
    ctx->magic = 0;

    /* Free context */
    ctx->allocator.free(ctx->allocator.userdata, ctx, sizeof(struct ExrContext_T));
}

void exr_context_add_ref(ExrContext ctx) {
    if (exr_context_is_valid(ctx)) {
        ATOMIC_FETCH_ADD(ctx->ref_count, 1);
    }
}

void exr_context_release(ExrContext ctx) {
    if (exr_context_is_valid(ctx)) {
        if (ATOMIC_FETCH_SUB(ctx->ref_count, 1) == 1) {
            exr_context_destroy(ctx);
        }
    }
}

/* ============================================================================
 * Error Retrieval
 * ============================================================================ */

ExrResult exr_get_last_error(ExrContext ctx, ExrErrorInfo* out_info) {
    if (!exr_context_is_valid(ctx)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!out_info) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }
    if (ctx->error_count == 0) {
        memset(out_info, 0, sizeof(ExrErrorInfo));
        return EXR_SUCCESS;
    }

    uint32_t index = (ctx->error_index - 1) % EXR_MAX_ERRORS;
    ExrErrorEntry* entry = &ctx->errors[index];

    out_info->code = entry->code;
    out_info->message = entry->message;
    out_info->context = entry->context;
    out_info->byte_position = entry->byte_position;
    out_info->line_number = entry->line_number;
    out_info->source_file = entry->source_file;

    return EXR_SUCCESS;
}

uint32_t exr_get_error_count(ExrContext ctx) {
    if (!exr_context_is_valid(ctx)) {
        return 0;
    }
    return ctx->error_count;
}

ExrResult exr_get_error_at(ExrContext ctx, uint32_t index, ExrErrorInfo* out_info) {
    if (!exr_context_is_valid(ctx)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!out_info) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }
    if (index >= ctx->error_count) {
        return EXR_ERROR_OUT_OF_BOUNDS;
    }

    /* Calculate actual index in ring buffer */
    uint32_t actual_index;
    if (ctx->error_count < EXR_MAX_ERRORS) {
        actual_index = index;
    } else {
        actual_index = (ctx->error_index + index) % EXR_MAX_ERRORS;
    }

    ExrErrorEntry* entry = &ctx->errors[actual_index];
    out_info->code = entry->code;
    out_info->message = entry->message;
    out_info->context = entry->context;
    out_info->byte_position = entry->byte_position;
    out_info->line_number = entry->line_number;
    out_info->source_file = entry->source_file;

    return EXR_SUCCESS;
}

void exr_clear_errors(ExrContext ctx) {
    if (!exr_context_is_valid(ctx)) return;
    ctx->error_count = 0;
    ctx->error_index = 0;
}

/* ============================================================================
 * Memory Pool Internal Structure
 * ============================================================================ */

struct ExrMemoryPool_T {
    ExrContext ctx;
    uint8_t* data;
    size_t size;
    size_t used;
    size_t max_size;
    uint32_t flags;
    uint32_t magic;
};

#define EXR_MEMORY_POOL_MAGIC 0x4D504F4C  /* 'MPOL' */

static int exr_memory_pool_is_valid(ExrMemoryPool pool) {
    return pool != NULL && pool->magic == EXR_MEMORY_POOL_MAGIC;
}

ExrResult exr_memory_pool_create(ExrContext ctx,
                                  const ExrMemoryPoolCreateInfo* create_info,
                                  ExrMemoryPool* out_pool) {
    if (!exr_context_is_valid(ctx)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!create_info || !out_pool) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    *out_pool = NULL;

    /* Allocate pool structure */
    ExrMemoryPool pool = (ExrMemoryPool)ctx->allocator.alloc(
        ctx->allocator.userdata,
        sizeof(struct ExrMemoryPool_T),
        EXR_DEFAULT_ALIGNMENT
    );
    if (!pool) {
        exr_context_add_error(ctx, EXR_ERROR_OUT_OF_MEMORY,
                              "Failed to allocate memory pool", NULL, 0);
        return EXR_ERROR_OUT_OF_MEMORY;
    }

    memset(pool, 0, sizeof(struct ExrMemoryPool_T));
    pool->ctx = ctx;
    pool->max_size = create_info->max_size;
    pool->flags = create_info->flags;
    pool->magic = EXR_MEMORY_POOL_MAGIC;

    /* Allocate initial buffer if requested */
    if (create_info->initial_size > 0) {
        pool->data = (uint8_t*)ctx->allocator.alloc(
            ctx->allocator.userdata,
            create_info->initial_size,
            EXR_DEFAULT_ALIGNMENT
        );
        if (!pool->data) {
            ctx->allocator.free(ctx->allocator.userdata, pool,
                               sizeof(struct ExrMemoryPool_T));
            exr_context_add_error(ctx, EXR_ERROR_OUT_OF_MEMORY,
                                  "Failed to allocate pool buffer", NULL, 0);
            return EXR_ERROR_OUT_OF_MEMORY;
        }
        pool->size = create_info->initial_size;
    }

    exr_context_add_ref(ctx);
    *out_pool = pool;
    return EXR_SUCCESS;
}

void exr_memory_pool_destroy(ExrMemoryPool pool) {
    if (!exr_memory_pool_is_valid(pool)) return;

    ExrContext ctx = pool->ctx;
    pool->magic = 0;

    if (pool->data) {
        ctx->allocator.free(ctx->allocator.userdata, pool->data, pool->size);
    }
    ctx->allocator.free(ctx->allocator.userdata, pool,
                        sizeof(struct ExrMemoryPool_T));
    exr_context_release(ctx);
}

void exr_memory_pool_reset(ExrMemoryPool pool) {
    if (!exr_memory_pool_is_valid(pool)) return;
    pool->used = 0;
}

size_t exr_memory_pool_get_used(ExrMemoryPool pool) {
    if (!exr_memory_pool_is_valid(pool)) return 0;
    return pool->used;
}

/* ============================================================================
 * Data Source from Memory
 * ============================================================================ */

typedef struct ExrMemorySourceData {
    const uint8_t* data;
    size_t size;
} ExrMemorySourceData;

static ExrResult memory_source_fetch(void* userdata, uint64_t offset, uint64_t size,
                                      void* dst, ExrFetchComplete on_complete,
                                      void* complete_userdata) {
    ExrMemorySourceData* src = (ExrMemorySourceData*)userdata;
    (void)on_complete;
    (void)complete_userdata;

    if (offset >= src->size) {
        return EXR_ERROR_OUT_OF_BOUNDS;
    }

    size_t available = src->size - (size_t)offset;
    size_t to_read = (size_t)size;
    if (to_read > available) {
        to_read = available;
    }

    memcpy(dst, src->data + offset, to_read);

    /* Synchronous completion - no callback needed */
    return EXR_SUCCESS;
}

/* Note: This allocates a small structure on the heap. The caller must
 * ensure it lives as long as the data source is in use. For simplicity,
 * we use a static buffer for single-threaded use. For proper thread-safe
 * usage, the caller should manage the ExrMemorySourceData lifetime. */
static EXR_THREAD_LOCAL ExrMemorySourceData g_memory_source_data;

ExrResult exr_data_source_from_memory(const void* data, size_t size,
                                       ExrDataSource* out_source) {
    if (!data || size == 0 || !out_source) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    g_memory_source_data.data = (const uint8_t*)data;
    g_memory_source_data.size = size;

    out_source->userdata = &g_memory_source_data;
    out_source->fetch = memory_source_fetch;
    out_source->cancel = NULL;
    out_source->total_size = size;
    out_source->flags = EXR_DATA_SOURCE_SEEKABLE | EXR_DATA_SOURCE_SIZE_KNOWN;

    return EXR_SUCCESS;
}

/* ============================================================================
 * Decoder Internal Structure
 * ============================================================================ */

typedef enum ExrDecoderState {
    EXR_DECODER_STATE_CREATED = 0,
    EXR_DECODER_STATE_PARSING_HEADER,
    EXR_DECODER_STATE_HEADER_PARSED,
    EXR_DECODER_STATE_LOADING,
    EXR_DECODER_STATE_ERROR,
} ExrDecoderState;

/* Parsing phases for async state machine (forward declaration) */
typedef enum ExrParsePhase {
    EXR_PHASE_IDLE = 0,
    EXR_PHASE_VERSION,             /* Reading magic + version bytes */
    EXR_PHASE_ATTRIBUTE_NAME,      /* Reading attribute name */
    EXR_PHASE_ATTRIBUTE_DATA,      /* Reading attribute value */
    EXR_PHASE_END_OF_HEADER,       /* Checking for header terminator */
    EXR_PHASE_OFFSET_TABLE,        /* Reading chunk offset table */
    EXR_PHASE_CHUNK_HEADER,        /* Reading chunk header (y + size) */
    EXR_PHASE_CHUNK_DATA,          /* Reading compressed chunk data */
} ExrParsePhase;

/* Forward declaration for suspend state */
struct ExrSuspendState_T;
typedef struct ExrSuspendState_T* ExrSuspendState_Internal;

struct ExrDecoder_T {
    ExrContext ctx;
    ExrDataSource source;
    ExrMemoryPool scratch_pool;
    uint32_t flags;
    ExrDecoderState state;

    /* Parsing state */
    uint64_t current_offset;
    uint8_t* read_buffer;
    size_t read_buffer_size;

    /* Async state for suspend/resume */
    ExrSuspendState suspend_state;
    ExrParsePhase current_phase;
    uint32_t current_part_index;
    uint32_t current_chunk_index;

    /* Parsed image */
    ExrImage image;

    /* Progress */
    ExrProgressCallback progress_callback;
    void* progress_userdata;
    int32_t progress_interval_ms;

    uint32_t magic;
};

#define EXR_DECODER_MAGIC 0x44454352  /* 'DECR' */

/* ============================================================================
 * Async/WASM Suspend State Structure
 * ============================================================================ */

/* Suspend state for async operations */
struct ExrSuspendState_T {
    uint32_t magic;

    /* Current parsing phase */
    ExrParsePhase phase;

    /* File position */
    uint64_t offset;

    /* Pending fetch info */
    uint64_t fetch_offset;
    uint64_t fetch_size;
    void* fetch_dst;

    /* Parsing context */
    uint32_t current_part;         /* Which part we're parsing */
    uint32_t current_chunk;        /* Which chunk we're reading */
    int32_t current_y;             /* Current Y coordinate for chunk */

    /* Partial data storage */
    uint8_t temp_buffer[512];      /* For attribute parsing */
    size_t temp_size;

    /* Async completion tracking */
    ExrResult async_result;
    size_t async_bytes_read;
    int async_complete;

    /* Reference to decoder */
    ExrDecoder decoder;
};

#define EXR_SUSPEND_MAGIC 0x53555350  /* 'SUSP' */

static int exr_suspend_is_valid(ExrSuspendState state) {
    return state != NULL && state->magic == EXR_SUSPEND_MAGIC;
}

static int exr_decoder_is_valid(ExrDecoder decoder) {
    return decoder != NULL && decoder->magic == EXR_DECODER_MAGIC;
}

/* ============================================================================
 * Decoder Creation/Destruction
 * ============================================================================ */

ExrResult exr_decoder_create(ExrContext ctx,
                              const ExrDecoderCreateInfo* create_info,
                              ExrDecoder* out_decoder) {
    if (!exr_context_is_valid(ctx)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!create_info || !out_decoder) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }
    if (!create_info->source.fetch) {
        exr_context_add_error(ctx, EXR_ERROR_INVALID_ARGUMENT,
                              "Data source fetch callback is required", NULL, 0);
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    *out_decoder = NULL;

    /* Allocate decoder */
    ExrDecoder decoder = (ExrDecoder)ctx->allocator.alloc(
        ctx->allocator.userdata,
        sizeof(struct ExrDecoder_T),
        EXR_DEFAULT_ALIGNMENT
    );
    if (!decoder) {
        exr_context_add_error(ctx, EXR_ERROR_OUT_OF_MEMORY,
                              "Failed to allocate decoder", NULL, 0);
        return EXR_ERROR_OUT_OF_MEMORY;
    }

    memset(decoder, 0, sizeof(struct ExrDecoder_T));
    decoder->ctx = ctx;
    decoder->source = create_info->source;
    decoder->scratch_pool = create_info->scratch_pool;
    decoder->flags = create_info->flags;
    decoder->state = EXR_DECODER_STATE_CREATED;
    decoder->magic = EXR_DECODER_MAGIC;

    exr_context_add_ref(ctx);
    *out_decoder = decoder;
    return EXR_SUCCESS;
}

void exr_decoder_destroy(ExrDecoder decoder) {
    if (!exr_decoder_is_valid(decoder)) return;

    ExrContext ctx = decoder->ctx;
    decoder->magic = 0;

    /* Free suspend state if present */
    if (decoder->suspend_state) {
        decoder->suspend_state->magic = 0;  /* Invalidate first */
        ctx->allocator.free(ctx->allocator.userdata, decoder->suspend_state,
                            sizeof(struct ExrSuspendState_T));
        decoder->suspend_state = NULL;
    }

    /* Free read buffer */
    if (decoder->read_buffer) {
        ctx->allocator.free(ctx->allocator.userdata,
                            decoder->read_buffer, decoder->read_buffer_size);
    }

    /* Destroy image if owned */
    if (decoder->image) {
        exr_image_destroy(decoder->image);
    }

    ctx->allocator.free(ctx->allocator.userdata, decoder,
                        sizeof(struct ExrDecoder_T));
    exr_context_release(ctx);
}

ExrResult exr_decoder_set_progress_callback(ExrDecoder decoder,
                                             ExrProgressCallback callback,
                                             void* userdata,
                                             int32_t interval_ms) {
    if (!exr_decoder_is_valid(decoder)) {
        return EXR_ERROR_INVALID_HANDLE;
    }

    decoder->progress_callback = callback;
    decoder->progress_userdata = userdata;
    decoder->progress_interval_ms = interval_ms;
    return EXR_SUCCESS;
}

/* ============================================================================
 * Image Internal Structure
 * ============================================================================ */

typedef struct ExrChannelData {
    char name[64];
    uint32_t pixel_type;
    int32_t x_sampling;
    int32_t y_sampling;
    uint8_t p_linear;
} ExrChannelData;

typedef struct ExrAttributeData {
    char name[256];
    char type_name[64];
    uint8_t* value;
    uint32_t size;
    ExrAttributeType type;
} ExrAttributeData;

typedef struct ExrPartData {
    char* name;
    char* type_string;
    uint32_t part_type;
    int32_t width;
    int32_t height;
    uint32_t num_channels;
    ExrChannelData* channels;
    uint32_t compression;
    uint32_t flags;

    /* Offset table */
    uint64_t* offsets;
    uint32_t num_chunks;

    /* Tile info */
    uint32_t tile_size_x;
    uint32_t tile_size_y;
    uint32_t tile_level_mode;
    uint32_t tile_rounding_mode;
    uint32_t num_x_levels;
    uint32_t num_y_levels;

    /* Attributes */
    ExrAttributeData* attributes;
    uint32_t num_attributes;
} ExrPartData;

struct ExrImage_T {
    ExrDecoder decoder;  /* Back reference */
    ExrContext ctx;

    /* Version info */
    int32_t version;
    uint32_t flags;  /* ExrImageFlags */

    /* Global info */
    ExrBox2i data_window;
    ExrBox2i display_window;
    float pixel_aspect_ratio;
    ExrVec2f screen_window_center;
    float screen_window_width;

    /* Parts */
    ExrPartData* parts;
    uint32_t num_parts;

    uint32_t magic;
};

#define EXR_IMAGE_MAGIC 0x494D4147  /* 'IMAG' */

static int exr_image_is_valid(ExrImage image) {
    return image != NULL && image->magic == EXR_IMAGE_MAGIC;
}

void exr_image_destroy(ExrImage image) {
    if (!exr_image_is_valid(image)) return;

    ExrContext ctx = image->ctx;
    image->magic = 0;

    /* Free parts */
    for (uint32_t i = 0; i < image->num_parts; i++) {
        ExrPartData* part = &image->parts[i];
        if (part->name) {
            ctx->allocator.free(ctx->allocator.userdata, part->name,
                               strlen(part->name) + 1);
        }
        if (part->type_string) {
            ctx->allocator.free(ctx->allocator.userdata, part->type_string,
                               strlen(part->type_string) + 1);
        }
        if (part->channels) {
            ctx->allocator.free(ctx->allocator.userdata, part->channels,
                               part->num_channels * sizeof(ExrChannelData));
        }
        if (part->offsets) {
            ctx->allocator.free(ctx->allocator.userdata, part->offsets,
                               part->num_chunks * sizeof(uint64_t));
        }
        if (part->attributes) {
            for (uint32_t j = 0; j < part->num_attributes; j++) {
                if (part->attributes[j].value) {
                    ctx->allocator.free(ctx->allocator.userdata,
                                       part->attributes[j].value,
                                       part->attributes[j].size);
                }
            }
            ctx->allocator.free(ctx->allocator.userdata, part->attributes,
                               part->num_attributes * sizeof(ExrAttributeData));
        }
    }

    if (image->parts) {
        ctx->allocator.free(ctx->allocator.userdata, image->parts,
                           image->num_parts * sizeof(ExrPartData));
    }

    ctx->allocator.free(ctx->allocator.userdata, image, sizeof(struct ExrImage_T));
    exr_context_release(ctx);
}

ExrResult exr_image_get_info(ExrImage image, ExrImageInfo* out_info) {
    if (!exr_image_is_valid(image)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!out_info) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    memset(out_info, 0, sizeof(ExrImageInfo));

    out_info->width = image->data_window.max_x - image->data_window.min_x + 1;
    out_info->height = image->data_window.max_y - image->data_window.min_y + 1;
    out_info->data_window = image->data_window;
    out_info->display_window = image->display_window;
    out_info->num_parts = image->num_parts;
    out_info->flags = image->flags;
    out_info->pixel_aspect_ratio = image->pixel_aspect_ratio;
    out_info->screen_window_center = image->screen_window_center;
    out_info->screen_window_width = image->screen_window_width;

    /* Get info from first part */
    if (image->num_parts > 0) {
        ExrPartData* part = &image->parts[0];
        out_info->num_channels = part->num_channels;
        out_info->compression = part->compression;
        out_info->tile_size_x = part->tile_size_x;
        out_info->tile_size_y = part->tile_size_y;
        out_info->num_x_levels = part->num_x_levels;
        out_info->num_y_levels = part->num_y_levels;
    }

    return EXR_SUCCESS;
}

ExrResult exr_image_get_channel_count(ExrImage image, uint32_t* out_count) {
    if (!exr_image_is_valid(image)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!out_count) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }
    if (image->num_parts == 0) {
        *out_count = 0;
        return EXR_SUCCESS;
    }
    *out_count = image->parts[0].num_channels;
    return EXR_SUCCESS;
}

ExrResult exr_image_get_channel(ExrImage image, uint32_t index,
                                 ExrChannelInfo* out_info) {
    if (!exr_image_is_valid(image)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!out_info) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }
    if (image->num_parts == 0) {
        return EXR_ERROR_OUT_OF_BOUNDS;
    }

    ExrPartData* part = &image->parts[0];
    if (index >= part->num_channels) {
        return EXR_ERROR_OUT_OF_BOUNDS;
    }

    ExrChannelData* ch = &part->channels[index];
    out_info->name = ch->name;
    out_info->pixel_type = ch->pixel_type;
    out_info->x_sampling = ch->x_sampling;
    out_info->y_sampling = ch->y_sampling;
    out_info->p_linear = ch->p_linear;

    return EXR_SUCCESS;
}

ExrResult exr_image_find_channel(ExrImage image, const char* name,
                                  uint32_t* out_index) {
    if (!exr_image_is_valid(image)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!name || !out_index) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }
    if (image->num_parts == 0) {
        return EXR_ERROR_MISSING_ATTRIBUTE;
    }

    ExrPartData* part = &image->parts[0];
    for (uint32_t i = 0; i < part->num_channels; i++) {
        if (strcmp(part->channels[i].name, name) == 0) {
            *out_index = i;
            return EXR_SUCCESS;
        }
    }

    return EXR_ERROR_MISSING_ATTRIBUTE;
}

ExrResult exr_image_get_part_count(ExrImage image, uint32_t* out_count) {
    if (!exr_image_is_valid(image)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!out_count) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }
    *out_count = image->num_parts;
    return EXR_SUCCESS;
}

/* ============================================================================
 * Part Internal Structure (ExrPart is a pointer into ExrImage)
 * ============================================================================ */

/* ExrPart is actually just an index + image reference */
struct ExrPart_T {
    ExrImage image;
    uint32_t part_index;
    uint32_t magic;
};

#define EXR_PART_MAGIC 0x50415254  /* 'PART' */

/* For now, we use a simple approach where ExrPart is allocated separately.
 * In a more optimized implementation, we could embed part handles in the image. */

ExrResult exr_image_get_part(ExrImage image, uint32_t index, ExrPart* out_part) {
    if (!exr_image_is_valid(image)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!out_part) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }
    if (index >= image->num_parts) {
        return EXR_ERROR_OUT_OF_BOUNDS;
    }

    /* Allocate part handle */
    ExrPart part = (ExrPart)image->ctx->allocator.alloc(
        image->ctx->allocator.userdata,
        sizeof(struct ExrPart_T),
        EXR_DEFAULT_ALIGNMENT
    );
    if (!part) {
        return EXR_ERROR_OUT_OF_MEMORY;
    }

    part->image = image;
    part->part_index = index;
    part->magic = EXR_PART_MAGIC;

    *out_part = part;
    return EXR_SUCCESS;
}

static int exr_part_is_valid(ExrPart part) {
    return part != NULL && part->magic == EXR_PART_MAGIC &&
           exr_image_is_valid(part->image);
}

void exr_part_destroy(ExrPart part) {
    if (!exr_part_is_valid(part)) return;
    ExrContext ctx = part->image->ctx;
    part->magic = 0;
    ctx->allocator.free(ctx->allocator.userdata, part, sizeof(struct ExrPart_T));
}

static ExrPartData* exr_part_get_data(ExrPart part) {
    if (!exr_part_is_valid(part)) return NULL;
    return &part->image->parts[part->part_index];
}

ExrResult exr_part_get_info(ExrPart part, ExrPartInfo* out_info) {
    ExrPartData* data = exr_part_get_data(part);
    if (!data) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!out_info) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    out_info->name = data->name;
    out_info->type_string = data->type_string;
    out_info->part_type = data->part_type;
    out_info->width = data->width;
    out_info->height = data->height;
    out_info->num_channels = data->num_channels;
    out_info->compression = data->compression;
    out_info->flags = data->flags;

    return EXR_SUCCESS;
}

ExrResult exr_part_get_channel(ExrPart part, uint32_t index,
                                ExrChannelInfo* out_info) {
    ExrPartData* data = exr_part_get_data(part);
    if (!data) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!out_info) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }
    if (index >= data->num_channels) {
        return EXR_ERROR_OUT_OF_BOUNDS;
    }

    ExrChannelData* ch = &data->channels[index];
    out_info->name = ch->name;
    out_info->pixel_type = ch->pixel_type;
    out_info->x_sampling = ch->x_sampling;
    out_info->y_sampling = ch->y_sampling;
    out_info->p_linear = ch->p_linear;

    return EXR_SUCCESS;
}

ExrResult exr_part_get_chunk_count(ExrPart part, uint32_t* out_count) {
    ExrPartData* data = exr_part_get_data(part);
    if (!data) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!out_count) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }
    *out_count = data->num_chunks;
    return EXR_SUCCESS;
}

/* ============================================================================
 * Fence Internal Structure
 * ============================================================================ */

struct ExrFence_T {
    ExrContext ctx;
    ATOMIC_INT signaled;
    uint32_t magic;

#if defined(_WIN32)
    void* event;  /* HANDLE */
#else
    pthread_mutex_t mutex;
    pthread_cond_t cond;
#endif
};

#define EXR_FENCE_MAGIC 0x46454E43  /* 'FENC' */

ExrResult exr_fence_create(ExrContext ctx, const ExrFenceCreateInfo* create_info,
                            ExrFence* out_fence) {
    if (!exr_context_is_valid(ctx)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!out_fence) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    *out_fence = NULL;

    ExrFence fence = (ExrFence)ctx->allocator.alloc(
        ctx->allocator.userdata,
        sizeof(struct ExrFence_T),
        EXR_DEFAULT_ALIGNMENT
    );
    if (!fence) {
        return EXR_ERROR_OUT_OF_MEMORY;
    }

    memset(fence, 0, sizeof(struct ExrFence_T));
    fence->ctx = ctx;
    fence->magic = EXR_FENCE_MAGIC;

    int initial_state = (create_info && (create_info->flags & EXR_FENCE_SIGNALED)) ? 1 : 0;
    ATOMIC_INIT(fence->signaled, initial_state);

#if defined(_WIN32)
    fence->event = CreateEventA(NULL, TRUE, initial_state, NULL);
    if (!fence->event) {
        ctx->allocator.free(ctx->allocator.userdata, fence, sizeof(struct ExrFence_T));
        return EXR_ERROR_OUT_OF_MEMORY;
    }
#else
    pthread_mutex_init(&fence->mutex, NULL);
    pthread_cond_init(&fence->cond, NULL);
#endif

    exr_context_add_ref(ctx);
    *out_fence = fence;
    return EXR_SUCCESS;
}

void exr_fence_destroy(ExrFence fence) {
    if (!fence || fence->magic != EXR_FENCE_MAGIC) return;

    ExrContext ctx = fence->ctx;
    fence->magic = 0;

#if defined(_WIN32)
    if (fence->event) {
        CloseHandle(fence->event);
    }
#else
    pthread_cond_destroy(&fence->cond);
    pthread_mutex_destroy(&fence->mutex);
#endif

    ctx->allocator.free(ctx->allocator.userdata, fence, sizeof(struct ExrFence_T));
    exr_context_release(ctx);
}

ExrResult exr_fence_wait(ExrFence fence, uint64_t timeout_ns) {
    if (!fence || fence->magic != EXR_FENCE_MAGIC) {
        return EXR_ERROR_INVALID_HANDLE;
    }

    /* Fast path: already signaled */
    if (ATOMIC_LOAD(fence->signaled)) {
        return EXR_SUCCESS;
    }

    if (timeout_ns == EXR_TIMEOUT_NONE) {
        return ATOMIC_LOAD(fence->signaled) ? EXR_SUCCESS : EXR_ERROR_NOT_READY;
    }

#if defined(_WIN32)
    DWORD timeout_ms = (timeout_ns == EXR_TIMEOUT_INFINITE) ? INFINITE :
                       (DWORD)(timeout_ns / 1000000);
    DWORD result = WaitForSingleObject(fence->event, timeout_ms);
    if (result == WAIT_OBJECT_0) {
        return EXR_SUCCESS;
    } else if (result == WAIT_TIMEOUT) {
        return EXR_ERROR_TIMEOUT;
    }
    return EXR_ERROR_IO;
#else
    pthread_mutex_lock(&fence->mutex);

    if (timeout_ns == EXR_TIMEOUT_INFINITE) {
        while (!ATOMIC_LOAD(fence->signaled)) {
            pthread_cond_wait(&fence->cond, &fence->mutex);
        }
    } else {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += timeout_ns / 1000000000ULL;
        ts.tv_nsec += timeout_ns % 1000000000ULL;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }

        int rc = 0;
        while (!ATOMIC_LOAD(fence->signaled) && rc == 0) {
            rc = pthread_cond_timedwait(&fence->cond, &fence->mutex, &ts);
        }
        if (rc == ETIMEDOUT) {
            pthread_mutex_unlock(&fence->mutex);
            return EXR_ERROR_TIMEOUT;
        }
    }

    pthread_mutex_unlock(&fence->mutex);
    return EXR_SUCCESS;
#endif
}

ExrResult exr_fence_get_status(ExrFence fence) {
    if (!fence || fence->magic != EXR_FENCE_MAGIC) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    return ATOMIC_LOAD(fence->signaled) ? EXR_SUCCESS : EXR_ERROR_NOT_READY;
}

ExrResult exr_fence_reset(ExrFence fence) {
    if (!fence || fence->magic != EXR_FENCE_MAGIC) {
        return EXR_ERROR_INVALID_HANDLE;
    }

    ATOMIC_STORE(fence->signaled, 0);

#if defined(_WIN32)
    ResetEvent(fence->event);
#endif

    return EXR_SUCCESS;
}

/* Signal fence (internal function) */
static void exr_fence_signal(ExrFence fence) {
    if (!fence || fence->magic != EXR_FENCE_MAGIC) return;

    ATOMIC_STORE(fence->signaled, 1);

#if defined(_WIN32)
    SetEvent(fence->event);
#else
    pthread_mutex_lock(&fence->mutex);
    pthread_cond_broadcast(&fence->cond);
    pthread_mutex_unlock(&fence->mutex);
#endif
}

/* ============================================================================
 * Command Buffer Implementation
 * ============================================================================ */

/* Command types */
typedef enum ExrCommandType {
    EXR_CMD_TYPE_NONE = 0,
    EXR_CMD_TYPE_READ_TILE,
    EXR_CMD_TYPE_READ_SCANLINES,
    EXR_CMD_TYPE_READ_FULL_IMAGE,
    EXR_CMD_TYPE_WRITE_TILE,
    EXR_CMD_TYPE_WRITE_SCANLINES,
} ExrCommandType;

/* Base command structure */
typedef struct ExrCommand {
    ExrCommandType type;
    uint32_t part_index;
} ExrCommand;

/* Tile read command */
typedef struct ExrTileReadCmd {
    ExrCommand base;
    int32_t tile_x;
    int32_t tile_y;
    int32_t level_x;
    int32_t level_y;
    void* output;
    size_t output_size;
    uint32_t channels_mask;
    uint32_t output_pixel_type;
    uint32_t output_layout;
} ExrTileReadCmd;

/* Scanline read command */
typedef struct ExrScanlineReadCmd {
    ExrCommand base;
    int32_t y_start;
    int32_t num_lines;
    void* output;
    size_t output_size;
    uint32_t channels_mask;
    uint32_t output_pixel_type;
    uint32_t output_layout;
} ExrScanlineReadCmd;

/* Full image read command */
typedef struct ExrFullImageReadCmd {
    ExrCommand base;
    void* output;
    size_t output_size;
    uint32_t channels_mask;
    uint32_t output_pixel_type;
    uint32_t output_layout;
    int32_t target_level;
} ExrFullImageReadCmd;

/* Union for all command types */
typedef union ExrCommandUnion {
    ExrCommand base;
    ExrTileReadCmd tile_read;
    ExrScanlineReadCmd scanline_read;
    ExrFullImageReadCmd full_image_read;
} ExrCommandUnion;

#define EXR_INITIAL_CMD_CAPACITY 16

struct ExrCommandBuffer_T {
    ExrContext ctx;
    ExrDecoder decoder;
    ExrEncoder encoder;
    uint32_t flags;

    /* Command storage */
    ExrCommandUnion* commands;
    uint32_t command_count;
    uint32_t command_capacity;

    int recording;
    uint32_t magic;
};

#define EXR_COMMAND_BUFFER_MAGIC 0x434D4442  /* 'CMDB' */

static int exr_command_buffer_is_valid(ExrCommandBuffer cmd) {
    return cmd != NULL && cmd->magic == EXR_COMMAND_BUFFER_MAGIC;
}

/* Ensure command buffer has capacity for one more command */
static ExrResult ensure_command_capacity(ExrCommandBuffer cmd) {
    if (cmd->command_count < cmd->command_capacity) {
        return EXR_SUCCESS;
    }

    ExrContext ctx = cmd->ctx;
    uint32_t new_capacity = cmd->command_capacity == 0 ?
        EXR_INITIAL_CMD_CAPACITY : cmd->command_capacity * 2;

    ExrCommandUnion* new_commands = (ExrCommandUnion*)ctx->allocator.alloc(
        ctx->allocator.userdata,
        new_capacity * sizeof(ExrCommandUnion),
        EXR_DEFAULT_ALIGNMENT
    );
    if (!new_commands) {
        return EXR_ERROR_OUT_OF_MEMORY;
    }

    if (cmd->commands) {
        memcpy(new_commands, cmd->commands,
               cmd->command_count * sizeof(ExrCommandUnion));
        ctx->allocator.free(ctx->allocator.userdata, cmd->commands,
                            cmd->command_capacity * sizeof(ExrCommandUnion));
    }

    cmd->commands = new_commands;
    cmd->command_capacity = new_capacity;
    return EXR_SUCCESS;
}

ExrResult exr_command_buffer_create(ExrContext ctx,
                                     const ExrCommandBufferCreateInfo* create_info,
                                     ExrCommandBuffer* out_cmd) {
    if (!exr_context_is_valid(ctx)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!create_info || !out_cmd) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    *out_cmd = NULL;

    ExrCommandBuffer cmd = (ExrCommandBuffer)ctx->allocator.alloc(
        ctx->allocator.userdata,
        sizeof(struct ExrCommandBuffer_T),
        EXR_DEFAULT_ALIGNMENT
    );
    if (!cmd) {
        return EXR_ERROR_OUT_OF_MEMORY;
    }

    memset(cmd, 0, sizeof(struct ExrCommandBuffer_T));
    cmd->ctx = ctx;
    cmd->decoder = create_info->decoder;
    cmd->encoder = create_info->encoder;
    cmd->flags = create_info->flags;
    cmd->magic = EXR_COMMAND_BUFFER_MAGIC;

    /* Pre-allocate command storage if specified */
    if (create_info->max_commands > 0) {
        cmd->commands = (ExrCommandUnion*)ctx->allocator.alloc(
            ctx->allocator.userdata,
            create_info->max_commands * sizeof(ExrCommandUnion),
            EXR_DEFAULT_ALIGNMENT
        );
        if (!cmd->commands) {
            ctx->allocator.free(ctx->allocator.userdata, cmd,
                               sizeof(struct ExrCommandBuffer_T));
            return EXR_ERROR_OUT_OF_MEMORY;
        }
        cmd->command_capacity = create_info->max_commands;
    }

    exr_context_add_ref(ctx);
    *out_cmd = cmd;
    return EXR_SUCCESS;
}

void exr_command_buffer_destroy(ExrCommandBuffer cmd) {
    if (!exr_command_buffer_is_valid(cmd)) return;

    ExrContext ctx = cmd->ctx;
    cmd->magic = 0;

    /* Free commands if any */
    if (cmd->commands) {
        ctx->allocator.free(ctx->allocator.userdata, cmd->commands,
                            cmd->command_capacity * sizeof(ExrCommandUnion));
    }

    ctx->allocator.free(ctx->allocator.userdata, cmd, sizeof(struct ExrCommandBuffer_T));
    exr_context_release(ctx);
}

ExrResult exr_command_buffer_reset(ExrCommandBuffer cmd) {
    if (!exr_command_buffer_is_valid(cmd)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    cmd->command_count = 0;
    cmd->recording = 0;
    return EXR_SUCCESS;
}

ExrResult exr_command_buffer_begin(ExrCommandBuffer cmd) {
    if (!exr_command_buffer_is_valid(cmd)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (cmd->recording) {
        return EXR_ERROR_INVALID_STATE;
    }
    cmd->recording = 1;
    cmd->command_count = 0;
    return EXR_SUCCESS;
}

ExrResult exr_command_buffer_end(ExrCommandBuffer cmd) {
    if (!exr_command_buffer_is_valid(cmd)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!cmd->recording) {
        return EXR_ERROR_INVALID_STATE;
    }
    cmd->recording = 0;
    return EXR_SUCCESS;
}

/* ============================================================================
 * Command Recording Functions
 * ============================================================================ */

ExrResult exr_cmd_request_tile(ExrCommandBuffer cmd, const ExrTileRequest* request) {
    if (!exr_command_buffer_is_valid(cmd)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!cmd->recording) {
        return EXR_ERROR_INVALID_STATE;
    }
    if (!request || !request->part || !request->output.data) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    ExrResult result = ensure_command_capacity(cmd);
    if (EXR_FAILED(result)) return result;

    ExrTileReadCmd* tile_cmd = &cmd->commands[cmd->command_count].tile_read;
    tile_cmd->base.type = EXR_CMD_TYPE_READ_TILE;
    tile_cmd->base.part_index = request->part->part_index;
    tile_cmd->tile_x = request->tile_x;
    tile_cmd->tile_y = request->tile_y;
    tile_cmd->level_x = request->level_x;
    tile_cmd->level_y = request->level_y;
    tile_cmd->output = request->output.data;
    tile_cmd->output_size = request->output.size;
    tile_cmd->channels_mask = request->channels_mask;
    tile_cmd->output_pixel_type = request->output_pixel_type;
    tile_cmd->output_layout = request->output_layout;

    cmd->command_count++;
    return EXR_SUCCESS;
}

ExrResult exr_cmd_request_tiles(ExrCommandBuffer cmd, uint32_t count,
                                 const ExrTileRequest* requests) {
    if (!exr_command_buffer_is_valid(cmd)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!requests || count == 0) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    for (uint32_t i = 0; i < count; i++) {
        ExrResult result = exr_cmd_request_tile(cmd, &requests[i]);
        if (EXR_FAILED(result)) return result;
    }
    return EXR_SUCCESS;
}

ExrResult exr_cmd_request_scanlines(ExrCommandBuffer cmd,
                                     const ExrScanlineRequest* request) {
    if (!exr_command_buffer_is_valid(cmd)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!cmd->recording) {
        return EXR_ERROR_INVALID_STATE;
    }
    if (!request || !request->part || !request->output.data) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    ExrResult result = ensure_command_capacity(cmd);
    if (EXR_FAILED(result)) return result;

    ExrScanlineReadCmd* scan_cmd = &cmd->commands[cmd->command_count].scanline_read;
    scan_cmd->base.type = EXR_CMD_TYPE_READ_SCANLINES;
    scan_cmd->base.part_index = request->part->part_index;
    scan_cmd->y_start = request->y_start;
    scan_cmd->num_lines = request->num_lines;
    scan_cmd->output = request->output.data;
    scan_cmd->output_size = request->output.size;
    scan_cmd->channels_mask = request->channels_mask;
    scan_cmd->output_pixel_type = request->output_pixel_type;
    scan_cmd->output_layout = request->output_layout;

    cmd->command_count++;
    return EXR_SUCCESS;
}

ExrResult exr_cmd_request_scanline_blocks(ExrCommandBuffer cmd, uint32_t count,
                                           const ExrScanlineRequest* requests) {
    if (!exr_command_buffer_is_valid(cmd)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!requests || count == 0) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    for (uint32_t i = 0; i < count; i++) {
        ExrResult result = exr_cmd_request_scanlines(cmd, &requests[i]);
        if (EXR_FAILED(result)) return result;
    }
    return EXR_SUCCESS;
}

ExrResult exr_cmd_request_full_image(ExrCommandBuffer cmd,
                                      const ExrFullImageRequest* request) {
    if (!exr_command_buffer_is_valid(cmd)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!cmd->recording) {
        return EXR_ERROR_INVALID_STATE;
    }
    if (!request || !request->part || !request->output.data) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    ExrResult result = ensure_command_capacity(cmd);
    if (EXR_FAILED(result)) return result;

    ExrFullImageReadCmd* full_cmd = &cmd->commands[cmd->command_count].full_image_read;
    full_cmd->base.type = EXR_CMD_TYPE_READ_FULL_IMAGE;
    full_cmd->base.part_index = request->part->part_index;
    full_cmd->output = request->output.data;
    full_cmd->output_size = request->output.size;
    full_cmd->channels_mask = request->channels_mask;
    full_cmd->output_pixel_type = request->output_pixel_type;
    full_cmd->output_layout = request->output_layout;
    full_cmd->target_level = request->target_level;

    cmd->command_count++;
    return EXR_SUCCESS;
}

/* ============================================================================
 * Compression/Decompression Support
 * ============================================================================ */

/* Use V2 deflate, PIZ, PXR24, B44 implementation by default */
#ifdef __cplusplus
#ifndef TINYEXR_V3_USE_MINIZ
#include "tinyexr_huffman.hh"
#include "tinyexr_piz.hh"
#include "tinyexr_v2_impl.hh"
#define TINYEXR_V3_HAS_DEFLATE 1
#define TINYEXR_V3_HAS_PIZ 1
#define TINYEXR_V3_HAS_PXR24 1
#define TINYEXR_V3_HAS_B44 1

/* V1 PXR24 support - requires external wrapper function */
#ifdef TINYEXR_V3_ENABLE_PXR24
#ifndef TINYEXR_V3_ENABLE_PIZ
#include "tinyexr.h"  /* For EXRChannelInfo type */
#endif
extern "C" bool tinyexr_v3_decompress_pxr24(
    unsigned char* outPtr, size_t outBufSize,
    const unsigned char* inPtr, size_t inLen,
    int data_width, int num_lines,
    size_t num_channels, const EXRChannelInfo* channels);
#define TINYEXR_V3_USE_V1_PXR24 1
#endif

/* V1 B44 support - requires external wrapper function */
#ifdef TINYEXR_V3_ENABLE_B44
#if !defined(TINYEXR_V3_ENABLE_PIZ) && !defined(TINYEXR_V3_ENABLE_PXR24)
#include "tinyexr.h"  /* For EXRChannelInfo type */
#endif
extern "C" bool tinyexr_v3_decompress_b44(
    unsigned char* outPtr, size_t outBufSize,
    const unsigned char* inPtr, size_t inLen,
    int data_width, int num_lines,
    size_t num_channels, const EXRChannelInfo* channels, bool is_b44a);
#define TINYEXR_V3_USE_V1_B44 1
#endif

#endif
#endif

/* Fall back to miniz if V2 deflate not available */
#if !defined(TINYEXR_V3_HAS_DEFLATE) && !defined(TINYEXR_V3_NO_MINIZ)
#define MINIZ_NO_STDIO
#define MINIZ_NO_TIME
#define MINIZ_NO_ARCHIVE_APIS
#define MINIZ_NO_ARCHIVE_WRITING_APIS
#include "deps/miniz/miniz.h"
#define TINYEXR_V3_USE_MINIZ 1
#endif

/* Forward declarations of helper functions defined in Header Parsing section */
static int32_t read_le_i32(const uint8_t* p);
static uint32_t read_le_u32(const uint8_t* p);
static uint64_t read_le_u64(const uint8_t* p);
static ExrResult sync_fetch(ExrDecoder decoder, uint64_t offset, uint64_t size, void* dst);

/* ZIP decompression with EXR-specific post-processing */
static ExrResult decompress_zip(const uint8_t* src, size_t src_size,
                                 uint8_t* dst, size_t dst_size,
                                 size_t* out_size, ExrContext ctx) {
    /* If sizes match, data is not compressed (Issue 40) */
    if (src_size == dst_size) {
        memcpy(dst, src, src_size);
        *out_size = src_size;
        return EXR_SUCCESS;
    }

    /* Allocate temp buffer for decompression */
    uint8_t* tmpBuf = (uint8_t*)ctx->allocator.alloc(
        ctx->allocator.userdata, dst_size, EXR_DEFAULT_ALIGNMENT);
    if (!tmpBuf) {
        return EXR_ERROR_OUT_OF_MEMORY;
    }

#if defined(TINYEXR_V3_HAS_DEFLATE)
    /* Use V2 deflate implementation */
    size_t uncomp_size = dst_size;
    bool ok = tinyexr::huffman::inflate_zlib(src, src_size, tmpBuf, &uncomp_size);
    if (!ok) {
        ctx->allocator.free(ctx->allocator.userdata, tmpBuf, dst_size);
        return EXR_ERROR_DECOMPRESSION_FAILED;
    }
#elif defined(TINYEXR_V3_USE_MINIZ)
    /* Fall back to miniz */
    mz_ulong uncomp_size = (mz_ulong)dst_size;
    int ret = mz_uncompress(tmpBuf, &uncomp_size, src, (mz_ulong)src_size);
    if (ret != MZ_OK) {
        ctx->allocator.free(ctx->allocator.userdata, tmpBuf, dst_size);
        return EXR_ERROR_DECOMPRESSION_FAILED;
    }
#else
    ctx->allocator.free(ctx->allocator.userdata, tmpBuf, dst_size);
    (void)src; (void)src_size; (void)dst; (void)dst_size; (void)out_size;
    return EXR_ERROR_UNSUPPORTED_FORMAT;
#endif

    /* Apply EXR predictor (delta decoding) */
    {
        uint8_t* t = tmpBuf + 1;
        uint8_t* stop = tmpBuf + uncomp_size;
        while (t < stop) {
            int d = (int)t[-1] + (int)t[0] - 128;
            t[0] = (uint8_t)d;
            ++t;
        }
    }

    /* Reorder pixel data (interleave two halves) */
    {
        const uint8_t* t1 = tmpBuf;
        const uint8_t* t2 = tmpBuf + (uncomp_size + 1) / 2;
        uint8_t* s = dst;
        uint8_t* stop = dst + uncomp_size;

        while (s < stop) {
            if (s < stop) *s++ = *t1++;
            if (s < stop) *s++ = *t2++;
        }
    }

    ctx->allocator.free(ctx->allocator.userdata, tmpBuf, dst_size);
    *out_size = uncomp_size;
    return EXR_SUCCESS;
}

/* ============================================================================
 * Command Execution (Decompression)
 * ============================================================================ */

/* Get lines per block for a compression type */
static int get_lines_per_block(uint8_t compression) {
    switch (compression) {
        case EXR_COMPRESSION_NONE:
        case EXR_COMPRESSION_RLE:
        case EXR_COMPRESSION_ZIPS:
            return 1;
        case EXR_COMPRESSION_ZIP:
        case EXR_COMPRESSION_PXR24:
            return 16;
        case EXR_COMPRESSION_PIZ:
        case EXR_COMPRESSION_B44:
        case EXR_COMPRESSION_B44A:
        case EXR_COMPRESSION_DWAA:
            return 32;
        case EXR_COMPRESSION_DWAB:
            return 256;
        default:
            return 1;
    }
}

/* Calculate bytes per pixel for a channel */
static int get_bytes_per_pixel(uint32_t pixel_type) {
    switch (pixel_type) {
        case EXR_PIXEL_UINT:   return 4;
        case EXR_PIXEL_HALF:   return 2;
        case EXR_PIXEL_FLOAT:  return 4;
        default:               return 2;
    }
}

/* ============================================================================
 * PIZ Decompression
 * ============================================================================ */

#define PIZ_BITMAP_SIZE 8192
#define PIZ_USHORT_RANGE 65536
#define PIZ_HUF_ENCBITS 16
#define PIZ_HUF_ENCSIZE ((1 << PIZ_HUF_ENCBITS) + 1)

/* PIZ channel data structure */
typedef struct {
    uint16_t* start;
    uint16_t* end;
    int nx;
    int ny;
    int size;  /* 1 for HALF, 2 for FLOAT/UINT */
} PizChannelData;

/* Build reverse LUT from bitmap */
static uint16_t piz_reverse_lut_from_bitmap(const uint8_t* bitmap, uint16_t* lut) {
    uint16_t k = 0;
    for (int i = 0; i < PIZ_USHORT_RANGE; i++) {
        if (i == 0 || (bitmap[i >> 3] & (1 << (i & 7)))) {
            lut[k++] = (uint16_t)i;
        }
    }
    uint16_t n = k - 1;
    while (k < PIZ_USHORT_RANGE) {
        lut[k++] = 0;
    }
    return n;
}

/* Apply LUT to data */
static void piz_apply_lut(const uint16_t* lut, uint16_t* data, int n) {
    for (int i = 0; i < n; i++) {
        data[i] = lut[data[i]];
    }
}

/* Wavelet decode (16-bit lifting scheme) */
static void piz_wav2_decode(uint16_t* in, int nx, int ox, int ny, int oy, uint16_t mx) {
    int w14 = (mx < (1 << 14)) ? (mx + 1) : (1 << 14);
    int n = (nx > ny) ? ny : nx;
    int p = 1;
    int p2;

    while (p <= n) p <<= 1;
    p >>= 1;
    p2 = p;
    p >>= 1;

    for (; p >= 1; p2 = p, p >>= 1) {
        uint16_t* py = in;
        uint16_t* ey = in + oy * (ny - p2);
        int oy1 = oy * p;
        int oy2 = oy * p2;
        int ox1 = ox * p;
        int ox2 = ox * p2;

        for (; py <= ey; py += oy2) {
            uint16_t* px = py;
            uint16_t* ex = py + ox * (nx - p2);

            for (; px <= ex; px += ox2) {
                uint16_t* p00 = px;
                uint16_t* p10 = px + ox1;
                uint16_t* p01 = px + oy1;
                uint16_t* p11 = px + ox1 + oy1;

                uint16_t s00 = *p00;
                uint16_t s10 = *p10;
                uint16_t s01 = *p01;
                uint16_t s11 = *p11;

                /* Wdec14 for s00, s10 */
                int d = s10;
                int m = s00;
                if (d > (w14 - 1) || d < -(w14 - 1)) {
                    m = ((d >= 0) ? (d - (w14 - 1)) : (d + (w14 - 1)));
                    d -= m;
                }
                s10 = (uint16_t)(s00 + d);
                s00 = (uint16_t)(s00 - d);

                /* Wdec14 for s01, s11 */
                d = s11;
                m = s01;
                if (d > (w14 - 1) || d < -(w14 - 1)) {
                    m = ((d >= 0) ? (d - (w14 - 1)) : (d + (w14 - 1)));
                    d -= m;
                }
                s11 = (uint16_t)(s01 + d);
                s01 = (uint16_t)(s01 - d);

                /* Wdec14 for s00, s01 */
                d = s01;
                m = s00;
                if (d > (w14 - 1) || d < -(w14 - 1)) {
                    m = ((d >= 0) ? (d - (w14 - 1)) : (d + (w14 - 1)));
                    d -= m;
                }
                *p01 = (uint16_t)(s00 + d);
                *p00 = (uint16_t)(s00 - d);

                /* Wdec14 for s10, s11 */
                d = s11;
                m = s10;
                if (d > (w14 - 1) || d < -(w14 - 1)) {
                    m = ((d >= 0) ? (d - (w14 - 1)) : (d + (w14 - 1)));
                    d -= m;
                }
                *p11 = (uint16_t)(s10 + d);
                *p10 = (uint16_t)(s10 - d);
            }

            /* Handle remaining column if width is odd */
            if ((nx & p) != 0) {
                uint16_t* p00 = px;
                uint16_t* p01 = px + oy1;

                uint16_t s00 = *p00;
                uint16_t s01 = *p01;

                int d = s01;
                if (d > (w14 - 1) || d < -(w14 - 1)) {
                    d = ((d >= 0) ? (d - (w14 - 1)) : (d + (w14 - 1)));
                }
                *p01 = (uint16_t)(s00 + d);
                *p00 = (uint16_t)(s00 - d);
            }
        }

        /* Handle remaining row if height is odd */
        if ((ny & p) != 0) {
            uint16_t* px = py;
            uint16_t* ex = py + ox * (nx - p2);

            for (; px <= ex; px += ox2) {
                uint16_t* p00 = px;
                uint16_t* p10 = px + ox1;

                uint16_t s00 = *p00;
                uint16_t s10 = *p10;

                int d = s10;
                if (d > (w14 - 1) || d < -(w14 - 1)) {
                    d = ((d >= 0) ? (d - (w14 - 1)) : (d + (w14 - 1)));
                }
                *p10 = (uint16_t)(s00 + d);
                *p00 = (uint16_t)(s00 - d);
            }
        }
    }
}

/* Huffman table entry */
typedef struct {
    uint64_t code;
    int len;
} HufCode;

/* Huffman decoder table for fast path */
#define HUF_DECBITS 10
#define HUF_DECSIZE (1 << HUF_DECBITS)
#define HUF_DECMASK (HUF_DECSIZE - 1)

/* Decode Huffman symbol using table lookup */
static int piz_huf_decode_symbol(const uint64_t* hdec, const uint16_t* hdec_short,
                                  const uint8_t* data, size_t data_len,
                                  size_t* bit_pos, uint64_t* buffer, int* bits_in_buffer) {
    /* Refill buffer if needed */
    while (*bits_in_buffer < 32 && (*bit_pos / 8) < data_len) {
        size_t byte_pos = *bit_pos / 8;
        int bit_offset = *bit_pos % 8;
        *buffer |= ((uint64_t)data[byte_pos] >> bit_offset) << *bits_in_buffer;
        int bits_taken = 8 - bit_offset;
        *bits_in_buffer += bits_taken;
        *bit_pos += bits_taken;
    }

    /* Fast path: table lookup for short codes */
    int idx = *buffer & HUF_DECMASK;
    int len = hdec_short[idx] & 0x3F;
    if (len > 0 && len <= HUF_DECBITS) {
        int sym = hdec_short[idx] >> 6;
        *buffer >>= len;
        *bits_in_buffer -= len;
        return sym;
    }

    /* Slow path: linear search through longer codes */
    /* This is a simplified fallback - real implementation would use canonical codes */
    return -1;  /* Error: symbol not found */
}

/* Simplified Huffman decompression (V1-compatible format) */
static bool piz_huf_uncompress(const uint8_t* in, size_t in_len,
                                uint16_t* out, size_t out_len) {
    if (in_len < 20) return false;

    /* Read header */
    uint32_t im_val, iM_val, nBits_val;
    memcpy(&im_val, in, 4);
    memcpy(&iM_val, in + 4, 4);
    memcpy(&nBits_val, in + 12, 4);  /* nBits at offset 12 */

    if (im_val >= PIZ_HUF_ENCSIZE || iM_val >= PIZ_HUF_ENCSIZE || im_val > iM_val) {
        return false;
    }

    const uint8_t* ptr = in + 20;  /* Skip header */
    size_t remaining = in_len - 20;

    /* Read Huffman table: for each symbol from im to iM, read code length and code */
    uint64_t codes[PIZ_HUF_ENCSIZE] = {0};
    uint8_t lengths[PIZ_HUF_ENCSIZE] = {0};

    for (uint32_t i = im_val; i <= iM_val; /* incremented in loop */) {
        if (remaining < 1) return false;

        uint8_t l = *ptr++;
        remaining--;

        if (l >= 59) {
            /* Run of zero-length codes */
            uint32_t n = l - 59 + 2;
            i += n;
        } else if (l >= 0x3F) {
            /* Long code: 6 bits length + code */
            if (remaining < 4) return false;
            uint32_t packed;
            memcpy(&packed, ptr, 4);
            ptr += 4;
            remaining -= 4;
            lengths[i] = (uint8_t)(packed & 0x3F);
            codes[i] = packed >> 6;
            i++;
        } else {
            lengths[i] = l;
            /* Code follows based on length */
            if (l > 0) {
                int code_bytes = (l + 7) / 8;
                if ((size_t)code_bytes > remaining) return false;
                uint64_t code = 0;
                for (int b = 0; b < code_bytes; b++) {
                    code |= ((uint64_t)ptr[b]) << (b * 8);
                }
                ptr += code_bytes;
                remaining -= code_bytes;
                codes[i] = code;
            }
            i++;
        }
    }

    /* Build decoding table for fast lookup */
    uint16_t dec_table[HUF_DECSIZE] = {0};
    for (uint32_t sym = im_val; sym <= iM_val; sym++) {
        int len = lengths[sym];
        if (len > 0 && len <= HUF_DECBITS) {
            uint64_t code = codes[sym];
            int fill_count = 1 << (HUF_DECBITS - len);
            for (int f = 0; f < fill_count; f++) {
                int idx = (int)(code | (f << len));
                if (idx < HUF_DECSIZE) {
                    dec_table[idx] = (uint16_t)((sym << 6) | len);
                }
            }
        }
    }

    /* Decode symbols */
    uint64_t buffer = 0;
    int bits_in_buffer = 0;
    size_t bit_pos = 0;
    const uint8_t* bit_data = ptr;
    size_t bit_data_len = remaining;

    /* Initialize buffer */
    while (bits_in_buffer < 64 && bit_pos < bit_data_len * 8) {
        size_t byte_idx = bit_pos / 8;
        if (byte_idx < bit_data_len) {
            buffer |= ((uint64_t)bit_data[byte_idx]) << bits_in_buffer;
            bits_in_buffer += 8;
            bit_pos += 8;
        }
    }

    for (size_t i = 0; i < out_len; i++) {
        /* Refill buffer */
        while (bits_in_buffer < 32) {
            size_t byte_idx = bit_pos / 8;
            if (byte_idx >= bit_data_len) break;
            buffer |= ((uint64_t)bit_data[byte_idx]) << bits_in_buffer;
            bits_in_buffer += 8;
            bit_pos += 8;
        }

        /* Fast decode using table */
        int idx = buffer & HUF_DECMASK;
        uint16_t entry = dec_table[idx];
        int len = entry & 0x3F;
        if (len > 0 && len <= HUF_DECBITS) {
            out[i] = (uint16_t)(entry >> 6);
            buffer >>= len;
            bits_in_buffer -= len;
        } else {
            /* Slow path: search for longer codes */
            bool found = false;
            for (int test_len = HUF_DECBITS + 1; test_len <= 58 && !found; test_len++) {
                uint64_t mask = (1ULL << test_len) - 1;
                uint64_t test_code = buffer & mask;
                for (uint32_t sym = im_val; sym <= iM_val; sym++) {
                    if (lengths[sym] == test_len && codes[sym] == test_code) {
                        out[i] = (uint16_t)sym;
                        buffer >>= test_len;
                        bits_in_buffer -= test_len;
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                return false;  /* Decode error */
            }
        }
    }

    return true;
}

/* PIZ decompression */
static ExrResult decompress_piz(const uint8_t* src, size_t src_size,
                                 uint8_t* dst, size_t dst_size,
                                 size_t* out_size,
                                 int num_channels, ExrChannelData* channels,
                                 int data_width, int num_lines,
                                 ExrContext ctx) {
    /* Handle uncompressed case */
    if (src_size == dst_size) {
        memcpy(dst, src, src_size);
        *out_size = src_size;
        return EXR_SUCCESS;
    }

#if defined(TINYEXR_V3_HAS_PIZ)
    /* Use V2 PIZ implementation - convert channel info */
    tinyexr::v2::Channel* v2_channels = new tinyexr::v2::Channel[num_channels];
    for (int i = 0; i < num_channels; i++) {
        v2_channels[i].name = channels[i].name;
        v2_channels[i].pixel_type = (int)channels[i].pixel_type;
        v2_channels[i].x_sampling = channels[i].x_sampling;
        v2_channels[i].y_sampling = channels[i].y_sampling;
        v2_channels[i].p_linear = channels[i].p_linear;
    }

    auto result = tinyexr::piz::DecompressPizV2(
        dst, dst_size, src, src_size,
        num_channels, v2_channels, data_width, num_lines);

    delete[] v2_channels;

    if (!result.success) {
        return EXR_ERROR_DECOMPRESSION_FAILED;
    }

    *out_size = dst_size;
    return EXR_SUCCESS;
#else
    /* No PIZ implementation available */
    (void)src; (void)src_size; (void)dst; (void)dst_size;
    (void)out_size; (void)num_channels; (void)channels;
    (void)data_width; (void)num_lines; (void)ctx;
    return EXR_ERROR_UNSUPPORTED_FORMAT;
#endif
}

/* RLE decompression */
static ExrResult decompress_rle(const uint8_t* src, size_t src_size,
                                 uint8_t* dst, size_t dst_size,
                                 size_t* out_size) {
    size_t src_pos = 0;
    size_t dst_pos = 0;

    while (src_pos < src_size && dst_pos < dst_size) {
        int8_t count = (int8_t)src[src_pos++];

        if (count >= 0) {
            /* Literal run */
            size_t len = (size_t)count + 1;
            if (src_pos + len > src_size || dst_pos + len > dst_size) {
                return EXR_ERROR_INVALID_DATA;
            }
            memcpy(dst + dst_pos, src + src_pos, len);
            src_pos += len;
            dst_pos += len;
        } else {
            /* RLE run */
            size_t len = (size_t)(-count) + 1;
            if (src_pos >= src_size || dst_pos + len > dst_size) {
                return EXR_ERROR_INVALID_DATA;
            }
            uint8_t val = src[src_pos++];
            memset(dst + dst_pos, val, len);
            dst_pos += len;
        }
    }

    *out_size = dst_pos;
    return EXR_SUCCESS;
}

/* Reconstruct interleaved channels to planar format */
static void deinterleave_channels(const uint8_t* src, uint8_t* dst,
                                   int width, int height, int num_channels,
                                   int bytes_per_channel) {
    size_t pixel_stride = num_channels * bytes_per_channel;
    size_t channel_size = (size_t)width * height * bytes_per_channel;

    for (int c = 0; c < num_channels; c++) {
        uint8_t* channel_dst = dst + c * channel_size;
        for (int y = 0; y < height; y++) {
            for (int x = 0; x < width; x++) {
                const uint8_t* pixel_src = src + (y * width + x) * pixel_stride;
                memcpy(channel_dst + (y * width + x) * bytes_per_channel,
                       pixel_src + c * bytes_per_channel,
                       bytes_per_channel);
            }
        }
    }
}

/* Read and decompress a single chunk */
static ExrResult read_chunk(ExrDecoder decoder, ExrPartData* part, uint32_t chunk_index,
                            uint8_t** out_data, size_t* out_size,
                            int* out_y_start, int* out_num_lines) {
    ExrContext ctx = decoder->ctx;
    ExrResult result;

    if (chunk_index >= part->num_chunks) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    uint64_t offset = part->offsets[chunk_index];

    /* Read chunk header (y coordinate + data size) */
    uint8_t header[8];
    result = sync_fetch(decoder, offset, 8, header);
    if (EXR_FAILED(result)) return result;

    int32_t y_coord = read_le_i32(header);
    uint32_t data_size = read_le_u32(header + 4);

    /* Validate data size */
    if (data_size > 128 * 1024 * 1024) {  /* 128MB max */
        exr_context_add_error(ctx, EXR_ERROR_INVALID_DATA,
                              "Chunk data size too large", "chunk", offset);
        return EXR_ERROR_INVALID_DATA;
    }

    /* Calculate expected uncompressed size */
    int lines_per_block = get_lines_per_block(part->compression);
    int y_start = y_coord;
    int y_end = y_coord + lines_per_block;
    if (y_end > part->height) {
        y_end = part->height;
    }
    int num_lines = y_end - y_start;

    size_t bytes_per_line = 0;
    for (uint32_t c = 0; c < part->num_channels; c++) {
        bytes_per_line += (size_t)part->width * get_bytes_per_pixel(part->channels[c].pixel_type);
    }
    size_t expected_size = bytes_per_line * num_lines;

    /* Allocate compressed data buffer */
    uint8_t* compressed = (uint8_t*)ctx->allocator.alloc(
        ctx->allocator.userdata, data_size, EXR_DEFAULT_ALIGNMENT);
    if (!compressed) {
        return EXR_ERROR_OUT_OF_MEMORY;
    }

    /* Read compressed data */
    result = sync_fetch(decoder, offset + 8, data_size, compressed);
    if (EXR_FAILED(result)) {
        ctx->allocator.free(ctx->allocator.userdata, compressed, data_size);
        return result;
    }

    /* Allocate decompressed buffer */
    uint8_t* decompressed = (uint8_t*)ctx->allocator.alloc(
        ctx->allocator.userdata, expected_size, EXR_DEFAULT_ALIGNMENT);
    if (!decompressed) {
        ctx->allocator.free(ctx->allocator.userdata, compressed, data_size);
        return EXR_ERROR_OUT_OF_MEMORY;
    }

    /* Decompress based on compression type */
    size_t decompressed_size = 0;
    switch (part->compression) {
        case EXR_COMPRESSION_NONE:
            if (data_size != expected_size) {
                ctx->allocator.free(ctx->allocator.userdata, compressed, data_size);
                ctx->allocator.free(ctx->allocator.userdata, decompressed, expected_size);
                return EXR_ERROR_INVALID_DATA;
            }
            memcpy(decompressed, compressed, data_size);
            decompressed_size = data_size;
            break;

        case EXR_COMPRESSION_RLE:
            result = decompress_rle(compressed, data_size, decompressed,
                                     expected_size, &decompressed_size);
            if (EXR_FAILED(result)) {
                ctx->allocator.free(ctx->allocator.userdata, compressed, data_size);
                ctx->allocator.free(ctx->allocator.userdata, decompressed, expected_size);
                return result;
            }
            break;

        case EXR_COMPRESSION_ZIP:
        case EXR_COMPRESSION_ZIPS:
            result = decompress_zip(compressed, data_size, decompressed,
                                     expected_size, &decompressed_size, ctx);
            if (EXR_FAILED(result)) {
                exr_context_add_error(ctx, result,
                                      "ZIP decompression failed", "chunk", offset);
                ctx->allocator.free(ctx->allocator.userdata, compressed, data_size);
                ctx->allocator.free(ctx->allocator.userdata, decompressed, expected_size);
                return result;
            }
            break;

        case EXR_COMPRESSION_PIZ: {
#if defined(TINYEXR_V3_USE_V1_PIZ)
            /* Use V1's DecompressPiz - construct EXRChannelInfo array */
            EXRChannelInfo* v1_channels = (EXRChannelInfo*)ctx->allocator.alloc(
                ctx->allocator.userdata, part->num_channels * sizeof(EXRChannelInfo),
                EXR_DEFAULT_ALIGNMENT);
            if (!v1_channels) {
                ctx->allocator.free(ctx->allocator.userdata, compressed, data_size);
                ctx->allocator.free(ctx->allocator.userdata, decompressed, expected_size);
                return EXR_ERROR_OUT_OF_MEMORY;
            }

            for (uint32_t c = 0; c < part->num_channels; c++) {
                memset(&v1_channels[c], 0, sizeof(EXRChannelInfo));
                v1_channels[c].pixel_type = part->channels[c].pixel_type;
                v1_channels[c].x_sampling = part->channels[c].x_sampling;
                v1_channels[c].y_sampling = part->channels[c].y_sampling;
            }

            bool piz_ok = tinyexr_v3_decompress_piz(
                decompressed, compressed, expected_size, data_size,
                static_cast<int>(part->num_channels), v1_channels,
                part->width, num_lines);

            ctx->allocator.free(ctx->allocator.userdata, v1_channels,
                               part->num_channels * sizeof(EXRChannelInfo));

            if (!piz_ok) {
                exr_context_add_error(ctx, EXR_ERROR_DECOMPRESSION_FAILED,
                                      "PIZ decompression failed", "chunk", offset);
                ctx->allocator.free(ctx->allocator.userdata, compressed, data_size);
                ctx->allocator.free(ctx->allocator.userdata, decompressed, expected_size);
                return EXR_ERROR_DECOMPRESSION_FAILED;
            }
            decompressed_size = expected_size;
#else
            /* Set up channel data for PIZ decompression */
            ExrChannelData* piz_channels = (ExrChannelData*)ctx->allocator.alloc(
                ctx->allocator.userdata, part->num_channels * sizeof(ExrChannelData),
                EXR_DEFAULT_ALIGNMENT);
            if (!piz_channels) {
                ctx->allocator.free(ctx->allocator.userdata, compressed, data_size);
                ctx->allocator.free(ctx->allocator.userdata, decompressed, expected_size);
                return EXR_ERROR_OUT_OF_MEMORY;
            }

            for (uint32_t c = 0; c < part->num_channels; c++) {
                piz_channels[c].pixel_type = part->channels[c].pixel_type;
                piz_channels[c].x_sampling = part->channels[c].x_sampling;
                piz_channels[c].y_sampling = part->channels[c].y_sampling;
            }

            result = decompress_piz(compressed, data_size, decompressed,
                                     expected_size, &decompressed_size,
                                     part->num_channels, piz_channels,
                                     part->width, num_lines, ctx);

            ctx->allocator.free(ctx->allocator.userdata, piz_channels,
                               part->num_channels * sizeof(ExrChannelData));

            if (EXR_FAILED(result)) {
                exr_context_add_error(ctx, result,
                                      "PIZ decompression failed", "chunk", offset);
                ctx->allocator.free(ctx->allocator.userdata, compressed, data_size);
                ctx->allocator.free(ctx->allocator.userdata, decompressed, expected_size);
                return result;
            }
#endif
            break;
        }

        case EXR_COMPRESSION_PXR24: {
#if defined(TINYEXR_V3_HAS_PXR24)
            /* Use V2 PXR24 implementation - convert channel info */
            tinyexr::v2::Channel* v2_channels = new tinyexr::v2::Channel[part->num_channels];
            for (uint32_t c = 0; c < part->num_channels; c++) {
                v2_channels[c].name = part->channels[c].name;
                v2_channels[c].pixel_type = (int)part->channels[c].pixel_type;
                v2_channels[c].x_sampling = part->channels[c].x_sampling;
                v2_channels[c].y_sampling = part->channels[c].y_sampling;
                v2_channels[c].p_linear = part->channels[c].p_linear;
            }

            /* Calculate expected PXR24 intermediate size */
            size_t pxr24_size = 0;
            for (uint32_t c = 0; c < part->num_channels; c++) {
                int ch_width = part->width / v2_channels[c].x_sampling;
                int ch_height = num_lines / v2_channels[c].y_sampling;
                int ch_pixels = ch_width * ch_height;
                switch (v2_channels[c].pixel_type) {
                    case 0: pxr24_size += (size_t)ch_pixels * 4; break; /* UINT */
                    case 1: pxr24_size += (size_t)ch_pixels * 2; break; /* HALF */
                    case 2: pxr24_size += (size_t)ch_pixels * 3; break; /* FLOAT */
                }
            }

            /* Decompress zlib using V2 deflate */
            uint8_t* pxr24_buf = (uint8_t*)ctx->allocator.alloc(
                ctx->allocator.userdata, pxr24_size, EXR_DEFAULT_ALIGNMENT);
            if (!pxr24_buf) {
                delete[] v2_channels;
                ctx->allocator.free(ctx->allocator.userdata, compressed, data_size);
                ctx->allocator.free(ctx->allocator.userdata, decompressed, expected_size);
                return EXR_ERROR_OUT_OF_MEMORY;
            }

            bool pxr24_ok = false;
            if (pxr24_size == data_size) {
                /* Uncompressed */
                memcpy(pxr24_buf, compressed, data_size);
                pxr24_ok = true;
            } else {
                /* Decompress with V2 deflate */
                size_t uncomp_size = pxr24_size;
                pxr24_ok = tinyexr::huffman::inflate_zlib(compressed, data_size, pxr24_buf, &uncomp_size);
                if (pxr24_ok && uncomp_size != pxr24_size) {
                    pxr24_ok = false;
                }
            }

            if (pxr24_ok) {
                /* Convert PXR24 to standard EXR format (per-scanline interleaved)
                 * PXR24 uses:
                 * 1. Byte plane separation: bytes stored by plane (high bytes, then low bytes)
                 * 2. Delta encoding: each pixel is difference from previous
                 */
                const uint8_t* in_ptr = pxr24_buf;
                uint8_t* out_ptr = decompressed;

                for (int line = 0; line < num_lines; line++) {
                    for (uint32_t c = 0; c < part->num_channels; c++) {
                        int w = part->width / v2_channels[c].x_sampling;
                        if ((line % v2_channels[c].y_sampling) != 0) continue;

                        switch (v2_channels[c].pixel_type) {
                            case 0: { /* UINT - 4 byte planes with delta encoding */
                                const uint8_t* ptr0 = in_ptr;
                                const uint8_t* ptr1 = in_ptr + w;
                                const uint8_t* ptr2 = in_ptr + w * 2;
                                const uint8_t* ptr3 = in_ptr + w * 3;
                                in_ptr += w * 4;

                                uint32_t pixel = 0;
                                for (int x = 0; x < w; x++) {
                                    uint32_t diff = ((uint32_t)ptr0[x] << 24) |
                                                    ((uint32_t)ptr1[x] << 16) |
                                                    ((uint32_t)ptr2[x] << 8) |
                                                    ((uint32_t)ptr3[x]);
                                    pixel += diff;
                                    memcpy(out_ptr, &pixel, 4);
                                    out_ptr += 4;
                                }
                                break;
                            }
                            case 1: { /* HALF - 2 byte planes with delta encoding */
                                const uint8_t* ptr0 = in_ptr;
                                const uint8_t* ptr1 = in_ptr + w;
                                in_ptr += w * 2;

                                uint32_t pixel = 0;
                                for (int x = 0; x < w; x++) {
                                    uint32_t diff = ((uint32_t)ptr0[x] << 8) |
                                                    ((uint32_t)ptr1[x]);
                                    pixel += diff;
                                    uint16_t h = (uint16_t)pixel;
                                    memcpy(out_ptr, &h, 2);
                                    out_ptr += 2;
                                }
                                break;
                            }
                            case 2: { /* FLOAT - 3 byte planes with delta encoding */
                                const uint8_t* ptr0 = in_ptr;
                                const uint8_t* ptr1 = in_ptr + w;
                                const uint8_t* ptr2 = in_ptr + w * 2;
                                in_ptr += w * 3;

                                uint32_t pixel = 0;
                                for (int x = 0; x < w; x++) {
                                    uint32_t diff = ((uint32_t)ptr0[x] << 24) |
                                                    ((uint32_t)ptr1[x] << 16) |
                                                    ((uint32_t)ptr2[x] << 8);
                                    pixel += diff;
                                    memcpy(out_ptr, &pixel, 4);
                                    out_ptr += 4;
                                }
                                break;
                            }
                        }
                    }
                }
            }

            ctx->allocator.free(ctx->allocator.userdata, pxr24_buf, pxr24_size);
            delete[] v2_channels;

            if (!pxr24_ok) {
                exr_context_add_error(ctx, EXR_ERROR_DECOMPRESSION_FAILED,
                                      "PXR24 decompression failed", "chunk", offset);
                ctx->allocator.free(ctx->allocator.userdata, compressed, data_size);
                ctx->allocator.free(ctx->allocator.userdata, decompressed, expected_size);
                return EXR_ERROR_DECOMPRESSION_FAILED;
            }
            decompressed_size = expected_size;
#else
            exr_context_add_error(ctx, EXR_ERROR_UNSUPPORTED_FORMAT,
                                  "PXR24 compression not supported",
                                  "chunk", offset);
            ctx->allocator.free(ctx->allocator.userdata, compressed, data_size);
            ctx->allocator.free(ctx->allocator.userdata, decompressed, expected_size);
            return EXR_ERROR_UNSUPPORTED_FORMAT;
#endif
            break;
        }

        case EXR_COMPRESSION_B44:
        case EXR_COMPRESSION_B44A: {
#if defined(TINYEXR_V3_HAS_B44)
            /* Use V2 B44 implementation - convert channel info */
            tinyexr::v2::Channel* v2_channels = new tinyexr::v2::Channel[part->num_channels];
            for (uint32_t c = 0; c < part->num_channels; c++) {
                v2_channels[c].name = part->channels[c].name;
                v2_channels[c].pixel_type = (int)part->channels[c].pixel_type;
                v2_channels[c].x_sampling = part->channels[c].x_sampling;
                v2_channels[c].y_sampling = part->channels[c].y_sampling;
                v2_channels[c].p_linear = part->channels[c].p_linear;
            }

            bool is_b44a = (part->compression == EXR_COMPRESSION_B44A);
            tinyexr::v2::ScratchPool pool;

            /* DecompressB44V2 outputs per-scanline interleaved layout directly */
            bool b44_ok = tinyexr::v2::DecompressB44V2(
                decompressed, expected_size,
                compressed, data_size,
                part->width, num_lines,
                part->num_channels, v2_channels, is_b44a, pool);

            delete[] v2_channels;

            if (!b44_ok) {
                exr_context_add_error(ctx, EXR_ERROR_DECOMPRESSION_FAILED,
                                      "B44 decompression failed", "chunk", offset);
                ctx->allocator.free(ctx->allocator.userdata, compressed, data_size);
                ctx->allocator.free(ctx->allocator.userdata, decompressed, expected_size);
                return EXR_ERROR_DECOMPRESSION_FAILED;
            }
            decompressed_size = expected_size;
#else
            exr_context_add_error(ctx, EXR_ERROR_UNSUPPORTED_FORMAT,
                                  "B44 compression not supported",
                                  "chunk", offset);
            ctx->allocator.free(ctx->allocator.userdata, compressed, data_size);
            ctx->allocator.free(ctx->allocator.userdata, decompressed, expected_size);
            return EXR_ERROR_UNSUPPORTED_FORMAT;
#endif
            break;
        }

        default:
            ctx->allocator.free(ctx->allocator.userdata, compressed, data_size);
            ctx->allocator.free(ctx->allocator.userdata, decompressed, expected_size);
            return EXR_ERROR_UNSUPPORTED_FORMAT;
    }

    ctx->allocator.free(ctx->allocator.userdata, compressed, data_size);

    *out_data = decompressed;
    *out_size = decompressed_size;
    *out_y_start = y_start;
    *out_num_lines = num_lines;

    return EXR_SUCCESS;
}

/* Execute a scanline read command */
static ExrResult execute_scanline_read(ExrDecoder decoder, ExrScanlineReadCmd* cmd) {
    ExrContext ctx = decoder->ctx;
    ExrImage image = decoder->image;

    if (!image || cmd->base.part_index >= image->num_parts) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    ExrPartData* part = &image->parts[cmd->base.part_index];
    int lines_per_block = get_lines_per_block(part->compression);

    /* Calculate which chunks we need to read */
    int start_chunk = cmd->y_start / lines_per_block;
    int end_y = cmd->y_start + cmd->num_lines;
    int end_chunk = (end_y + lines_per_block - 1) / lines_per_block;

    /* Calculate output stride */
    size_t bytes_per_pixel_out = get_bytes_per_pixel(cmd->output_pixel_type);
    size_t output_stride = (size_t)part->width * part->num_channels * bytes_per_pixel_out;

    uint8_t* output = (uint8_t*)cmd->output;
    int lines_written = 0;

    /* Read each chunk */
    for (int chunk = start_chunk; chunk < end_chunk && chunk < (int)part->num_chunks; chunk++) {
        uint8_t* chunk_data = NULL;
        size_t chunk_size;
        int chunk_y_start, chunk_num_lines;

        ExrResult result = read_chunk(decoder, part, (uint32_t)chunk,
                                       &chunk_data, &chunk_size,
                                       &chunk_y_start, &chunk_num_lines);
        if (EXR_FAILED(result)) {
            return result;
        }

        /* Calculate overlap with requested region */
        int copy_start = (chunk_y_start > cmd->y_start) ? chunk_y_start : cmd->y_start;
        int copy_end = (chunk_y_start + chunk_num_lines < end_y) ?
                       (chunk_y_start + chunk_num_lines) : end_y;
        int copy_lines = copy_end - copy_start;

        if (copy_lines > 0) {
            /* Calculate source offset within chunk */
            int src_y_offset = copy_start - chunk_y_start;
            size_t bytes_per_line = 0;
            for (uint32_t c = 0; c < part->num_channels; c++) {
                bytes_per_line += (size_t)part->width *
                                  get_bytes_per_pixel(part->channels[c].pixel_type);
            }
            size_t src_offset = src_y_offset * bytes_per_line;

            /* Copy data - simple copy for now (TODO: pixel type conversion) */
            size_t copy_size = copy_lines * bytes_per_line;
            if ((lines_written * output_stride + copy_size) <= cmd->output_size) {
                memcpy(output + lines_written * output_stride,
                       chunk_data + src_offset, copy_size);
            }

            lines_written += copy_lines;
        }

        ctx->allocator.free(ctx->allocator.userdata, chunk_data, chunk_size);
    }

    return EXR_SUCCESS;
}

/* Execute a full image read command */
static ExrResult execute_full_image_read(ExrDecoder decoder, ExrFullImageReadCmd* cmd) {
    ExrContext ctx = decoder->ctx;
    ExrImage image = decoder->image;

    if (!image || cmd->base.part_index >= image->num_parts) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    ExrPartData* part = &image->parts[cmd->base.part_index];

    /* For scanline images, read all chunks sequentially */
    if (part->part_type == EXR_PART_SCANLINE) {
        ExrScanlineReadCmd scan_cmd;
        scan_cmd.base.type = EXR_CMD_TYPE_READ_SCANLINES;
        scan_cmd.base.part_index = cmd->base.part_index;
        scan_cmd.y_start = 0;
        scan_cmd.num_lines = part->height;
        scan_cmd.output = cmd->output;
        scan_cmd.output_size = cmd->output_size;
        scan_cmd.channels_mask = cmd->channels_mask;
        scan_cmd.output_pixel_type = cmd->output_pixel_type;
        scan_cmd.output_layout = cmd->output_layout;

        return execute_scanline_read(decoder, &scan_cmd);
    }

    /* For tiled images - read all tiles */
    if (part->part_type == EXR_PART_TILED) {
        /* TODO: Implement tiled reading */
        exr_context_add_error(ctx, EXR_ERROR_UNSUPPORTED_FORMAT,
                              "Tiled image reading not yet implemented", NULL, 0);
        return EXR_ERROR_UNSUPPORTED_FORMAT;
    }

    return EXR_ERROR_UNSUPPORTED_FORMAT;
}

/* Execute all commands in a command buffer */
static ExrResult execute_commands(ExrDecoder decoder, ExrCommandBuffer cmd) {
    ExrResult result = EXR_SUCCESS;

    for (uint32_t i = 0; i < cmd->command_count; i++) {
        ExrCommandUnion* command = &cmd->commands[i];

        switch (command->base.type) {
            case EXR_CMD_TYPE_READ_TILE:
                /* TODO: Implement tile reading */
                result = EXR_ERROR_UNSUPPORTED_FORMAT;
                break;

            case EXR_CMD_TYPE_READ_SCANLINES:
                result = execute_scanline_read(decoder, &command->scanline_read);
                break;

            case EXR_CMD_TYPE_READ_FULL_IMAGE:
                result = execute_full_image_read(decoder, &command->full_image_read);
                break;

            default:
                result = EXR_ERROR_INVALID_ARGUMENT;
                break;
        }

        if (EXR_FAILED(result)) {
            return result;
        }
    }

    return EXR_SUCCESS;
}

/* ============================================================================
 * Submit Function
 * ============================================================================ */

ExrResult exr_submit(ExrDecoder decoder, const ExrSubmitInfo* submit_info) {
    if (!exr_decoder_is_valid(decoder)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!submit_info) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }
    if (decoder->state != EXR_DECODER_STATE_HEADER_PARSED) {
        return EXR_ERROR_INVALID_STATE;
    }

    ExrResult result = EXR_SUCCESS;

    /* Execute all command buffers */
    for (uint32_t i = 0; i < submit_info->command_buffer_count; i++) {
        ExrCommandBuffer cmd = submit_info->command_buffers[i];
        if (!exr_command_buffer_is_valid(cmd)) {
            return EXR_ERROR_INVALID_HANDLE;
        }
        if (cmd->recording) {
            return EXR_ERROR_INVALID_STATE;  /* Can't submit recording buffer */
        }

        result = execute_commands(decoder, cmd);
        if (EXR_FAILED(result)) {
            break;
        }
    }

    /* Signal fence if provided */
    if (submit_info->signal_fence) {
        if (EXR_FAILED(result)) {
            /* Don't signal on failure */
        } else {
            exr_fence_signal(submit_info->signal_fence);
        }
    }

    return result;
}

/* ============================================================================
 * SIMD Information
 * ============================================================================ */

uint32_t exr_get_simd_capabilities(void) {
    uint32_t caps = EXR_SIMD_NONE;

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
    /* x86/x64 CPU feature detection */
#if defined(_MSC_VER)
    int cpuInfo[4];
    __cpuid(cpuInfo, 1);
    if (cpuInfo[3] & (1 << 26)) caps |= EXR_SIMD_SSE2;
    if (cpuInfo[2] & (1 << 19)) caps |= EXR_SIMD_SSE4_1;
    if (cpuInfo[2] & (1 << 28)) caps |= EXR_SIMD_AVX;

    __cpuidex(cpuInfo, 7, 0);
    if (cpuInfo[1] & (1 << 5)) caps |= EXR_SIMD_AVX2;
    if (cpuInfo[1] & (1 << 3)) caps |= EXR_SIMD_BMI1;
    if (cpuInfo[1] & (1 << 8)) caps |= EXR_SIMD_BMI2;
    if (cpuInfo[1] & (1 << 16)) caps |= EXR_SIMD_AVX512;
#elif defined(__GNUC__) || defined(__clang__)
    uint32_t eax, ebx, ecx, edx;
    if (__get_cpuid(1, &eax, &ebx, &ecx, &edx)) {
        if (edx & (1 << 26)) caps |= EXR_SIMD_SSE2;
        if (ecx & (1 << 19)) caps |= EXR_SIMD_SSE4_1;
        if (ecx & (1 << 28)) caps |= EXR_SIMD_AVX;
    }
    if (__get_cpuid_count(7, 0, &eax, &ebx, &ecx, &edx)) {
        if (ebx & (1 << 5)) caps |= EXR_SIMD_AVX2;
        if (ebx & (1 << 3)) caps |= EXR_SIMD_BMI1;
        if (ebx & (1 << 8)) caps |= EXR_SIMD_BMI2;
        if (ebx & (1 << 16)) caps |= EXR_SIMD_AVX512;
    }
#endif
#elif defined(__aarch64__) || defined(_M_ARM64) || defined(__arm__) || defined(_M_ARM)
    /* ARM always has NEON on 64-bit and usually on 32-bit */
    caps |= EXR_SIMD_NEON;
#endif

    return caps;
}

const char* exr_get_simd_info(void) {
    static char info[128];
    static int initialized = 0;

    if (!initialized) {
        uint32_t caps = exr_get_simd_capabilities();
        char* p = info;
        int first = 1;

        if (caps == EXR_SIMD_NONE) {
            strcpy(info, "Scalar");
        } else {
            info[0] = '\0';
            if (caps & EXR_SIMD_AVX512) { p += sprintf(p, "%sAVX512", first ? "" : "+"); first = 0; }
            else if (caps & EXR_SIMD_AVX2) { p += sprintf(p, "%sAVX2", first ? "" : "+"); first = 0; }
            else if (caps & EXR_SIMD_AVX) { p += sprintf(p, "%sAVX", first ? "" : "+"); first = 0; }
            else if (caps & EXR_SIMD_SSE4_1) { p += sprintf(p, "%sSSE4.1", first ? "" : "+"); first = 0; }
            else if (caps & EXR_SIMD_SSE2) { p += sprintf(p, "%sSSE2", first ? "" : "+"); first = 0; }
            if (caps & EXR_SIMD_NEON) { p += sprintf(p, "%sNEON", first ? "" : "+"); first = 0; }
            if (caps & EXR_SIMD_BMI2) { p += sprintf(p, "%sBMI2", first ? "" : "+"); first = 0; }
            else if (caps & EXR_SIMD_BMI1) { p += sprintf(p, "%sBMI1", first ? "" : "+"); first = 0; }
            (void)p;  /* Suppress unused warning */
        }
        initialized = 1;
    }

    return info;
}

/* ============================================================================
 * Pixel Conversion Utilities (Stub - to be implemented with SIMD)
 * ============================================================================ */

/* Half-float conversion tables */
static uint32_t g_mantissa_table[2048];
static uint32_t g_exponent_table[64];
static uint16_t g_offset_table[64];
static uint16_t g_base_table[512];
static uint8_t g_shift_table[512];
static int g_tables_initialized = 0;

static void init_half_tables(void) {
    if (g_tables_initialized) return;

    /* Initialize mantissa table */
    g_mantissa_table[0] = 0;
    for (int i = 1; i < 1024; i++) {
        uint32_t m = i << 13;
        uint32_t e = 0;
        while ((m & 0x00800000) == 0) {
            e -= 0x00800000;
            m <<= 1;
        }
        m &= ~0x00800000;
        e += 0x38800000;
        g_mantissa_table[i] = m | e;
    }
    for (int i = 1024; i < 2048; i++) {
        g_mantissa_table[i] = 0x38000000 + ((i - 1024) << 13);
    }

    /* Initialize exponent table */
    g_exponent_table[0] = 0;
    for (int i = 1; i < 31; i++) {
        g_exponent_table[i] = (uint32_t)(i) << 23;
    }
    g_exponent_table[31] = 0x47800000;
    g_exponent_table[32] = 0x80000000;
    for (int i = 33; i < 63; i++) {
        g_exponent_table[i] = 0x80000000 | ((uint32_t)(i - 32) << 23);
    }
    g_exponent_table[63] = 0xC7800000;

    /* Initialize offset table */
    for (int i = 0; i < 64; i++) {
        g_offset_table[i] = 1024;
    }
    g_offset_table[0] = 0;
    g_offset_table[32] = 0;

    /* Initialize base/shift tables for float to half */
    for (int i = 0; i < 256; i++) {
        int e = i - 127;
        if (e < -24) {
            g_base_table[i] = 0x0000;
            g_base_table[i | 0x100] = 0x8000;
            g_shift_table[i] = 24;
            g_shift_table[i | 0x100] = 24;
        } else if (e < -14) {
            g_base_table[i] = (0x0400 >> (-e - 14));
            g_base_table[i | 0x100] = (0x0400 >> (-e - 14)) | 0x8000;
            g_shift_table[i] = -e - 1;
            g_shift_table[i | 0x100] = -e - 1;
        } else if (e <= 15) {
            g_base_table[i] = (uint16_t)((e + 15) << 10);
            g_base_table[i | 0x100] = (uint16_t)(((e + 15) << 10) | 0x8000);
            g_shift_table[i] = 13;
            g_shift_table[i | 0x100] = 13;
        } else if (e < 128) {
            g_base_table[i] = 0x7C00;
            g_base_table[i | 0x100] = 0xFC00;
            g_shift_table[i] = 24;
            g_shift_table[i | 0x100] = 24;
        } else {
            g_base_table[i] = 0x7C00;
            g_base_table[i | 0x100] = 0xFC00;
            g_shift_table[i] = 13;
            g_shift_table[i | 0x100] = 13;
        }
    }

    g_tables_initialized = 1;
}

void exr_convert_half_to_float(const uint16_t* src, float* dst, size_t count) {
    init_half_tables();

    for (size_t i = 0; i < count; i++) {
        uint16_t h = src[i];
        uint32_t f = g_mantissa_table[g_offset_table[h >> 10] + (h & 0x3FF)] +
                     g_exponent_table[h >> 10];
        memcpy(&dst[i], &f, sizeof(float));
    }
}

void exr_convert_float_to_half(const float* src, uint16_t* dst, size_t count) {
    init_half_tables();

    for (size_t i = 0; i < count; i++) {
        uint32_t f;
        memcpy(&f, &src[i], sizeof(float));
        uint32_t sign = (f >> 16) & 0x8000;  /* Sign bit to half position */
        uint32_t exp = (f >> 23) & 0xFF;     /* 8-bit exponent */
        uint32_t sign_idx = (f >> 23) & 0x100; /* Sign bit for table index */
        dst[i] = (uint16_t)(g_base_table[exp | sign_idx] +
                            ((f & 0x007FFFFF) >> g_shift_table[exp | sign_idx]));
        dst[i] |= (uint16_t)sign;
    }
}

void exr_interleave_rgba(const float* r, const float* g, const float* b,
                          const float* a, float* rgba, size_t pixel_count) {
    for (size_t i = 0; i < pixel_count; i++) {
        rgba[i * 4 + 0] = r ? r[i] : 0.0f;
        rgba[i * 4 + 1] = g ? g[i] : 0.0f;
        rgba[i * 4 + 2] = b ? b[i] : 0.0f;
        rgba[i * 4 + 3] = a ? a[i] : 1.0f;
    }
}

void exr_deinterleave_rgba(const float* rgba, float* r, float* g, float* b,
                            float* a, size_t pixel_count) {
    for (size_t i = 0; i < pixel_count; i++) {
        if (r) r[i] = rgba[i * 4 + 0];
        if (g) g[i] = rgba[i * 4 + 1];
        if (b) b[i] = rgba[i * 4 + 2];
        if (a) a[i] = rgba[i * 4 + 3];
    }
}

/* ============================================================================
 * Header Parsing Constants
 * ============================================================================ */

/* EXR magic number: 0x76, 0x2f, 0x31, 0x01 */
static const uint8_t EXR_MAGIC[4] = { 0x76, 0x2f, 0x31, 0x01 };
static const size_t EXR_VERSION_SIZE = 8;
static const size_t EXR_MAX_ATTRIBUTE_NAME = 256;
static const size_t EXR_MAX_ATTRIBUTES = 128;

/* Parsing sub-states */
typedef enum ExrParseSubState {
    PARSE_STATE_VERSION = 0,
    PARSE_STATE_HEADER_ATTRS,
    PARSE_STATE_OFFSET_TABLE,
    PARSE_STATE_DONE,
} ExrParseSubState;

/* ============================================================================
 * Helper Functions for Parsing
 * ============================================================================ */

/* Read little-endian int32 */
static int32_t read_le_i32(const uint8_t* p) {
    return (int32_t)((uint32_t)p[0] | ((uint32_t)p[1] << 8) |
                     ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24));
}

/* Read little-endian uint32 */
static uint32_t read_le_u32(const uint8_t* p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
           ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

/* Read little-endian uint64 */
static uint64_t read_le_u64(const uint8_t* p) {
    return (uint64_t)p[0] | ((uint64_t)p[1] << 8) |
           ((uint64_t)p[2] << 16) | ((uint64_t)p[3] << 24) |
           ((uint64_t)p[4] << 32) | ((uint64_t)p[5] << 40) |
           ((uint64_t)p[6] << 48) | ((uint64_t)p[7] << 56);
}

/* Read little-endian float */
static float read_le_f32(const uint8_t* p) {
    uint32_t u = read_le_u32(p);
    float f;
    memcpy(&f, &u, sizeof(float));
    return f;
}

/* Read null-terminated string (up to max_len chars, returns length including null) */
static size_t read_string(const uint8_t* data, size_t size, char* out, size_t max_len) {
    size_t i;
    for (i = 0; i < size && i < max_len - 1; i++) {
        out[i] = (char)data[i];
        if (data[i] == 0) {
            return i + 1;  /* Include null terminator in count */
        }
    }
    out[max_len - 1] = '\0';
    return 0;  /* Not found or truncated */
}

/* Synchronous fetch helper - fetches data synchronously from the data source */
static ExrResult sync_fetch(ExrDecoder decoder, uint64_t offset, uint64_t size, void* dst) {
    ExrDataSource* src = &decoder->source;
    return src->fetch(src->userdata, offset, size, dst, NULL, NULL);
}

/* Ensure read buffer has at least 'size' bytes capacity */
static ExrResult ensure_read_buffer(ExrDecoder decoder, size_t size) {
    if (decoder->read_buffer_size >= size) {
        return EXR_SUCCESS;
    }

    ExrContext ctx = decoder->ctx;

    /* Free old buffer */
    if (decoder->read_buffer) {
        ctx->allocator.free(ctx->allocator.userdata,
                            decoder->read_buffer, decoder->read_buffer_size);
    }

    /* Allocate new buffer */
    decoder->read_buffer = (uint8_t*)ctx->allocator.alloc(
        ctx->allocator.userdata, size, EXR_DEFAULT_ALIGNMENT);
    if (!decoder->read_buffer) {
        decoder->read_buffer_size = 0;
        return EXR_ERROR_OUT_OF_MEMORY;
    }
    decoder->read_buffer_size = size;
    return EXR_SUCCESS;
}

/* ============================================================================
 * Version Parsing
 * ============================================================================ */

static ExrResult parse_exr_version(ExrDecoder decoder, ExrImage image) {
    ExrResult result;
    uint8_t version_buf[8];

    /* SUSPEND POINT 1: Read version header (8 bytes) */
    result = sync_fetch(decoder, 0, EXR_VERSION_SIZE, version_buf);
    if (EXR_FAILED(result)) {
        exr_context_add_error(decoder->ctx, result,
                              "Failed to read EXR version header", "version", 0);
        return result;
    }

    /* Verify magic number */
    if (version_buf[0] != EXR_MAGIC[0] || version_buf[1] != EXR_MAGIC[1] ||
        version_buf[2] != EXR_MAGIC[2] || version_buf[3] != EXR_MAGIC[3]) {
        exr_context_add_error(decoder->ctx, EXR_ERROR_INVALID_MAGIC,
                              "Invalid EXR magic number", "version", 0);
        return EXR_ERROR_INVALID_MAGIC;
    }

    /* Parse version byte (should be 2) */
    if (version_buf[4] != 2) {
        exr_context_add_error_fmt(decoder->ctx, EXR_ERROR_INVALID_VERSION,
                                  "version", 4, "Invalid EXR version: %d (expected 2)",
                                  version_buf[4]);
        return EXR_ERROR_INVALID_VERSION;
    }
    image->version = 2;

    /* Parse flags byte */
    uint8_t flags = version_buf[5];
    if (flags & 0x02) image->flags |= EXR_IMAGE_TILED;
    if (flags & 0x04) image->flags |= EXR_IMAGE_LONG_NAMES;
    if (flags & 0x08) image->flags |= EXR_IMAGE_DEEP;
    if (flags & 0x10) image->flags |= EXR_IMAGE_MULTIPART;

    decoder->current_offset = EXR_VERSION_SIZE;
    return EXR_SUCCESS;
}

/* ============================================================================
 * Attribute Parsing
 * ============================================================================ */

/* Map attribute type string to enum */
static ExrAttributeType parse_attribute_type(const char* type_name) {
    if (strcmp(type_name, "int") == 0) return EXR_ATTR_INT;
    if (strcmp(type_name, "float") == 0) return EXR_ATTR_FLOAT;
    if (strcmp(type_name, "double") == 0) return EXR_ATTR_DOUBLE;
    if (strcmp(type_name, "string") == 0) return EXR_ATTR_STRING;
    if (strcmp(type_name, "box2i") == 0) return EXR_ATTR_BOX2I;
    if (strcmp(type_name, "box2f") == 0) return EXR_ATTR_BOX2F;
    if (strcmp(type_name, "v2i") == 0) return EXR_ATTR_V2I;
    if (strcmp(type_name, "v2f") == 0) return EXR_ATTR_V2F;
    if (strcmp(type_name, "v3i") == 0) return EXR_ATTR_V3I;
    if (strcmp(type_name, "v3f") == 0) return EXR_ATTR_V3F;
    if (strcmp(type_name, "m33f") == 0) return EXR_ATTR_M33F;
    if (strcmp(type_name, "m44f") == 0) return EXR_ATTR_M44F;
    if (strcmp(type_name, "chlist") == 0) return EXR_ATTR_CHLIST;
    if (strcmp(type_name, "compression") == 0) return EXR_ATTR_COMPRESSION;
    if (strcmp(type_name, "lineOrder") == 0) return EXR_ATTR_LINEORDER;
    if (strcmp(type_name, "tiledesc") == 0) return EXR_ATTR_TILEDESC;
    if (strcmp(type_name, "preview") == 0) return EXR_ATTR_PREVIEW;
    if (strcmp(type_name, "rational") == 0) return EXR_ATTR_RATIONAL;
    if (strcmp(type_name, "keycode") == 0) return EXR_ATTR_KEYCODE;
    if (strcmp(type_name, "timecode") == 0) return EXR_ATTR_TIMECODE;
    if (strcmp(type_name, "chromaticities") == 0) return EXR_ATTR_CHROMATICITIES;
    if (strcmp(type_name, "envmap") == 0) return EXR_ATTR_ENVMAP;
    if (strcmp(type_name, "deepImageState") == 0) return EXR_ATTR_DEEPIMAGETYPE;
    return EXR_ATTR_OPAQUE;
}

/* Parse channel list attribute */
static ExrResult parse_channel_list(ExrPartData* part, const uint8_t* data, uint32_t size,
                                    ExrContext ctx) {
    /* Channel format:
     *   name: null-terminated string (1-255 bytes)
     *   pixel_type: int32 (0=UINT, 1=HALF, 2=FLOAT)
     *   pLinear: uint8
     *   reserved: 3 bytes (zeros)
     *   xSampling: int32
     *   ySampling: int32
     *   Repeat until empty name (single null byte)
     */

    /* First pass: count channels */
    uint32_t num_channels = 0;
    const uint8_t* p = data;
    const uint8_t* end = data + size;

    while (p < end && *p != 0) {
        /* Skip name */
        while (p < end && *p != 0) p++;
        if (p >= end) break;
        p++;  /* Skip null terminator */

        /* Skip channel data (16 bytes: pixel_type + pLinear + reserved + xSampling + ySampling) */
        if (p + 16 > end) break;
        p += 16;
        num_channels++;
    }

    if (num_channels == 0) {
        exr_context_add_error(ctx, EXR_ERROR_INVALID_DATA,
                              "No channels found in channel list", "channels", 0);
        return EXR_ERROR_INVALID_DATA;
    }

    /* Allocate channel array */
    part->channels = (ExrChannelData*)ctx->allocator.alloc(
        ctx->allocator.userdata, num_channels * sizeof(ExrChannelData), EXR_DEFAULT_ALIGNMENT);
    if (!part->channels) {
        return EXR_ERROR_OUT_OF_MEMORY;
    }
    memset(part->channels, 0, num_channels * sizeof(ExrChannelData));
    part->num_channels = num_channels;

    /* Second pass: parse channels */
    p = data;
    for (uint32_t i = 0; i < num_channels && p < end && *p != 0; i++) {
        ExrChannelData* ch = &part->channels[i];

        /* Parse name */
        size_t name_len = 0;
        while (p + name_len < end && p[name_len] != 0 && name_len < sizeof(ch->name) - 1) {
            ch->name[name_len] = (char)p[name_len];
            name_len++;
        }
        ch->name[name_len] = '\0';
        p += name_len + 1;  /* Skip name + null */

        if (p + 16 > end) break;

        /* Parse pixel type */
        int32_t pixel_type = read_le_i32(p);
        if (pixel_type < 0 || pixel_type > 2) {
            exr_context_add_error_fmt(ctx, EXR_ERROR_INVALID_DATA, "channels", 0,
                                      "Invalid pixel type %d for channel %s",
                                      pixel_type, ch->name);
            return EXR_ERROR_INVALID_DATA;
        }
        ch->pixel_type = (uint32_t)pixel_type;
        p += 4;

        /* Parse pLinear */
        ch->p_linear = *p;
        p += 4;  /* pLinear + 3 reserved bytes */

        /* Parse sampling */
        ch->x_sampling = read_le_i32(p);
        p += 4;
        ch->y_sampling = read_le_i32(p);
        p += 4;
    }

    return EXR_SUCCESS;
}

/* Parse a single attribute */
static ExrResult parse_attribute(ExrDecoder decoder, ExrPartData* part,
                                 uint64_t* offset, int* end_of_header) {
    ExrResult result;
    ExrContext ctx = decoder->ctx;
    uint8_t buf[512];

    *end_of_header = 0;

    /* SUSPEND POINT: Read attribute name */
    /* Read first byte to check for end of header */
    result = sync_fetch(decoder, *offset, 1, buf);
    if (EXR_FAILED(result)) return result;

    if (buf[0] == 0) {
        *end_of_header = 1;
        (*offset)++;
        return EXR_SUCCESS;
    }

    /* Read enough for name + type + size (max ~512 bytes for names) */
    result = sync_fetch(decoder, *offset, sizeof(buf), buf);
    if (EXR_FAILED(result)) return result;

    /* Parse attribute name */
    char attr_name[256];
    size_t name_len = read_string(buf, sizeof(buf), attr_name, sizeof(attr_name));
    if (name_len == 0) {
        exr_context_add_error(ctx, EXR_ERROR_INVALID_DATA,
                              "Attribute name too long or missing null terminator",
                              "header", *offset);
        return EXR_ERROR_INVALID_DATA;
    }

    /* Parse attribute type */
    char attr_type[64];
    size_t type_len = read_string(buf + name_len, sizeof(buf) - name_len, attr_type, sizeof(attr_type));
    if (type_len == 0) {
        exr_context_add_error(ctx, EXR_ERROR_INVALID_DATA,
                              "Attribute type too long or missing null terminator",
                              "header", *offset);
        return EXR_ERROR_INVALID_DATA;
    }

    /* Parse attribute size */
    size_t header_size = name_len + type_len;
    if (header_size + 4 > sizeof(buf)) {
        exr_context_add_error(ctx, EXR_ERROR_INVALID_DATA,
                              "Attribute header too large", "header", *offset);
        return EXR_ERROR_INVALID_DATA;
    }

    uint32_t attr_size = read_le_u32(buf + header_size);
    header_size += 4;

    /* Validate size */
    if (attr_size > 16 * 1024 * 1024) {  /* 16MB max for single attribute */
        exr_context_add_error_fmt(ctx, EXR_ERROR_INVALID_DATA, "header", *offset,
                                  "Attribute %s size too large: %u", attr_name, attr_size);
        return EXR_ERROR_INVALID_DATA;
    }

    /* Read attribute value */
    uint8_t* attr_data = buf + header_size;
    if (header_size + attr_size > sizeof(buf)) {
        /* Need to allocate larger buffer */
        result = ensure_read_buffer(decoder, attr_size);
        if (EXR_FAILED(result)) return result;

        result = sync_fetch(decoder, *offset + header_size, attr_size, decoder->read_buffer);
        if (EXR_FAILED(result)) return result;
        attr_data = decoder->read_buffer;
    }

    /* Process known attributes */
    ExrImage image = part->channels ? NULL : decoder->image;  /* Use image for first part */
    (void)image;

    if (strcmp(attr_name, "channels") == 0) {
        result = parse_channel_list(part, attr_data, attr_size, ctx);
        if (EXR_FAILED(result)) return result;
    }
    else if (strcmp(attr_name, "compression") == 0 && attr_size >= 1) {
        part->compression = attr_data[0];
    }
    else if (strcmp(attr_name, "dataWindow") == 0 && attr_size >= 16) {
        if (decoder->image) {
            decoder->image->data_window.min_x = read_le_i32(attr_data);
            decoder->image->data_window.min_y = read_le_i32(attr_data + 4);
            decoder->image->data_window.max_x = read_le_i32(attr_data + 8);
            decoder->image->data_window.max_y = read_le_i32(attr_data + 12);
        }
        part->width = read_le_i32(attr_data + 8) - read_le_i32(attr_data) + 1;
        part->height = read_le_i32(attr_data + 12) - read_le_i32(attr_data + 4) + 1;
    }
    else if (strcmp(attr_name, "displayWindow") == 0 && attr_size >= 16) {
        if (decoder->image) {
            decoder->image->display_window.min_x = read_le_i32(attr_data);
            decoder->image->display_window.min_y = read_le_i32(attr_data + 4);
            decoder->image->display_window.max_x = read_le_i32(attr_data + 8);
            decoder->image->display_window.max_y = read_le_i32(attr_data + 12);
        }
    }
    else if (strcmp(attr_name, "lineOrder") == 0 && attr_size >= 1) {
        /* Line order: 0=increasing Y, 1=decreasing Y, 2=random */
        /* Stored in flags for now */
    }
    else if (strcmp(attr_name, "pixelAspectRatio") == 0 && attr_size >= 4) {
        if (decoder->image) {
            decoder->image->pixel_aspect_ratio = read_le_f32(attr_data);
        }
    }
    else if (strcmp(attr_name, "screenWindowCenter") == 0 && attr_size >= 8) {
        if (decoder->image) {
            decoder->image->screen_window_center.x = read_le_f32(attr_data);
            decoder->image->screen_window_center.y = read_le_f32(attr_data + 4);
        }
    }
    else if (strcmp(attr_name, "screenWindowWidth") == 0 && attr_size >= 4) {
        if (decoder->image) {
            decoder->image->screen_window_width = read_le_f32(attr_data);
        }
    }
    else if (strcmp(attr_name, "tiles") == 0 && attr_size >= 9) {
        part->tile_size_x = read_le_u32(attr_data);
        part->tile_size_y = read_le_u32(attr_data + 4);
        uint8_t tile_mode = attr_data[8];
        part->tile_level_mode = tile_mode & 0x3;
        part->tile_rounding_mode = (tile_mode >> 4) & 0x1;
        part->flags |= EXR_IMAGE_TILED;
    }
    else if (strcmp(attr_name, "chunkCount") == 0 && attr_size >= 4) {
        part->num_chunks = read_le_u32(attr_data);
    }
    else if (strcmp(attr_name, "name") == 0 && attr_size > 0) {
        /* Part name */
        size_t len = attr_size;
        if (attr_data[len - 1] == 0) len--;  /* Exclude null if present */
        part->name = (char*)ctx->allocator.alloc(ctx->allocator.userdata, len + 1, 1);
        if (part->name) {
            memcpy(part->name, attr_data, len);
            part->name[len] = '\0';
        }
    }
    else if (strcmp(attr_name, "type") == 0 && attr_size > 0) {
        /* Part type */
        size_t len = attr_size;
        if (attr_data[len - 1] == 0) len--;
        part->type_string = (char*)ctx->allocator.alloc(ctx->allocator.userdata, len + 1, 1);
        if (part->type_string) {
            memcpy(part->type_string, attr_data, len);
            part->type_string[len] = '\0';
            /* Parse part type */
            if (strcmp(part->type_string, "scanlineimage") == 0) {
                part->part_type = EXR_PART_SCANLINE;
            } else if (strcmp(part->type_string, "tiledimage") == 0) {
                part->part_type = EXR_PART_TILED;
            } else if (strcmp(part->type_string, "deepscanline") == 0) {
                part->part_type = EXR_PART_DEEP_SCANLINE;
            } else if (strcmp(part->type_string, "deeptile") == 0) {
                part->part_type = EXR_PART_DEEP_TILED;
            }
        }
    }
    else {
        /* Store as custom attribute */
        if (part->num_attributes < EXR_MAX_ATTRIBUTES) {
            /* Allocate or reallocate attribute array */
            size_t new_count = part->num_attributes + 1;
            ExrAttributeData* new_attrs = (ExrAttributeData*)ctx->allocator.alloc(
                ctx->allocator.userdata, new_count * sizeof(ExrAttributeData), EXR_DEFAULT_ALIGNMENT);
            if (new_attrs) {
                if (part->attributes) {
                    memcpy(new_attrs, part->attributes,
                           part->num_attributes * sizeof(ExrAttributeData));
                    ctx->allocator.free(ctx->allocator.userdata, part->attributes,
                                        part->num_attributes * sizeof(ExrAttributeData));
                }
                part->attributes = new_attrs;

                ExrAttributeData* attr = &part->attributes[part->num_attributes];
                memset(attr, 0, sizeof(ExrAttributeData));
                strncpy(attr->name, attr_name, sizeof(attr->name) - 1);
                strncpy(attr->type_name, attr_type, sizeof(attr->type_name) - 1);
                attr->type = parse_attribute_type(attr_type);
                attr->size = attr_size;
                if (attr_size > 0) {
                    attr->value = (uint8_t*)ctx->allocator.alloc(
                        ctx->allocator.userdata, attr_size, 1);
                    if (attr->value) {
                        memcpy(attr->value, attr_data, attr_size);
                    }
                }
                part->num_attributes++;
            }
        }
    }

    *offset += header_size + attr_size;
    return EXR_SUCCESS;
}

/* ============================================================================
 * Offset Table Parsing
 * ============================================================================ */

/* Calculate number of chunks for scanline images */
static uint32_t calc_scanline_chunks(ExrPartData* part) {
    int height = part->height;
    int lines_per_block;

    switch (part->compression) {
        case EXR_COMPRESSION_NONE:
        case EXR_COMPRESSION_RLE:
        case EXR_COMPRESSION_ZIPS:
            lines_per_block = 1;
            break;
        case EXR_COMPRESSION_ZIP:
        case EXR_COMPRESSION_PXR24:
            lines_per_block = 16;
            break;
        case EXR_COMPRESSION_PIZ:
        case EXR_COMPRESSION_B44:
        case EXR_COMPRESSION_B44A:
            lines_per_block = 32;
            break;
        case EXR_COMPRESSION_DWAA:
            lines_per_block = 32;
            break;
        case EXR_COMPRESSION_DWAB:
            lines_per_block = 256;
            break;
        default:
            lines_per_block = 1;
            break;
    }

    return (height + lines_per_block - 1) / lines_per_block;
}

/* Calculate number of chunks for tiled images */
static uint32_t calc_tiled_chunks(ExrPartData* part) {
    if (part->tile_size_x == 0 || part->tile_size_y == 0) {
        return 0;
    }

    uint32_t num_x_tiles = (part->width + part->tile_size_x - 1) / part->tile_size_x;
    uint32_t num_y_tiles = (part->height + part->tile_size_y - 1) / part->tile_size_y;

    part->num_x_levels = 1;
    part->num_y_levels = 1;

    if (part->tile_level_mode == EXR_TILE_ONE_LEVEL) {
        return num_x_tiles * num_y_tiles;
    }

    /* Mipmap/Ripmap levels - TODO: proper calculation */
    uint32_t total = 0;
    int w = part->width;
    int h = part->height;
    while (w >= 1 && h >= 1) {
        uint32_t nx = (w + (int)part->tile_size_x - 1) / (int)part->tile_size_x;
        uint32_t ny = (h + (int)part->tile_size_y - 1) / (int)part->tile_size_y;
        total += nx * ny;
        if (part->tile_level_mode == EXR_TILE_MIPMAP_LEVELS) {
            w = (w + 1) / 2;
            h = (h + 1) / 2;
            part->num_x_levels++;
            if (w <= 1 && h <= 1) break;
        } else {
            break;  /* Ripmap not fully supported yet */
        }
    }
    part->num_y_levels = part->num_x_levels;
    return total;
}

static ExrResult parse_offset_table(ExrDecoder decoder, ExrPartData* part, uint64_t* offset) {
    ExrResult result;
    ExrContext ctx = decoder->ctx;

    /* Calculate expected number of chunks */
    uint32_t expected_chunks;
    if (part->flags & EXR_IMAGE_TILED) {
        expected_chunks = calc_tiled_chunks(part);
    } else {
        expected_chunks = calc_scanline_chunks(part);
    }

    /* If chunkCount attribute was provided, use it */
    if (part->num_chunks == 0) {
        part->num_chunks = expected_chunks;
    }

    if (part->num_chunks == 0) {
        exr_context_add_error(ctx, EXR_ERROR_INVALID_DATA,
                              "Cannot determine chunk count", "offsets", *offset);
        return EXR_ERROR_INVALID_DATA;
    }

    /* SUSPEND POINT: Read offset table */
    size_t table_size = part->num_chunks * sizeof(uint64_t);
    part->offsets = (uint64_t*)ctx->allocator.alloc(
        ctx->allocator.userdata, table_size, EXR_DEFAULT_ALIGNMENT);
    if (!part->offsets) {
        return EXR_ERROR_OUT_OF_MEMORY;
    }

    result = sync_fetch(decoder, *offset, table_size, part->offsets);
    if (EXR_FAILED(result)) {
        exr_context_add_error(ctx, result,
                              "Failed to read offset table", "offsets", *offset);
        return result;
    }

    /* Convert from little-endian */
    for (uint32_t i = 0; i < part->num_chunks; i++) {
        uint8_t* p = (uint8_t*)&part->offsets[i];
        part->offsets[i] = read_le_u64(p);
    }

    *offset += table_size;
    return EXR_SUCCESS;
}

/* ============================================================================
 * Main Header Parsing Function
 * ============================================================================ */

ExrResult exr_decoder_parse_header(ExrDecoder decoder, ExrImage* out_image) {
    if (!exr_decoder_is_valid(decoder)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!out_image) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }
    if (decoder->state != EXR_DECODER_STATE_CREATED) {
        exr_context_add_error(decoder->ctx, EXR_ERROR_INVALID_STATE,
                              "Header already parsed or decoder in error state", NULL, 0);
        return EXR_ERROR_INVALID_STATE;
    }

    ExrContext ctx = decoder->ctx;
    ExrResult result;

    decoder->state = EXR_DECODER_STATE_PARSING_HEADER;

    /* Allocate image structure */
    ExrImage image = (ExrImage)ctx->allocator.alloc(
        ctx->allocator.userdata, sizeof(struct ExrImage_T), EXR_DEFAULT_ALIGNMENT);
    if (!image) {
        decoder->state = EXR_DECODER_STATE_ERROR;
        return EXR_ERROR_OUT_OF_MEMORY;
    }
    memset(image, 0, sizeof(struct ExrImage_T));
    image->decoder = decoder;
    image->ctx = ctx;
    image->magic = EXR_IMAGE_MAGIC;
    image->pixel_aspect_ratio = 1.0f;
    image->screen_window_width = 1.0f;
    decoder->image = image;
    exr_context_add_ref(ctx);

    /* Parse version header */
    result = parse_exr_version(decoder, image);
    if (EXR_FAILED(result)) {
        exr_image_destroy(image);
        decoder->image = NULL;
        decoder->state = EXR_DECODER_STATE_ERROR;
        return result;
    }

    /* Allocate parts array (at least 1 for single-part files) */
    uint32_t max_parts = (image->flags & EXR_IMAGE_MULTIPART) ? 32 : 1;
    image->parts = (ExrPartData*)ctx->allocator.alloc(
        ctx->allocator.userdata, max_parts * sizeof(ExrPartData), EXR_DEFAULT_ALIGNMENT);
    if (!image->parts) {
        exr_image_destroy(image);
        decoder->image = NULL;
        decoder->state = EXR_DECODER_STATE_ERROR;
        return EXR_ERROR_OUT_OF_MEMORY;
    }
    memset(image->parts, 0, max_parts * sizeof(ExrPartData));

    uint64_t offset = decoder->current_offset;

    /* Parse headers (multiple for multipart files) */
    do {
        if (image->num_parts >= max_parts) {
            exr_context_add_error(ctx, EXR_ERROR_INVALID_DATA,
                                  "Too many parts in multipart file", "header", offset);
            exr_image_destroy(image);
            decoder->image = NULL;
            decoder->state = EXR_DECODER_STATE_ERROR;
            return EXR_ERROR_INVALID_DATA;
        }

        ExrPartData* part = &image->parts[image->num_parts];
        part->part_type = (image->flags & EXR_IMAGE_TILED) ? EXR_PART_TILED : EXR_PART_SCANLINE;

        /* Parse all attributes for this part */
        int end_of_header = 0;
        while (!end_of_header) {
            result = parse_attribute(decoder, part, &offset, &end_of_header);
            if (EXR_FAILED(result)) {
                exr_image_destroy(image);
                decoder->image = NULL;
                decoder->state = EXR_DECODER_STATE_ERROR;
                return result;
            }
        }

        /* Update part type from tiles attribute */
        if (part->flags & EXR_IMAGE_TILED) {
            if (image->flags & EXR_IMAGE_DEEP) {
                part->part_type = EXR_PART_DEEP_TILED;
            } else {
                part->part_type = EXR_PART_TILED;
            }
        } else if (image->flags & EXR_IMAGE_DEEP) {
            part->part_type = EXR_PART_DEEP_SCANLINE;
        }

        image->num_parts++;

        /* For multipart files, check for empty header marking end */
        if (image->flags & EXR_IMAGE_MULTIPART) {
            uint8_t next_byte;
            result = sync_fetch(decoder, offset, 1, &next_byte);
            if (EXR_FAILED(result)) {
                break;
            }
            if (next_byte == 0) {
                offset++;  /* Skip null terminator */
                break;
            }
        }
    } while (image->flags & EXR_IMAGE_MULTIPART);

    /* Parse offset tables for each part */
    for (uint32_t i = 0; i < image->num_parts; i++) {
        result = parse_offset_table(decoder, &image->parts[i], &offset);
        if (EXR_FAILED(result)) {
            exr_image_destroy(image);
            decoder->image = NULL;
            decoder->state = EXR_DECODER_STATE_ERROR;
            return result;
        }
    }

    decoder->current_offset = offset;
    decoder->state = EXR_DECODER_STATE_HEADER_PARSED;
    *out_image = image;

    return EXR_SUCCESS;
}

ExrResult exr_decoder_wait_idle(ExrDecoder decoder) {
    if (!exr_decoder_is_valid(decoder)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    /* For now, nothing to wait for in stub implementation */
    return EXR_SUCCESS;
}

/* ============================================================================
 * Stub Implementations for Other Functions
 * ============================================================================ */

ExrResult exr_image_find_part_by_name(ExrImage image, const char* name,
                                       ExrPart* out_part) {
    if (!exr_image_is_valid(image)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!name || !out_part) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    for (uint32_t i = 0; i < image->num_parts; i++) {
        if (image->parts[i].name && strcmp(image->parts[i].name, name) == 0) {
            return exr_image_get_part(image, i, out_part);
        }
    }

    return EXR_ERROR_MISSING_ATTRIBUTE;
}

ExrResult exr_part_get_attribute_count(ExrPart part, uint32_t* out_count) {
    ExrPartData* data = exr_part_get_data(part);
    if (!data) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!out_count) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }
    *out_count = data->num_attributes;
    return EXR_SUCCESS;
}

ExrResult exr_part_get_attribute(ExrPart part, const char* name,
                                  ExrAttribute* out_attr) {
    ExrPartData* data = exr_part_get_data(part);
    if (!data) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!name || !out_attr) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    for (uint32_t i = 0; i < data->num_attributes; i++) {
        if (strcmp(data->attributes[i].name, name) == 0) {
            out_attr->name = data->attributes[i].name;
            out_attr->type_name = data->attributes[i].type_name;
            out_attr->type = data->attributes[i].type;
            out_attr->value = data->attributes[i].value;
            out_attr->size = data->attributes[i].size;
            return EXR_SUCCESS;
        }
    }

    return EXR_ERROR_MISSING_ATTRIBUTE;
}

ExrResult exr_part_get_attribute_at(ExrPart part, uint32_t index,
                                     ExrAttribute* out_attr) {
    ExrPartData* data = exr_part_get_data(part);
    if (!data) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!out_attr) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }
    if (index >= data->num_attributes) {
        return EXR_ERROR_OUT_OF_BOUNDS;
    }

    out_attr->name = data->attributes[index].name;
    out_attr->type_name = data->attributes[index].type_name;
    out_attr->type = data->attributes[index].type;
    out_attr->value = data->attributes[index].value;
    out_attr->size = data->attributes[index].size;
    return EXR_SUCCESS;
}

int exr_part_has_attribute(ExrPart part, const char* name) {
    ExrPartData* data = exr_part_get_data(part);
    if (!data || !name) {
        return 0;
    }

    for (uint32_t i = 0; i < data->num_attributes; i++) {
        if (strcmp(data->attributes[i].name, name) == 0) {
            return 1;
        }
    }
    return 0;
}

/* Command buffer request and submit functions are implemented above */

/* Encoder stubs */
ExrResult exr_encoder_create(ExrContext ctx,
                              const ExrEncoderCreateInfo* create_info,
                              ExrEncoder* out_encoder) {
    (void)ctx; (void)create_info; (void)out_encoder;
    return EXR_ERROR_UNSUPPORTED_FORMAT;
}

void exr_encoder_destroy(ExrEncoder encoder) {
    (void)encoder;
}

ExrResult exr_encoder_finalize(ExrEncoder encoder) {
    (void)encoder;
    return EXR_ERROR_UNSUPPORTED_FORMAT;
}

/* ============================================================================
 * Async Suspend/Resume Implementation
 * ============================================================================ */

/* Callback for async fetch completion - called by the data source when data is ready */
static void async_fetch_complete(void* userdata, ExrResult result, size_t bytes_read) {
    ExrSuspendState state = (ExrSuspendState)userdata;
    if (!exr_suspend_is_valid(state)) return;

    state->async_result = result;
    state->async_bytes_read = bytes_read;
    state->async_complete = 1;
}

/* Async-aware fetch that returns EXR_WOULD_BLOCK for async sources */
static ExrResult async_fetch(ExrDecoder decoder, uint64_t offset, uint64_t size,
                              void* dst, ExrSuspendState* out_state) {
    ExrDataSource* src = &decoder->source;

    /* Check if this is an async data source */
    if (!(src->flags & EXR_DATA_SOURCE_ASYNC)) {
        /* Synchronous source - fetch directly */
        return src->fetch(src->userdata, offset, size, dst, NULL, NULL);
    }

    /* Async source - need to allocate suspend state if not already present */
    ExrSuspendState state = decoder->suspend_state;
    if (!state) {
        ExrContext ctx = decoder->ctx;
        state = (ExrSuspendState)ctx->allocator.alloc(
            ctx->allocator.userdata, sizeof(struct ExrSuspendState_T), EXR_DEFAULT_ALIGNMENT);
        if (!state) {
            return EXR_ERROR_OUT_OF_MEMORY;
        }
        memset(state, 0, sizeof(struct ExrSuspendState_T));
        state->magic = EXR_SUSPEND_MAGIC;
        state->decoder = decoder;
        decoder->suspend_state = state;
    }

    /* Save pending fetch info */
    state->fetch_offset = offset;
    state->fetch_size = size;
    state->fetch_dst = dst;
    state->phase = decoder->current_phase;
    state->offset = decoder->current_offset;
    state->current_part = decoder->current_part_index;
    state->current_chunk = decoder->current_chunk_index;
    state->async_complete = 0;

    /* Initiate async fetch */
    ExrResult result = src->fetch(src->userdata, offset, size, dst,
                                   async_fetch_complete, state);

    if (result == EXR_WOULD_BLOCK) {
        /* Async operation started, caller should wait and call exr_resume */
        if (out_state) *out_state = state;
        return EXR_WOULD_BLOCK;
    }

    /* Synchronous completion (even from async source) */
    return result;
}

ExrResult exr_get_suspend_state(ExrDecoder decoder, ExrSuspendState* out_state) {
    if (!exr_decoder_is_valid(decoder)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!out_state) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    ExrSuspendState state = decoder->suspend_state;
    if (!state || !exr_suspend_is_valid(state)) {
        return EXR_ERROR_NOT_READY;
    }

    /* Return the current suspend state */
    *out_state = state;
    return EXR_SUCCESS;
}

ExrResult exr_resume(ExrDecoder decoder, ExrSuspendState state) {
    if (!exr_decoder_is_valid(decoder)) {
        return EXR_ERROR_INVALID_HANDLE;
    }
    if (!exr_suspend_is_valid(state)) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }
    if (state->decoder != decoder) {
        return EXR_ERROR_INVALID_ARGUMENT;
    }

    /* Check if async operation completed */
    if (!state->async_complete) {
        return EXR_WOULD_BLOCK;  /* Still waiting */
    }

    /* Check for async fetch error */
    if (EXR_FAILED(state->async_result)) {
        return state->async_result;
    }

    /* Restore decoder state and continue parsing */
    decoder->current_offset = state->offset;
    decoder->current_phase = state->phase;
    decoder->current_part_index = state->current_part;
    decoder->current_chunk_index = state->current_chunk;

    /* Clear the suspend state */
    decoder->suspend_state = NULL;
    state->magic = 0;  /* Invalidate */

    return EXR_SUCCESS;
}

void exr_suspend_state_destroy(ExrSuspendState state) {
    if (!exr_suspend_is_valid(state)) return;

    ExrDecoder decoder = state->decoder;
    if (decoder && decoder->suspend_state == state) {
        decoder->suspend_state = NULL;
    }

    state->magic = 0;

    /* Free the state - need context for allocator */
    if (decoder && exr_decoder_is_valid(decoder)) {
        ExrContext ctx = decoder->ctx;
        ctx->allocator.free(ctx->allocator.userdata, state,
                            sizeof(struct ExrSuspendState_T));
    }
    /* If decoder is invalid, we leak - but that's a programming error */
}

/* Decompression stubs */
ExrResult exr_decompress_chunk(ExrContext ctx, const ExrDecompressInfo* info) {
    (void)ctx; (void)info;
    return EXR_ERROR_UNSUPPORTED_FORMAT;
}

ExrResult exr_compress_chunk(ExrContext ctx, const ExrCompressInfo* info) {
    (void)ctx; (void)info;
    return EXR_ERROR_UNSUPPORTED_FORMAT;
}

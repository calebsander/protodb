#include <assert.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>
#include <node_api.h>

#define MMAP_NAME "mmap"

#define EXPECTED_MMAP_PARAMS 3
#define FD_PARAM 0
#define OFFSET_PARAM 1
#define CALLBACK_PARAM 2

#define CALLBACK_PARAMS 2
#define ERROR_PARAM 0
#define BUFFER_PARAM 1

uint32_t PAGE_SIZE;

// Unmaps an mmap()ed page when the wrapping ArrayBuffer is GCed
void mmap_finalize(napi_env env, void *addr, void *hint) {
	if (munmap(addr, PAGE_SIZE)) assert(false);

	// External memory is no longer in use
	int64_t bytes;
	napi_status status =
		napi_adjust_external_memory(env, -(int64_t) PAGE_SIZE, &bytes);
	assert(status == napi_ok);
}

typedef struct {
	int fd;
	off_t offset;
	void *addr;
	napi_ref callback;
	napi_async_work work;
} MmapContext;

// Async worker to mmap() a page
void mmap_execute(napi_env env, void *data) {
	MmapContext *context = (MmapContext *) data;

	// Ensure the file contains a full page at that index
	off_t length = lseek(context->fd, 0, SEEK_END);
	if (length < context->offset + PAGE_SIZE) {
		context->addr = MAP_FAILED;
		return;
	}

	// mmap() the page at any address
	context->addr = mmap(
		NULL,
		PAGE_SIZE,
		PROT_READ | PROT_WRITE,
		MAP_SHARED,
		context->fd,
		context->offset
	);
	if (context->addr == MAP_FAILED) return;

	// Force the page to be loaded
	volatile uint8_t byte = *(uint8_t *) context->addr;
	(void) byte;
}

// Async worker completion callback
void mmap_complete(napi_env env, napi_status status, void *data) {
	MmapContext *context = (MmapContext *) data;

	// Check whether mmap() succeeded
	char *error_message = NULL;
	napi_value argv[CALLBACK_PARAMS];
	if (status == napi_ok) {
		if (context->addr == MAP_FAILED) error_message = "mmap() failed";
		else {
			// Wrap the mmap()ed page in an ArrayBuffer
			status = napi_create_external_arraybuffer(
				env, context->addr, PAGE_SIZE, mmap_finalize, NULL, &argv[BUFFER_PARAM]
			);
			assert(status == napi_ok);

			// Tell V8 that the ArrayBuffer is holding a page
			int64_t bytes;
			status = napi_adjust_external_memory(env, PAGE_SIZE, &bytes);
			assert(status == napi_ok);
		}
	}
	else error_message = "Async operation failed";

	// Invoke callback with error or buffer
	napi_value callback;
	status = napi_get_reference_value(env, context->callback, &callback);
	assert(status == napi_ok);
	napi_value null;
	status = napi_get_null(env, &null);
	assert(status == napi_ok);
	if (error_message) { // operation errored
		napi_value message_string;
		status = napi_create_string_utf8(
			env, error_message, NAPI_AUTO_LENGTH, &message_string
		);
		assert(status == napi_ok);
		status = napi_create_error(env, NULL, message_string, &argv[ERROR_PARAM]);
		assert(status == napi_ok);
		argv[BUFFER_PARAM] = null;
	}
	else argv[ERROR_PARAM] = null; // operation succeeded
	status = napi_call_function(env, null, callback, CALLBACK_PARAMS, argv, NULL);
	if (status != napi_ok) {
		// The callback may have thrown an error. If so, don't throw another.
		bool error_pending;
		status = napi_is_exception_pending(env, &error_pending);
		assert(status == napi_ok);
		if (!error_pending) {
			status = napi_throw_type_error(env, NULL, "Failed to invoke callback");
			assert(status == napi_ok);
		}
	}

	// Clean up context
	status = napi_delete_reference(env, context->callback);
	assert(status == napi_ok);
	status = napi_delete_async_work(env, context->work);
	assert(status == napi_ok);
	free(context);
}

// Function called to initiate mmap() async work
napi_value mmap_wrapper(napi_env env, napi_callback_info info) {
	// Expected params: fd, offset, callback
	size_t argc = EXPECTED_MMAP_PARAMS;
	napi_value argv[EXPECTED_MMAP_PARAMS];
	napi_status status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
	assert(status == napi_ok);
	if (argc != EXPECTED_MMAP_PARAMS) {
		status = napi_throw_type_error(env, NULL, "Invalid arguments to " MMAP_NAME "()");
		assert(status == napi_ok);
		return NULL;
	}

	// Extract fd
	MmapContext *context = malloc(sizeof(*context));
	status = napi_get_value_int32(env, argv[FD_PARAM], &context->fd);
	if (status == napi_number_expected) {
		status = napi_throw_type_error(env, NULL, "Invalid fd");
		assert(status == napi_ok);
		free(context);
		return NULL;
	}
	assert(status == napi_ok);

	// Extract offset
	status = napi_get_value_int64(env, argv[OFFSET_PARAM], &context->offset);
	if (
		status == napi_number_expected ||
		context->offset < 0 || context->offset & (PAGE_SIZE - 1)
	) {
		status = napi_throw_type_error(env, NULL, "Invalid offset");
		assert(status == napi_ok);
		free(context);
		return NULL;
	}
	assert(status == napi_ok);

	// Make reference to callback for use in mmap_complete()
	status = napi_create_reference(env, argv[CALLBACK_PARAM], 1, &context->callback);
	if (status != napi_ok) {
		status = napi_throw_type_error(env, NULL, "Invalid callback");
		assert(status == napi_ok);
		free(context);
		return NULL;
	}

	// Create async work to perform the mmap()
	napi_value name;
	status = napi_create_string_utf8(env, "MMAPWRAP", NAPI_AUTO_LENGTH, &name);
	assert(status == napi_ok);
	status = napi_create_async_work(
		env, NULL, name, mmap_execute, mmap_complete, context, &context->work
	);
	assert(status == napi_ok);
	status = napi_queue_async_work(env, context->work);
	assert(status == napi_ok);

	return NULL;
}

// Initialize the native module
napi_value init(napi_env env, napi_value exports) {
	// Fetch the page size and its base-2 log
	// Export "LOG_PAGE_SIZE" and "PAGE_SIZE"
	PAGE_SIZE = getpagesize();
	napi_value log_page_size;
	napi_status status =
		napi_create_uint32(env, __builtin_ctz(PAGE_SIZE), &log_page_size);
	assert(status == napi_ok);
	status = napi_set_named_property(env, exports, "LOG_PAGE_SIZE", log_page_size);
	assert(status == napi_ok);
	napi_value page_size;
	status = napi_create_uint32(env, PAGE_SIZE, &page_size);
	assert(status == napi_ok);
	status = napi_set_named_property(env, exports, "PAGE_SIZE", page_size);
	assert(status == napi_ok);

	// Export mmap_wrapper as function "mmap"
	napi_value mmap_fn;
	status = napi_create_function(
		env, MMAP_NAME, NAPI_AUTO_LENGTH, mmap_wrapper, NULL, &mmap_fn
	);
	assert(status == napi_ok);
	status = napi_set_named_property(env, exports, MMAP_NAME, mmap_fn);
	assert(status == napi_ok);

	return exports;
}
NAPI_MODULE(NODE_GYP_MODULE_NAME, init)

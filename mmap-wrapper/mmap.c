#include <assert.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>
#include <node_api.h>

#define MMAP_NAME "mmap"
#define EXPECTED_MMAP_PARAMS 3
#define CALLBACK_PARAMS 2

uint32_t PAGE_SIZE;

void mmap_finalize(napi_env env, void *finalize_data, void *addr) {
	if (munmap(addr, PAGE_SIZE)) {
		napi_status status = napi_throw_error(env, NULL, "munmap() failed");
		assert(status == napi_ok);
	}
}

typedef struct {
	int fd;
	off_t offset;
	void *addr;
	napi_ref callback;
	napi_async_work work;
} MmapContext;

void mmap_execute(napi_env env, void *data) {
	MmapContext *context = (MmapContext *) data;
	off_t length = lseek(context->fd, 0, SEEK_END);
	if (length < context->offset + PAGE_SIZE) {
		context->addr = MAP_FAILED;
		return;
	}

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
	int64_t bytes;
	napi_status status = napi_adjust_external_memory(env, PAGE_SIZE, &bytes);
	assert(status == napi_ok);
}

void mmap_complete(napi_env env, napi_status status, void *data) {
	MmapContext *context = (MmapContext *) data;
	char *error_message = NULL;
	napi_value argv[CALLBACK_PARAMS];
	if (status == napi_ok) {
		if (context->addr == MAP_FAILED) error_message = "mmap() failed";
		else {
			status = napi_create_external_arraybuffer(
				env, context->addr, PAGE_SIZE, mmap_finalize, context->addr, &argv[1]
			);
			assert(status == napi_ok);
		}
	}
	else error_message = "Async operation failed";

	napi_value callback;
	status = napi_get_reference_value(env, context->callback, &callback);
	assert(status == napi_ok);
	napi_value null;
	status = napi_get_null(env, &null);
	assert(status == napi_ok);
	if (error_message) {
		napi_value message_string;
		status = napi_create_string_utf8(
			env, error_message, NAPI_AUTO_LENGTH, &message_string
		);
		assert(status == napi_ok);
		status = napi_create_error(env, NULL, message_string, &argv[0]);
		assert(status == napi_ok);
		argv[1] = null;
	}
	else argv[0] = null;
	status = napi_call_function(env, null, callback, CALLBACK_PARAMS, argv, NULL);
	if (status != napi_ok) {
		bool error_pending;
		status = napi_is_exception_pending(env, &error_pending);
		assert(status == napi_ok);
		if (!error_pending) {
			status = napi_throw_type_error(env, NULL, "Failed to invoke callback");
			assert(status == napi_ok);
		}
	}

	status = napi_delete_reference(env, context->callback);
	assert(status == napi_ok);
	status = napi_delete_async_work(env, context->work);
	assert(status == napi_ok);
	free(context);
}

napi_value mmap_wrapper(napi_env env, napi_callback_info info) {
	size_t argc = EXPECTED_MMAP_PARAMS;
	napi_value argv[EXPECTED_MMAP_PARAMS];
	napi_status status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
	assert(status == napi_ok);

	if (argc != EXPECTED_MMAP_PARAMS) {
		status = napi_throw_type_error(env, NULL, "Invalid arguments to " MMAP_NAME "()");
		assert(status == napi_ok);
		return NULL;
	}

	MmapContext *context = malloc(sizeof(*context));
	status = napi_get_value_int32(env, argv[0], &context->fd);
	if (status == napi_number_expected) {
		status = napi_throw_type_error(env, NULL, "Invalid fd");
		assert(status == napi_ok);
		free(context);
		return NULL;
	}
	assert(status == napi_ok);

	status = napi_get_value_int64(env, argv[1], &context->offset);
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

	status = napi_create_reference(env, argv[2], 1, &context->callback);
	if (status != napi_ok) {
		status = napi_throw_type_error(env, NULL, "Invalid callback");
		assert(status == napi_ok);
		free(context);
		return NULL;
	}

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

napi_value Init(napi_env env, napi_value exports) {
	PAGE_SIZE = sysconf(_SC_PAGESIZE);
	napi_value log_page_size;
	napi_status status = napi_create_uint32(env, __builtin_ctz(PAGE_SIZE), &log_page_size);
	assert(status == napi_ok);
	status = napi_set_named_property(env, exports, "LOG_PAGE_SIZE", log_page_size);
	assert(status == napi_ok);

	napi_value page_size;
	status = napi_create_uint32(env, PAGE_SIZE, &page_size);
	assert(status == napi_ok);
	status = napi_set_named_property(env, exports, "PAGE_SIZE", page_size);
	assert(status == napi_ok);

	napi_value mmap_fn;
	status = napi_create_function(
		env, MMAP_NAME, NAPI_AUTO_LENGTH, mmap_wrapper, NULL, &mmap_fn
	);
	assert(status == napi_ok);

	status = napi_set_named_property(env, exports, MMAP_NAME, mmap_fn);
	assert(status == napi_ok);

	return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)

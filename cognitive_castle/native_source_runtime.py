"""Engine-free request boundary for the one fixed native source stage."""

from .native_export_codec import frame_bytes, initial_cursor, source_binding, validate_cursor
from .sqlite_source_runtime import source_refusal as source_refusal, source_result as source_result


def source_request(descriptor, cursor=None, *, layout=None):
    binding = source_binding(descriptor)
    if cursor is None:
        cursor = initial_cursor(binding, descriptor["store_id"], descriptor["start_cursor"])
    validate_cursor(cursor, binding, descriptor["store_id"], descriptor["start_cursor"])
    if layout is not None:
        from .native_export_codec import prepare_layout_request

        return prepare_layout_request(descriptor, cursor, layout)
    request = {"descriptor": descriptor, "cursor": cursor}
    frame_bytes(request)
    return request

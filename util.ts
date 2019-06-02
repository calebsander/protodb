export function concat(buffers: Uint8Array[]): Uint8Array {
	const totalLength =
		buffers.reduce((totalLength, {length}) => totalLength + length, 0)
	const result = new Uint8Array(totalLength)
	let offset = 0
	for (const buffer of buffers) {
		result.set(buffer, offset)
		offset += buffer.length
	}
	return result
}

export function ensureOverflowError(e: Error) {
	if (!(e instanceof RangeError && e.message === 'Source is too large')) {
		throw e // unexpected error; rethrow it
	}
}
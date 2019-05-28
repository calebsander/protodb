export function concat(buffers: Uint8Array[]): Uint8Array {
	const totalLength =
		buffers.map(({length}) => length).reduce((a, b) => a + b, 0)
	const result = new Uint8Array(totalLength)
	let offset = 0
	for (const buffer of buffers) {
		result.set(buffer, offset)
		offset += buffer.length
	}
	return result
}
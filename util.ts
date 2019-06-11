import {Reader} from 'protobufjs'
import {FilePage} from './cache'

// Finds the index of the value in an array that minimizes an objective function
export function argmin<T>(arr: T[], keyFunc: (t: T) => number): number {
	let minIndex: number | undefined
	let minValue = Infinity
	arr.forEach((item, index) => {
		const value = keyFunc(item)
		if (value < minValue) {
			minIndex = index
			minValue = value
		}
	})
	// istanbul ignore if
	if (minIndex === undefined) throw new Error('Empty array')
	return minIndex
}

// Concatenates an array of Uint8Arrays
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

// Checks that an error is the result of a write overflowing its page
export function ensureOverflowError(e: Error): void {
	// istanbul ignore if
	if (!(e instanceof RangeError && e.message === 'Source is too large')) {
		throw e // unexpected error; rethrow it
	}
}

// Gets the size of an object that is serialized with encodeDelimited().
// Useful for figuring out what portion of a page is occupied.
export const getNodeLength = (file: string, page: number): Promise<number> =>
	new FilePage(file, page).use(async page => {
		const reader = new Reader(new Uint8Array(page))
		return reader.uint32() + reader.pos
	})
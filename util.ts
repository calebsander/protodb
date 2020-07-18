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

let OVERFLOW_ERROR_TYPE: Function,
    OVERFLOW_ERROR_MESSAGE: string
try {
	// Trigger an overflow by assigning 1 byte to a 0-byte buffer
	const buffer = new Uint8Array
	buffer.set([0])
}
catch (e) {
	const err: Error = e
	OVERFLOW_ERROR_TYPE = err.constructor
	OVERFLOW_ERROR_MESSAGE = err.message
}

// Checks that an error is the result of a write overflowing its page
export function ensureOverflowError(e: Error): void {
	// istanbul ignore if
	if (!(e instanceof OVERFLOW_ERROR_TYPE && e.message === OVERFLOW_ERROR_MESSAGE)) {
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
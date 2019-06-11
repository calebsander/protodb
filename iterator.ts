import {randomBytes} from 'crypto'
import {promisify} from 'util'
import {ITER_BYTE_LENGTH} from './constants'

const randomBytesPromise = promisify(randomBytes)

// Converts a 16-byte iterator handle to a string for use as a Map key
const getKey = (iter: Uint8Array): string =>
	Buffer.from(iter.buffer, iter.byteOffset, iter.length).toString('hex')

export interface CollectionIterator<STATE> {
	name: string
	iterator: STATE
}

export class Iterators<STATE> {
	// Maps iterator handles to their associated iterators
	private readonly iterators = new Map<string, CollectionIterator<STATE>>()
	// Maps collection names to their number of active iterators
	private readonly iteratorCounts = new Map<string, number>()

	private lookupIterator(key: string) {
		const iterator = this.iterators.get(key)
		if (!iterator) throw new Error('Unknown iterator')
		return iterator
	}
	async registerIterator(name: string, iterator: STATE): Promise<Uint8Array> {
		const iter = await randomBytesPromise(ITER_BYTE_LENGTH)
		this.iterators.set(getKey(iter), {name, iterator})
		this.iteratorCounts.set(name, (this.iteratorCounts.get(name) || 0) + 1)
		return iter
	}
	getIterator(iter: Uint8Array): STATE {
		return this.lookupIterator(getKey(iter)).iterator
	}
	closeIterator(iter: Uint8Array): void {
		const key = getKey(iter)
		const {name} = this.lookupIterator(key)
		this.iterators.delete(key)
		const oldCount = this.iteratorCounts.get(name)
		// istanbul ignore if
		if (!oldCount) throw new Error('Hash has no iterators?')
		if (oldCount > 1) this.iteratorCounts.set(name, oldCount - 1)
		else this.iteratorCounts.delete(name)
	}
	checkNoIterators(name: string): void {
		if (this.iteratorCounts.has(name)) {
			throw new Error(`Collection ${name} has active iterators`)
		}
	}
}
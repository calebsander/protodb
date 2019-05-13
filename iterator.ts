import * as crypto from 'crypto'
import {promisify} from 'util'
import {ITER_BYTE_LENGTH} from './constants'

const randomBytes = promisify(crypto.randomBytes)

const getKey = (iter: Uint8Array): string =>
	Buffer.from(iter).toString('hex')

export interface CollectionIterator<STATE> {
	name: string
	iterator: STATE
}

export class Iterators<STATE> {
	private readonly iterators = new Map<string, CollectionIterator<STATE>>()
	private readonly iteratorCounts = new Map<string, number>()

	async registerIterator(name: string, iterator: STATE): Promise<Uint8Array> {
		this.iteratorCounts.set(name, (this.iteratorCounts.get(name) || 0) + 1)
		const iter = await randomBytes(ITER_BYTE_LENGTH)
		this.iterators.set(getKey(iter), {name, iterator})
		return iter
	}
	getIterator(iter: Uint8Array): STATE {
		const iterator = this.iterators.get(getKey(iter))
		if (!iterator) throw new Error('Unknown iterator')
		return iterator.iterator
	}
	closeIterator(iter: Uint8Array): void {
		const key = getKey(iter)
		const iterator = this.iterators.get(key)
		if (!iterator) throw new Error('Unknown iterator')
		const {name} = iterator
		this.iterators.delete(getKey(iter))
		const oldCount = this.iteratorCounts.get(name)
		if (!oldCount) throw new Error('Hash has no iterators?')
		if (oldCount > 1) this.iteratorCounts.set(name, oldCount - 1)
		else this.iteratorCounts.delete(name)
	}
	checkNoIterators(name: string) {
		if (this.iteratorCounts.has(name)) {
			throw new Error(`Collection ${name} has active iterators`)
		}
	}
}
import net = require('net')
import {Writable} from 'stream'
import {DEFAULT_PORT} from '../constants'
import {DelimitedReader, DelimitedWriter} from '../delimited-stream'
import {Type} from '../pb/common'
import {
	Collections,
	CollectionType,
	KeyElement,
	KeyValuePair,
	SortedKeyValuePair
} from '../pb/interface'
import {
	bytesResponseType,
	Command,
	commandType,
	ErrorResponse,
	iterResponseType,
	listResponseType,
	optionalBytesResponseType,
	optionalPairResponseType,
	optionalSortedPairResponse,
	sizeResponseType,
	sortedPairListResponseType,
	voidResponseType
} from '../pb/request'
import {Queue} from '../queue'

const toUint8Array = (buffer: ArrayBuffer | Uint8Array) =>
	buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer)
const toOptionalIndex = (index?: number) =>
	index === undefined ? {none: {}} : {value: index}
const toOptionalKey = (key?: KeyElement[]) =>
	key ? {value: {elements: key}} : {none: {}}
const toOptionalBytes = (value: {data: Uint8Array} | {none: {}}) =>
	'data' in value ? value.data : null
const bufferToUint8Array = ({buffer, byteOffset, byteLength}: Buffer) =>
	new Uint8Array(buffer, byteOffset, byteLength)

/** An error indicating that the `protoDB` server failed to run a command */
export class ProtoDBError extends Error {
	get name() {
		return this.constructor.name
	}
}

type ResponseCallback = (response: Uint8Array) => void

/**
 * A `protoDB` client that can be used from JavaScript/TypeScript.
 * Each method serializes a type of command and sends it over TCP to the server.
 * The returned `Promise` resolves to the server's response.
 */
export class ProtoDBClient {
	private readonly responseQueue = new Queue<ResponseCallback>()
	private readonly socket: net.Socket
	private readonly connected: Promise<void>
	private readonly requestStream: Writable

	/**
	 * @param port the TCP port to issues commands to (default `9000`)
	 * @param host the hostname to connect to (default `localhost`)
	 */
	constructor(port = DEFAULT_PORT, host = 'localhost') {
		this.socket = net.connect(port, host)
		this.connected = new Promise((resolve, reject) =>
			this.socket
				.on('connect', resolve)
				.on('error', reject)
		)
		this.requestStream = new DelimitedWriter
		this.requestStream.pipe(this.socket)
		this.socket.pipe(new DelimitedReader)
			.on('data', (response: Buffer) =>
				this.responseQueue.dequeue()(bufferToUint8Array(response))
			)
	}

	private async runCommand<T extends object>(
		command: Command, responseType: Type<T | ErrorResponse>
	): Promise<T> {
		await this.connected
		const data = await new Promise<Uint8Array>(resolve => {
			this.requestStream.write(commandType.encode(command).finish())
			this.responseQueue.enqueue(resolve)
		})
		const response = responseType.toObject(
			responseType.decode(data),
			{defaults: true, longs: Number}
		)
		if ('error' in response) throw new ProtoDBError(response.error)
		return response
	}

	/**
	 * Closes the TCP connection to the server.
	 *
	 * @return a promise that resolves when the socket is closed
	 */
	async close(): Promise<void> {
		await this.connected
		await new Promise(resolve => this.socket.end(resolve))
	}

	/**
	 * Lists the name and type of each collection stored by the database.
	 * Example:
	 * ```js
	 * await client.hashCreate('h')
	 * console.log(await client.list()) // { h: CollectionType.HASH }
	 * ```
	 *
	 * @return all collections in the database
	 */
	async list(): Promise<Collections> {
		const {db} = await this.runCommand({list: {}}, listResponseType)
		return db.collections
	}

	/**
	 * Creates an item, a collection that stores a mutable global value.
	 *
	 * @param name the name of the item to create
	 */
	async itemCreate(name: string): Promise<void> {
		await this.runCommand({itemCreate: {name}}, voidResponseType)
	}
	/**
	 * Removes an item from the database.
	 *
	 * @param name the name passed to `itemCreate()`
	 */
	async itemDrop(name: string): Promise<void> {
		await this.runCommand({itemDrop: {name}}, voidResponseType)
	}
	/**
	 * Retrieves the value stored in an item.
	 * Results in an error if the item has not been set.
	 * Example:
	 * ```js
	 * await client.itemCreate('i')
	 * await client.itemSet('i', new Uint8Array([1, 2, 3]))
	 * console.log(await client.itemGet('i')) // Uint8Array [ 1, 2, 3 ]
	 * ```
	 *
	 * @param name the name passed to `itemCreate()`
	 * @returns the value that the item was last set to
	 */
	async itemGet(name: string): Promise<Uint8Array> {
		const {data} = await this.runCommand({itemGet: {name}}, bytesResponseType)
		return data
	}
	/**
	 * Stores a value in an item.
	 * Example:
	 * ```js
	 * await client.itemCreate('i')
	 * await client.itemSet('i', new Uint8Array([1, 2, 3]))
	 * console.log(await client.itemGet('i')) // Uint8Array [ 1, 2, 3 ]
	 * ```
	 *
	 * @param name the name passed to `itemCreate()`
	 * @param value the value to set the item to
	 */
	async itemSet(name: string, value: ArrayBuffer | Uint8Array): Promise<void> {
		await this.runCommand(
			{itemSet: {name, value: toUint8Array(value)}},
			voidResponseType
		)
	}

	/**
	 * Creates a hash, a collection that maps arbitrary keys to values.
	 * Hashes are optimized for setting and retrieving
	 * the value corresponding to a given key.
	 * They can also be used as sets by storing empty value buffers.
	 *
	 * @param name the name of the hash to create
	 */
	async hashCreate(name: string): Promise<void> {
		await this.runCommand({hashCreate: {name}}, voidResponseType)
	}
	/**
	 * Removes a hash from the database.
	 *
	 * @param name the name passed to `hashCreate()`
	 */
	async hashDrop(name: string): Promise<void> {
		await this.runCommand({hashDrop: {name}}, voidResponseType)
	}
	/**
	 * Deletes the key-value pair with the given key from a hash.
	 * If the key is not in the hash, the hash is left unchanged.
	 * Example:
	 * ```js
	 * await client.hashCreate('h')
	 * await client.hashSet('h', new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6]))
	 * console.log(await client.hashGet('h', new Uint8Array([1, 2, 3]))) // Uint8Array [ 4, 5, 6 ]
	 * await client.hashDelete('h', new Uint8Array([1, 2, 3]))
	 * console.log(await client.hashGet('h', new Uint8Array([1, 2, 3]))) // null
	 * ```
	 *
	 * @param name the name passed to `hashCreate()`
	 * @param key the key of the key-value pair to remove from the hash
	 */
	async hashDelete(name: string, key: ArrayBuffer | Uint8Array): Promise<void> {
		await this.runCommand(
			{hashDelete: {name, key: toUint8Array(key)}},
			voidResponseType
		)
	}
	/**
	 * Retrieves the value corresponding to the given key in a hash.
	 * Results in `null` if the key is not in the hash.
	 * Example:
	 * ```js
	 * await client.hashCreate('h')
	 * await client.hashSet('h', new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6]))
	 * console.log(await client.hashGet('h', new Uint8Array([1, 2, 3]))) // Uint8Array [ 4, 5, 6 ]
	 * console.log(await client.hashGet('h', new Uint8Array([3, 2, 1]))) // null
	 * ```
	 *
	 * @param name the name passed to `hashCreate()`
	 * @param key the key to look up in the hash
	 * @return the value that the key maps to, or `null` if the key is not present
	 */
	async hashGet(name: string, key: ArrayBuffer | Uint8Array): Promise<Uint8Array | null> {
		const value = await this.runCommand(
			{hashGet: {name, key: toUint8Array(key)}},
			optionalBytesResponseType
		)
		return toOptionalBytes(value)
	}
	/**
	 * Inserts a key-value pair into a hash
	 * or updates the value of the pair with the given key.
	 * Example:
	 * ```js
	 * await client.hashCreate('h')
	 * await client.hashSet('h', new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6]))
	 * console.log(await client.hashGet('h', new Uint8Array([1, 2, 3]))) // Uint8Array [ 4, 5, 6 ]
	 * await client.hashSet('h', new Uint8Array([1, 2, 3]), new Uint8Array([7, 8, 9]))
	 * console.log(await client.hashGet('h', new Uint8Array([1, 2, 3]))) // Uint8Array [ 7, 8, 9 ]
	 * ```
	 *
	 * @param name the name passed to `hashCreate()`
	 * @param key the key to insert/update in the hash
	 * @param value the new value to associate with the key
	 */
	async hashSet(
		name: string, key: ArrayBuffer | Uint8Array, value: ArrayBuffer | Uint8Array
	): Promise<void> {
		await this.runCommand(
			{hashSet: {name, key: toUint8Array(key), value: toUint8Array(value)}},
			voidResponseType
		)
	}
	/**
	 * Gets the number of key-value pairs in a hash.
	 * Example:
	 * ```js
	 * await client.hashCreate('h')
	 * await client.hashSet('h', new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6]))
	 * await client.hashSet('h', new Uint8Array([3, 2, 1]), new Uint8Array([6, 5, 4]))
	 * console.log(await client.hashSize('h')) // 2
	 * ```
	 *
	 * @param name the name passed to `hashCreate()`
	 * @return the size of the hash
	 */
	async hashSize(name: string): Promise<number> {
		const {size} = await this.runCommand({hashSize: {name}}, sizeResponseType)
		return size
	}
	/**
	 * Starts an iteration over the key-value pairs in a hash.
	 * The iteration order is undefined but guaranteed
	 * not to change unless the hash is modified.
	 * The hash cannot be modified while any iterations are active.
	 * Example:
	 * ```js
	 * await client.hashCreate('h')
	 * await client.hashSet('h', new Uint8Array([1]), new Uint8Array([10]))
	 * await client.hashSet('h', new Uint8Array([2]), new Uint8Array([20]))
	 * const iter = await client.hashIter('h')
	 * console.log(await client.hashIterNext(iter)) // { key: Uint8Array [ 1 ], value: Uint8Array [ 10 ] }
	 * console.log(await client.hashIterNext(iter)) // { key: Uint8Array [ 2 ], value: Uint8Array [ 20 ] }
	 * console.log(await client.hashIterNext(iter)) // null
	 * ```
	 *
	 * @param name the name passed to `hashCreate()`
	 * @return an iteration key that refers to the iterator
	 */
	async hashIter(name: string): Promise<Uint8Array> {
		const {iter} = await this.runCommand({hashIter: {name}}, iterResponseType)
		return iter
	}
	/**
	 * Stops a hash iteration.
	 * This is only necessary if the iteration is ended early,
	 * i.e. before `hashIterNext()` returns `null`.
	 *
	 * @param iter the iteration key returned from `hashIter()`
	 */
	async hashIterBreak(iter: Uint8Array): Promise<void> {
		await this.runCommand({hashIterBreak: {iter}}, voidResponseType)
	}
	/**
	 * Retrieves the next key-value pair in a hash iteration,
	 * or `null` if the iteration has finished.
	 * The iteration is marked inactive as soon as it finishes.
	 *
	 * @param iter the iteration key returned from `hashIter()`
	 * @return the next item in the hash
	 */
	async hashIterNext(iter: Uint8Array): Promise<KeyValuePair | null> {
		const {pair} = await this.runCommand(
			{hashIterNext: {iter}},
			optionalPairResponseType
		)
		return pair || null
	}

	/**
	 * Creates a list, a sequence of values that can be accessed by index.
	 * Lists are optimized for random access, insertion, and deletion.
	 * The first element in a list is at index `0` and the last is at `size - 1`.
	 * Lists can be used for queues, stacks, or deques.
	 *
	 * @param name the name of the list to create
	 */
	async listCreate(name: string): Promise<void> {
		await this.runCommand({listCreate: {name}}, voidResponseType)
	}
	/**
	 * Removes a list from the database.
	 *
	 * @param name the name passed to `listCreate()`
	 */
	async listDrop(name: string): Promise<void> {
		await this.runCommand({listDrop: {name}}, voidResponseType)
	}
	/**
	 * Deletes the value at a given index in a list.
	 * If the index is negative, it is interpreted as an offset
	 * from the end of the list (e.g. `-1` is the last element).
	 * The indices of all subsequent elements decrease by 1.
	 * Example:
	 * ```js
	 * await client.listCreate('l')
	 * await client.listInsert('l', new Uint8Array([1]))
	 * await client.listInsert('l', new Uint8Array([2]))
	 * await client.listDelete('l', 0) // remove the first element
	 * console.log(await client.listGet('l', 0)) // Uint8Array [ 2 ]
	 * ```
	 *
	 * @param name the name passed to `listCreate()`
	 * @param index the index of the element to remove
	 */
	async listDelete(name: string, index: number): Promise<void> {
		await this.runCommand({listDelete: {name, index}}, voidResponseType)
	}
	/**
	 * Retrieves the value at a given index in a list.
	 * If the index is negative, it is interpreted as an offset
	 * from the end of the list (e.g. `-1` is the last element).
	 * Example:
	 * ```js
	 * await client.listCreate('l')
	 * await client.listInsert('l', new Uint8Array([1]))
	 * await client.listInsert('l', new Uint8Array([2]))
	 * console.log(await client.listGet('l', 1)) // Uint8Array [ 2 ]
	 * ```
	 *
	 * @param name the name passed to `listCreate()`
	 * @param index the index of the element to fetch
	 * @return the list element at the given index
	 */
	async listGet(name: string, index: number): Promise<Uint8Array> {
		const {data} =
			await this.runCommand({listGet: {name, index}}, bytesResponseType)
		return data
	}
	/**
	 * Inserts a value at a given index in a list.
	 * If the index is negative, it is interpreted as an offset
	 * from the end of the list (e.g. `-1` is the last element).
	 * If the index is omitted, the value is appended to the end of the list.
	 * The indices of all subsequent elements increase by 1.
	 * Example:
	 * ```js
	 * await client.listCreate('l')
	 * await client.listInsert('l', new Uint8Array([2])) // add to the end
	 * await client.listInsert('l', new Uint8Array([1]), 0) // add to the start
	 * console.log(await client.listGet('l', 0)) // Uint8Array [ 1 ]
	 * console.log(await client.listGet('l', 1)) // Uint8Array [ 2 ]
	 * ```
	 *
	 * @param name the name passed to `listCreate()`
	 * @param value the value to insert
	 * @param index the insertion index, or undefined if appending to the end
	 */
	async listInsert(
		name: string, value: ArrayBuffer | Uint8Array, index?: number
	): Promise<void> {
		await this.runCommand(
			{listInsert: {
				name,
				index: toOptionalIndex(index),
				value: toUint8Array(value)
			}},
			voidResponseType
		)
	}
	/**
	 * Stores a value at a given index in a list.
	 * If the index is negative, it is interpreted as an offset
	 * from the end of the list (e.g. `-1` is the last element).
	 * Example:
	 * ```js
	 * await client.listCreate('l')
	 * await client.listInsert('l', new Uint8Array([1]))
	 * console.log(await client.listGet('l', 0)) // Uint8Array [ 1 ]
	 * await client.listSet('l', 0, new Uint8Array([100]))
	 * console.log(await client.listGet('l', 0)) // Uint8Array [ 100 ]
	 * ```
	 *
	 * @param name the name passed to `listCreate()`
	 * @param index the index of the element to set
	 * @param value the value to store
	 */
	async listSet(
		name: string, index: number, value: ArrayBuffer | Uint8Array
	): Promise<void> {
		await this.runCommand(
			{listSet: {name, index, value: toUint8Array(value)}},
			voidResponseType
		)
	}
	/**
	 * Gets the length of a list.
	 * Example:
	 * ```js
	 * await client.listCreate('l')
	 * await client.listInsert('l', new Uint8Array([1]))
	 * await client.listInsert('l', new Uint8Array([2]))
	 * await client.listInsert('l', new Uint8Array([3]))
	 * console.log(await client.listSize('l')) // 3
	 * ```
	 *
	 * @param name the name passed to `listCreate()`
	 * @return the number of elements in the list
	 */
	async listSize(name: string): Promise<number> {
		const {size} = await this.runCommand({listSize: {name}}, sizeResponseType)
		return size
	}
	/**
	 * Starts an iteration over the values in a list.
	 * Start and end indices may be specified to iterate over a slice of the list.
	 * The indices cannot be negative and the end index is exclusive.
	 * The list cannot be modified while any iterations are active.
	 * Example:
	 * ```js
	 * await client.listCreate('l')
	 * await client.listInsert('l', new Uint8Array([1]))
	 * await client.listInsert('l', new Uint8Array([2]))
	 * await client.listInsert('l', new Uint8Array([3]))
	 * const iter = await client.listIter('l', 0, 2) // return the first 2 values
	 * console.log(await client.listIterNext(iter)) // Uint8Array [ 1 ]
	 * console.log(await client.listIterNext(iter)) // Uint8Array [ 2 ]
	 * console.log(await client.listIterNext(iter)) // null
	 * ```
	 *
	 * @param name the name passed to `listCreate()`
	 * @param start an optional (inclusive) starting index
	 * @param end an optional (exclusive) ending index
	 * @return an iteration key that refers to the iterator
	 */
	async listIter(name: string, start?: number, end?: number): Promise<Uint8Array> {
		if (start && start < 0 || end && end < 0) {
			throw new RangeError(`Bounds cannot be end-relative; got ${start} and ${end}`)
		}
		const {iter} = await this.runCommand(
			{listIter: {
				name,
				start: toOptionalIndex(start),
				end: toOptionalIndex(end)
			}},
			iterResponseType
		)
		return iter
	}
	/**
	 * Stops a list iteration.
	 * This is only necessary if the iteration is ended early,
	 * i.e. before `listIterNext()` returns `null`.
	 *
	 * @param iter the iteration key returned from `listIter()`
	 */
	async listIterBreak(iter: Uint8Array): Promise<void> {
		await this.runCommand({listIterBreak: {iter}}, voidResponseType)
	}
	/**
	 * Retrieves the next value in a list iteration,
	 * or `null` if the iteration has finished.
	 * The iteration is marked inactive as soon as it finishes.
	 *
	 * @param iter the iteration key returned from `listIter()`
	 * @return the next value in the list
	 */
	async listIterNext(iter: Uint8Array): Promise<Uint8Array | null> {
		const value = await this.runCommand(
			{listIterNext: {iter}},
			optionalBytesResponseType
		)
		return toOptionalBytes(value)
	}

	/**
	 * Creates a sorted map that stores values ordered by associated keys.
	 * Keys are specified as tuples of 32-bit integers, floats, and strings.
	 * Keys are compared by the first element where they differ.
	 * If multiple values have the same key, a "uniquifier" is added to each key.
	 * The comparison order of uniquifiers is unspecified.
	 *
	 * @param name the name of the sorted map to create
	 */
	async sortedCreate(name: string): Promise<void> {
		await this.runCommand({sortedCreate: {name}}, voidResponseType)
	}
	/**
	 * Removes a sorted map from the database.
	 *
	 * @param name the name passed to `sortedCreate()`
	 */
	async sortedDrop(name: string): Promise<void> {
		await this.runCommand({sortedDrop: {name}}, voidResponseType)
	}
	/**
	 * Removes the value with the first matching key from a sorted map.
	 * Any prefix of a key will match it.
	 * Results in an error if no matching key is found.
	 * Example:
	 * ```js
	 * await client.sortedCreate('s')
	 * await client.sortedInsert('s', [{string: 'a'}, {int: 3}], new Uint8Array([1]))
	 * await client.sortedInsert('s', [{string: 'b'}, {int: 1}], new Uint8Array([2]))
	 * await client.sortedInsert('s', [{string: 'b'}, {int: 2}], new Uint8Array([3]))
	 * await client.sortedDelete('s', [{string: 'b'}])
	 * console.log(await client.sortedGet('s', []))
	 * // [
	 * //   { key: [ { string: 'a' }, { int: 3 } ], value: Uint8Array [ 1 ] },
	 * //   { key: [ { string: 'b' }, { int: 2 } ], value: Uint8Array [ 3 ] }
	 * // ]
	 * ```
	 *
	 * @param name the name passed to `sortedCreate()`
	 * @param key the key to search for in the sorted map
	 */
	async sortedDelete(name: string, key: KeyElement[]): Promise<void> {
		await this.runCommand({sortedDelete: {name, key}}, voidResponseType)
	}
	/**
	 * Retrieves all a sorted map's key-value pairs whose keys match a query key.
	 * Any prefix of a key will match it, e.g. `[]` will match all keys.
	 * Example:
	 * ```js
	 * await client.sortedCreate('s')
	 * await client.sortedInsert('s', [{string: 'a'}, {int: 3}], new Uint8Array([1]))
	 * await client.sortedInsert('s', [{string: 'b'}, {int: 1}], new Uint8Array([2]))
	 * await client.sortedInsert('s', [{string: 'b'}, {int: 2}], new Uint8Array([3]))
	 * console.log(await client.sortedGet('s', [{string: 'b'}]))
	 * // [
	 * //   { key: [ { string: 'b' }, { int: 1 } ], value: Uint8Array [ 2 ] },
	 * //   { key: [ { string: 'b' }, { int: 2 } ], value: Uint8Array [ 3 ] }
	 * // ]
	 * ```
	 *
	 * @param name the name passed to `sortedCreate()`
	 * @param key the key to search for in the sorted map
	 * @return the matching key-value pairs
	 */
	async sortedGet(name: string, key: KeyElement[]): Promise<SortedKeyValuePair[]> {
		const {pairs} = await this.runCommand(
			{sortedGet: {name, key}},
			sortedPairListResponseType
		)
		return pairs.pairs
	}
	/**
	 * Inserts a key-value pair into a sorted map.
	 * If the key matches an existing key-value pair,
	 * a uniquifier is added to both keys to disambiguate them.
	 * Different keys in a map may have different schemas
	 * but they must be prefixes of each other, e.g. `[int]` and `[int, string]`.
	 * The comparison order is unspecified if one key is a prefix of the other.
	 * Example:
	 * ```js
	 * await client.sortedCreate('s')
	 * await client.sortedInsert('s', [{float: 3.14}], new Uint8Array([1]))
	 * await client.sortedInsert('s', [{float: 3.14}], new Uint8Array([2]))
	 * console.log(await client.sortedGet('s', []))
	 * // [
	 * //   { key: [ { float: 3.14 }, { uniquifier: 1 } ], value: Uint8Array [ 2 ] },
	 * //   { key: [ { float: 3.14 }, { uniquifier: 0 } ], value: Uint8Array [ 1 ] }
	 * // ]
	 * ```
	 *
	 * @param name the name passed to `sortedCreate()`
	 * @param key the key to add to the sorted map
	 * @param value the value to add to the sorted map
	 */
	async sortedInsert(
		name: string, key: KeyElement[], value: ArrayBuffer | Uint8Array
	): Promise<void> {
		await this.runCommand(
			{sortedInsert: {name, key, value: toUint8Array(value)}},
			voidResponseType
		)
	}
	/**
	 * Gets the number of key-value pairs in a sorted map.
	 * Example:
	 * ```js
	 * await client.sortedCreate('s')
	 * await client.sortedInsert('s', [{string: 'a'}], new Uint8Array([1]))
	 * await client.sortedInsert('s', [{string: 'b'}], new Uint8Array([2]))
	 * await client.sortedInsert('s', [{string: 'c'}], new Uint8Array([3]))
	 * console.log(await client.sortedSize('s')) // 3
	 * ```
	 *
	 * @param name the name passed to `sortedCreate()`
	 * @return the size of the sorted map
	 */
	async sortedSize(name: string): Promise<number> {
		const {size} = await this.runCommand({sortedSize: {name}}, sizeResponseType)
		return size
	}
	/**
	 * Starts an iteration over the values in a sorted map.
	 * Start and end keys may be specified to iterate over a range of keys.
	 * Key-value pairs are returned from lowest key to highest.
	 * The sorted map cannot be modified while any iterations are active.
	 * Example:
	 * ```js
	 * await client.sortedCreate('s')
	 * await client.sortedInsert('s', [{string: 'c'}], new Uint8Array([3]))
	 * await client.sortedInsert('s', [{string: 'b'}], new Uint8Array([2]))
	 * await client.sortedInsert('s', [{string: 'a'}], new Uint8Array([1]))
	 * const iter = await client.sortedIter('s', [{string: 'b'}])
	 * console.log(await client.sortedIterNext(iter)) // { key: [ { string: 'b' } ], value: Uint8Array [ 2 ] }
	 * console.log(await client.sortedIterNext(iter)) // { key: [ { string: 'c' } ], value: Uint8Array [ 3 ] }
	 * console.log(await client.sortedIterNext(iter)) // null
	 * ```
	 *
	 * @param name the name passed to `sortedCreate()`
	 * @param start an optional (inclusive) minimum key to match
	 * @param end an optional (inclusive or exclusive) maximum key to match
	 * @param inclusive whether to include keys that match `end` (default false)
	 * @return an iteration key that refers to the iterator
	 */
	async sortedIter(
		name: string, start?: KeyElement[], end?: KeyElement[], inclusive = false
	): Promise<Uint8Array> {
		const {iter} = await this.runCommand(
			{sortedIter: {
				name,
				start: toOptionalKey(start),
				end: toOptionalKey(end),
				inclusive
			}},
			iterResponseType
		)
		return iter
	}
	/**
	 * Stops a sorted map iteration.
	 * This is only necessary if the iteration is ended early,
	 * i.e. before `sortedIterNext()` returns `null`.
	 *
	 * @param iter the iteration key returned from `sortedIter()`
	 */
	async sortedIterBreak(iter: Uint8Array): Promise<void> {
		await this.runCommand({sortedIterBreak: {iter}}, voidResponseType)
	}
	/**
	 * Retrieves the next key-value pair in a sorted map iteration,
	 * or `null` if the iteration has finished.
	 * The iteration is marked inactive as soon as it finishes.
	 *
	 * @param iter the iteration key returned from `sortedIter()`
	 * @return the next item in the sorted map
	 */
	async sortedIterNext(iter: Uint8Array): Promise<SortedKeyValuePair | null> {
		const {pair} = await this.runCommand(
			{sortedIterNext: {iter}},
			optionalSortedPairResponse
		)
		return pair || null
	}
}

export {CollectionType}
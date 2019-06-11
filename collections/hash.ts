import {createHash} from 'crypto'
import path = require('path')
import {addCollection, dropCollection, getCollections} from '.'
import {dataDir} from '../args'
import {
	copyWithinFile,
	createFile,
	FilePage,
	getFile,
	getPageCount,
	removeFile,
	setPageCount,
	setFileSegment
} from '../cache'
import {Iterators} from '../iterator'
import {
	Bucket,
	BUCKET_INDEX_BYTES,
	bucketIndexType,
	bucketType,
	HEADER_BYTES,
	Header,
	headerType
} from '../pb/hash'
import {CollectionType, KeyValuePair} from '../pb/interface'
import {ensureOverflowError} from '../util'

// Number of bits of hash to consider initially (only 1 bucket)
const INITIAL_DEPTH = 0
// crypto hashing algorithm to use, chosen because it's the fastest to compute
const HASH = 'sha1'

const filename = (name: string, fileType: string): string =>
	path.join(dataDir, `${name}.hash.${fileType}`)
const directoryFilename = (name: string): string => filename(name, 'directory')
const bucketsFilename = (name: string): string => filename(name, 'buckets')

const equal = (buffer1: Uint8Array, buffer2: Uint8Array): boolean =>
	!Buffer.from(buffer1.buffer, buffer1.byteOffset, buffer1.length)
		.compare(buffer2)

// Computes the unmasked hash of a key
function fullHash(key: Uint8Array): number {
	const {buffer, byteOffset, length} = createHash(HASH).update(key).digest()
	// Break the hash into 32-bit ints and xor them
	const hash = new Int32Array(buffer, byteOffset, length >> 2)
	let hash32 = 0
	for (const word of hash) hash32 ^= word
	return hash32
}
// Computes the bits of the hash used to find the bucket at a given depth
const depthHash = (hash: number, depth: number): number =>
	hash & ((1 << depth) - 1)

async function checkIsHash(name: string): Promise<void> {
	const collections = await getCollections
	const collection = collections[name]
	if (collection !== CollectionType.HASH) {
		throw new Error(`Collection ${name} is not a hash`)
	}
}

async function getHeader(name: string): Promise<Header> {
	const contents = await getFile(directoryFilename(name), 0, HEADER_BYTES)
	return headerType.toObject(headerType.decode(contents), {longs: Number})
}

function setHeader(name: string, header: Header): Promise<void> {
	const contents = headerType.encode(header).finish()
	return setFileSegment(directoryFilename(name), contents, 0, HEADER_BYTES)
}

const getBucket = (name: string, page: number): Promise<Bucket> =>
	new FilePage(bucketsFilename(name), page).use(async page =>
		bucketType.toObject(
			bucketType.decodeDelimited(new Uint8Array(page)),
			{defaults: true}
		)
	)

const setBucket = (name: string, page: number, bucket: Bucket): Promise<void> =>
	new FilePage(bucketsFilename(name), page).use(async page =>
		new Uint8Array(page).set(bucketType.encodeDelimited(bucket).finish())
	)

async function addBucket(name: string, page: number, bucket: Bucket): Promise<void> {
	await setPageCount(bucketsFilename(name), page + 1)
	await setBucket(name, page, bucket)
}

// Gets the position in the directory file of a given bucket index
const locateBucketIndex = (bucket: number): number =>
	HEADER_BYTES + bucket * BUCKET_INDEX_BYTES
// Gets the page of the buckets file storing a given bucket index
async function getBucketPage(name: string, bucket: number): Promise<number> {
	const offset = locateBucketIndex(bucket)
	const contents =
		await getFile(directoryFilename(name), offset, BUCKET_INDEX_BYTES)
	return bucketIndexType.toObject(bucketIndexType.decode(contents)).page
}
// Sets a given bucket index to point to a given page of the buckets file
const setBucketPage =
	(name: string, bucket: number, page: number): Promise<void> =>
		setFileSegment(
			directoryFilename(name),
			bucketIndexType.encode({page}).finish(),
			locateBucketIndex(bucket),
			BUCKET_INDEX_BYTES
		)

// Duplicates the directory, incrementing the depth
async function extendDirectory(name: string, header: Header): Promise<void> {
	const {depth} = header
	const indexBytes = BUCKET_INDEX_BYTES << depth
	await copyWithinFile(
		directoryFilename(name),
		HEADER_BYTES,
		indexBytes,
		HEADER_BYTES + indexBytes
	)
	header.depth = depth + 1
}

// Splits a bucket until it no longer overflows a page;
// return whether the global depth changed
async function splitBucket(
	name: string,
	index: number,
	bucketPage: number,
	{depth, items}: Bucket,
	header: Header
): Promise<boolean> {
	// Copy the keys and values because they are slices of the old page,
	// which will be overwritten
	for (const item of items) {
		item.key = item.key.slice()
		item.value = item.value.slice()
	}
	// May need to repeatedly split if keys happen to end up in the same half
	let splitAgain: boolean
	let depthChanged = false
	do {
		// Grow directory if necessary
		if (depth === header.depth) {
			await extendDirectory(name, header)
			depthChanged = true
		}

		// Split bucket
		const index1 = index | 1 << depth++
		const items0: KeyValuePair[] = [],
		      items1: KeyValuePair[] = []
		for (const item of items) {
			(depthHash(fullHash(item.key), depth) === index ? items0 : items1)
				.push(item)
		}
		splitAgain = false
		const makeNewBucket = async() => {
			// Add a page for the bucket to the end of the bucket file
			const newBucketPage = await getPageCount(bucketsFilename(name))
			const updatePromises = [
				addBucket(name, newBucketPage, {depth, items: items1})
					.catch(e => {
						ensureOverflowError(e)
						// istanbul ignore if
						if (splitAgain) throw new Error('Both buckets overflowed?')
						splitAgain = true
						index = index1
						bucketPage = newBucketPage
						items = items1
					})
			]
			// Update the 2 ** (header.depth - depth) bucket indices
			// that now point to the new bucket
			const bucketRepeatInterval = 1 << depth
			const maxBucketIndex = 1 << header.depth
			for (
				let bucket1 = index1;
				bucket1 < maxBucketIndex;
				bucket1 += bucketRepeatInterval
			) {
				updatePromises.push(setBucketPage(name, bucket1, newBucketPage))
			}
			await Promise.all(updatePromises)
		}
		await Promise.all([
			setBucket(name, bucketPage, {depth, items: items0})
				.catch(e => {
					ensureOverflowError(e)
					// istanbul ignore if
					if (splitAgain) throw new Error('Both buckets overflowed?')
					splitAgain = true
					items = items0
				}),
			makeNewBucket()
		])
	} while (splitAgain)
	return depthChanged
}

// Generates all the key-value pairs in the hash
async function* hashEntries(name: string): AsyncIterator<KeyValuePair> {
	const buckets = await getPageCount(bucketsFilename(name))
	for (let i = 0; i < buckets; i++) {
		const {items} = await getBucket(name, i)
		yield* items
	}
}
const iterators = new Iterators<AsyncIterator<KeyValuePair>>()

export async function create(name: string): Promise<void> {
	await addCollection(name, CollectionType.HASH)
	const initDirectory = async () => {
		const directoryFile = directoryFilename(name)
		await createFile(directoryFile)
		await setPageCount(directoryFile, 1)
		await Promise.all([
			setHeader(name, {depth: INITIAL_DEPTH, size: 0}),
			setBucketPage(name, 0, 0)
		])
	}
	const initBucket = async () => {
		await createFile(bucketsFilename(name))
		await addBucket(name, 0, {depth: INITIAL_DEPTH, items: []})
	}
	await Promise.all([initDirectory(), initBucket()])
}

export async function drop(name: string): Promise<void> {
	await checkIsHash(name)
	iterators.checkNoIterators(name)
	await Promise.all([
		dropCollection(name),
		removeFile(directoryFilename(name)),
		removeFile(bucketsFilename(name))
	])
}

// "delete" is a reserved name, so we use "remove" instead
export async function remove(name: string, key: Uint8Array): Promise<void> {
	await checkIsHash(name)
	iterators.checkNoIterators(name)
	const header = await getHeader(name)
	const bucketIndex = depthHash(fullHash(key), header.depth)
	const bucketPage = await getBucketPage(name, bucketIndex)
	const bucket = await getBucket(name, bucketPage)
	const {items} = bucket
	for (let i = 0; i < items.length; i++) {
		if (equal(items[i].key, key)) {
			items.splice(i, 1)
			header.size--
			await Promise.all([
				setBucket(name, bucketPage, bucket),
				setHeader(name, header)
			])
			break
		}
	}
}

export async function get(name: string, key: Uint8Array): Promise<Uint8Array | null> {
	await checkIsHash(name)
	const {depth} = await getHeader(name)
	const bucketIndex = depthHash(fullHash(key), depth)
	const {items} = await getBucket(name, await getBucketPage(name, bucketIndex))
	for (const item of items) {
		if (equal(item.key, key)) return item.value
	}
	return null
}

export async function set(
	name: string, key: Uint8Array, value: Uint8Array
): Promise<void> {
	await checkIsHash(name)
	iterators.checkNoIterators(name)
	const header = await getHeader(name)
	const hash = fullHash(key)
	const bucketIndex = depthHash(hash, header.depth)
	const bucketPage = await getBucketPage(name, bucketIndex)
	const bucket = await getBucket(name, bucketPage)
	// Update value corresponding to key, or add new key-value pair
	const {items} = bucket
	let newKey = true
	for (const item of items) {
		if (equal(item.key, key)) {
			item.value = value
			newKey = false
			break
		}
	}
	if (newKey) {
		items.push({key, value})
		header.size++
	}

	let depthChanged = false
	try {
		await setBucket(name, bucketPage, bucket)
	}
	catch (e) { // bucket is full
		ensureOverflowError(e)
		depthChanged = await splitBucket(
			name, depthHash(hash, bucket.depth), bucketPage, bucket, header
		)
	}
	// Only write header if it was modified
	if (newKey || depthChanged) await setHeader(name, header)
}

export async function size(name: string): Promise<number> {
	await checkIsHash(name)
	const {size} = await getHeader(name)
	return size
}

export async function iter(name: string): Promise<Uint8Array> {
	await checkIsHash(name)
	return iterators.registerIterator(name, hashEntries(name))
}

export const iterBreak = (iter: Uint8Array): void =>
	iterators.closeIterator(iter)

export async function iterNext(iter: Uint8Array): Promise<KeyValuePair | null> {
	const iterator = iterators.getIterator(iter)
	const {value, done} = await iterator.next()
	if (done) {
		iterators.closeIterator(iter)
		return null
	}
	return value
}
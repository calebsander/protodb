import {createHash, randomBytes} from 'crypto'
import * as path from 'path'
import {promisify} from 'util'
import {addCollection, dropCollection, getCollections} from '.'
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
import {DATA_DIR} from '../constants'
import {
	Bucket,
	BucketItem,
	BUCKET_INDEX_BYTES,
	bucketIndexType,
	bucketType,
	HEADER_BYTES,
	Header,
	headerType
} from '../pb/hash'
import {ITER_BYTE_LENGTH} from '../sb-types/request'
import {toArrayBuffer} from '../util'

const COLLECTION_TYPE = 'hash'
const INITIAL_DEPTH = 0

const randomBytesPromise = promisify(randomBytes)

const filename = (name: string, fileType: string) =>
	path.join(DATA_DIR, `${name}.${COLLECTION_TYPE}.${fileType}`)
const directoryFilename = (name: string) => filename(name, 'directory')
const bucketsFilename = (name: string) => filename(name, 'buckets')

const equal = (buffer1: Uint8Array, buffer2: Uint8Array): boolean =>
	!Buffer.from(buffer1).compare(buffer2)

function fullHash(key: Uint8Array): number {
	const hash =
		new Int32Array(toArrayBuffer(createHash('md5').update(key).digest()))
	let hash32 = 0
	for (const word of hash) hash32 ^= word
	return hash32
}
const depthHash = (hash: number, depth: number): number =>
	hash & ((1 << depth) - 1)

async function checkIsHash(name: string): Promise<void> {
	const collections = await getCollections
	const collection = collections[name]
	if (!(collection && COLLECTION_TYPE in collection)) {
		throw new Error(`Collection ${name} is not a hash`)
	}
}

async function getHeader(name: string): Promise<Header> {
	const contents = await getFile(directoryFilename(name), 0, HEADER_BYTES)
	return headerType.toObject(headerType.decode(contents), {longs: Number})
}

function setHeader(name: string, header: Header): Promise<void> {
	const contents = headerType.encode(headerType.fromObject(header)).finish()
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
		new Uint8Array(page).set(
			bucketType.encodeDelimited(bucketType.fromObject(bucket)).finish()
		)
	)

async function addBucket(name: string, page: number, bucket: Bucket): Promise<void> {
	await setPageCount(bucketsFilename(name), page + 1)
	await setBucket(name, page, bucket)
}

const locateBucketIndex = (bucket: number): number =>
	HEADER_BYTES + bucket * BUCKET_INDEX_BYTES
async function getBucketPage(name: string, bucket: number): Promise<number> {
	const offset = locateBucketIndex(bucket)
	const contents =
		await getFile(directoryFilename(name), offset, BUCKET_INDEX_BYTES)
	return bucketIndexType.toObject(bucketIndexType.decode(contents)).page
}
const setBucketPage = (name: string, bucket: number, page: number): Promise<void> =>
	setFileSegment(
		directoryFilename(name),
		bucketIndexType.encode(bucketIndexType.fromObject({page})).finish(),
		locateBucketIndex(bucket),
		BUCKET_INDEX_BYTES
	)

async function extendDirectory(name: string, header: Header): Promise<void> {
	const {depth} = header
	const indexBytes = BUCKET_INDEX_BYTES << depth
	const doubleDirectory = copyWithinFile(
		directoryFilename(name),
		HEADER_BYTES,
		indexBytes,
		HEADER_BYTES + indexBytes
	)
	header.depth = depth + 1
	await Promise.all([doubleDirectory, setHeader(name, header)])
}

interface HashIterator {
	name: string
	iterator: AsyncIterator<BucketItem>
}

async function* hashEntries(name: string): AsyncIterator<BucketItem> {
	const buckets = await getPageCount(bucketsFilename(name))
	for (let i = 0; i < buckets; i++) {
		const {items} = await getBucket(name, i)
		yield* items
	}
}

const iterators = new Map<string, HashIterator>()
const iteratorCounts = new Map<string, number>()

function checkNoIterators(name: string) {
	if (iteratorCounts.has(name)) {
		throw new Error(`Hash ${name} has active iterators`)
	}
}

function getIterator(iter: number[]): {key: string, iterator: HashIterator} {
	const key = Buffer.from(iter).toString('hex')
	const iterator = iterators.get(key)
	if (!iterator) throw new Error('Unknown iterator')
	return {key, iterator}
}

function iterClose(key: string, name: string): void {
	iterators.delete(key)
	const oldCount = iteratorCounts.get(name)
	if (oldCount === undefined) throw new Error('Hash has no iterators?')
	if (oldCount > 1) iteratorCounts.set(name, oldCount - 1)
	else iteratorCounts.delete(name)
}

export async function create(name: string): Promise<void> {
	await addCollection(name, {[COLLECTION_TYPE]: {}})
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
	checkNoIterators(name)
	await Promise.all([
		dropCollection(name),
		removeFile(directoryFilename(name)),
		removeFile(bucketsFilename(name))
	])
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
	checkNoIterators(name)
	const header = await getHeader(name)
	const hash = fullHash(key)
	const bucketIndex = depthHash(hash, header.depth)
	const bucketPage = await getBucketPage(name, bucketIndex)
	const bucket = await getBucket(name, bucketPage)
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
		await setHeader(name, header)
	}

	// TODO: what if hash needs to be resized multiple times in a row?
	// (This is very unlikely if each bucket stores ~100 items)
	try {
		await setBucket(name, bucketPage, bucket)
	}
	catch (e) {
		// Bucket is full
		if (!(e instanceof RangeError && e.message === 'Source is too large')) {
			throw e // unexpected error; rethrow it
		}

		// Grow directory if necessary
		const oldDepth = bucket.depth
		if (oldDepth === header.depth) await extendDirectory(name, header)

		// Split bucket
		const newDepth = oldDepth + 1
		const items0: BucketItem[] = [],
		      items1: BucketItem[] = []
		for (const item of items) {
			const hash = fullHash(item.key)
			const newItems = depthHash(hash, oldDepth) === depthHash(hash, newDepth)
				? items0
				: items1
			newItems.push(item)
		}
		// Add a page for the bucket to the end of the bucket file
		const newBucketPage = await getPageCount(bucketsFilename(name))
		const updatePromises = [(async () => {
			await addBucket(name, newBucketPage, {depth: newDepth, items: items1})
			// Need to wait to overwrite the old bucket until the new bucket is written
			// because the items are slices of the old page
			await setBucket(name, bucketPage, {depth: newDepth, items: items0})
		})()]
		// Update the 2 ** (header.depth - newDepth) bucket indices
		// that now point to the new bucket
		const bucketRepeatInterval = 1 << newDepth
		const maxBucketIndex = 1 << header.depth
		for (
			let bucket1 = depthHash(hash, oldDepth) | 1 << oldDepth;
			bucket1 < maxBucketIndex;
			bucket1 += bucketRepeatInterval
		) {
			updatePromises.push(setBucketPage(name, bucket1, newBucketPage))
		}
		await Promise.all(updatePromises)
	}
}

export async function remove(name: string, key: Uint8Array): Promise<void> {
	await checkIsHash(name)
	checkNoIterators(name)
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

export async function size(name: string): Promise<number> {
	await checkIsHash(name)
	const {size} = await getHeader(name)
	return size
}

export async function iter(name: string): Promise<number[]> {
	await checkIsHash(name)
	iteratorCounts.set(name, (iteratorCounts.get(name) || 0) + 1)
	const iter = await randomBytesPromise(ITER_BYTE_LENGTH)
	const iterKey = iter.toString('hex')
	iterators.set(iterKey, {name, iterator: hashEntries(name)})
	return [...iter]
}

export async function iterNext(iter: number[]): Promise<BucketItem | null> {
	const {key, iterator: {iterator, name}} = getIterator(iter)
	const {value, done} = await iterator.next()
	if (done) {
		iterClose(key, name)
		return null
	}
	return value
}

export function iterBreak(iter: number[]): void {
	const {key, iterator: {name}} = getIterator(iter)
	iterClose(key, name)
}
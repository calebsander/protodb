import {createHash} from 'crypto'
import * as path from 'path'
import {addCollection, dropCollection, getCollections} from '.'
import {
	createFile,
	FilePage,
	getPageCount,
	getPageNo,
	getPageOffset,
	PAGE_SIZE,
	setPageCount,
	removeFile
} from '../cache'
import {DATA_DIR} from '../constants'
import {
	Bucket,
	BucketItem,
	BUCKET_INDEX_BYTES,
	bucketIndexType,
	bucketType,
	Header,
	headerType
} from '../sb-types/hash'
import {toArrayBuffer} from '../util'

const COLLECTION_TYPE = 'hash'
const HEADER_PAGE = 0
const DIRECTORY_START_PAGE = 1
const INITIAL_DEPTH = 0

const filename = (name: string, fileType: string) =>
	path.join(DATA_DIR, `${name}.${COLLECTION_TYPE}.${fileType}`)
const directoryFilename = (name: string) => filename(name, 'directory')
const bucketsFilename = (name: string) => filename(name, 'buckets')

const equal = (buffer1: ArrayBuffer, buffer2: ArrayBuffer): boolean =>
	!Buffer.from(buffer1).compare(new Uint8Array(buffer2))

function fullHash(key: ArrayBuffer): number {
	const hash = new Int32Array(toArrayBuffer(
		createHash('md5').update(new Uint8Array(key)).digest()
	))
	let hash32 = 0
	for (const word of hash) hash32 ^= word
	return hash32
}
const depthHash = (hash: number, depth: number): number =>
	hash & ((1 << depth) - 1)

async function checkIsHash(name: string): Promise<void> {
	const collections = await getCollections()
	const collection = collections.get(name)
	if (!(collection && collection.type === COLLECTION_TYPE)) {
		throw new Error(`Collection ${name} is not a hash`)
	}
}

function getHeader(name: string): Promise<Header> {
	return new FilePage(directoryFilename(name), HEADER_PAGE).use(async page =>
		headerType.consumeValue(page, 0).value
	)
}

function setHeader(name: string, header: Header): Promise<void> {
	return new FilePage(directoryFilename(name), HEADER_PAGE).use(async page =>
		new Uint8Array(page).set(new Uint8Array(headerType.valueBuffer(header)))
	)
}

async function addBucket(name: string, page: number, bucket: Bucket): Promise<void> {
	const bucketsFile = bucketsFilename(name)
	await setPageCount(bucketsFile, page + 1)
	return new FilePage(bucketsFile, page).use(async page =>
		new Uint8Array(page).set(new Uint8Array(bucketType.valueBuffer(bucket)))
	)
}

function locateBucketIndex(bucket: number): {page: number, offset: number} {
	const bucketIndexByte = bucket * BUCKET_INDEX_BYTES
	return {
		page: DIRECTORY_START_PAGE + getPageNo(bucketIndexByte),
		offset: getPageOffset(bucketIndexByte)
	}
}
function getBucketPage(name: string, bucket: number): Promise<number> {
	const {page, offset} = locateBucketIndex(bucket)
	return new FilePage(directoryFilename(name), page).use(async page =>
		bucketIndexType.consumeValue(page, offset).value
	)
}
function setBucketPage(name: string, bucket: number, bucketPage: number): Promise<void> {
	const {page, offset} = locateBucketIndex(bucket)
	return new FilePage(directoryFilename(name), page).use(async page =>
		new Uint8Array(page, offset)
			.set(new Uint8Array(bucketIndexType.valueBuffer(bucketPage)))
	)
}

function getBucket(name: string, page: number): Promise<Bucket> {
	return new FilePage(bucketsFilename(name), page).use(async page =>
		bucketType.consumeValue(page, 0).value
	)
}

function setBucket(name: string, page: number, bucket: Bucket): Promise<void> {
	return new FilePage(bucketsFilename(name), page).use(async page =>
		new Uint8Array(page).set(new Uint8Array(bucketType.valueBuffer(bucket)))
	)
}

async function extendDirectory(name: string, header: Header): Promise<void> {
	const {depth} = header
	const doubleDirectory = async () => {
		const directoryFile = directoryFilename(name)
		const bucketIndexBytes = BUCKET_INDEX_BYTES << depth
		// Since bucketIndexBytes and PAGE_SIZE are powers of 2,
		// we are either copying within the first page, or duplicating whole pages
		if (bucketIndexBytes < PAGE_SIZE) {
			await new FilePage(directoryFile, DIRECTORY_START_PAGE)
				.use(async page =>
					new Uint8Array(page, bucketIndexBytes)
						.set(new Uint8Array(page, 0, bucketIndexBytes))
				)
		}
		else {
			const copyPages = getPageNo(bucketIndexBytes)
			await setPageCount(directoryFile, DIRECTORY_START_PAGE + (copyPages << 1))
			const copyPromises: Promise<void>[] = []
			for (let pageNo = 0; pageNo < copyPages; pageNo++) {
				const sourcePage = DIRECTORY_START_PAGE + pageNo
				copyPromises.push(
					new FilePage(directoryFile, sourcePage).use(async page =>
						new FilePage(directoryFile, sourcePage + copyPages).use(async newPage =>
							new Uint8Array(newPage).set(new Uint8Array(page))
						)
					)
				)
			}
			await Promise.all(copyPromises)
		}
	}
	header.depth++
	await Promise.all([doubleDirectory(), setHeader(name, header)])
}

export async function create(name: string): Promise<void> {
	await addCollection(name, {type: COLLECTION_TYPE})
	const initDirectory = async () => {
		const directoryFile = directoryFilename(name)
		await createFile(directoryFile)
		await setPageCount(directoryFile, 2)
		await Promise.all([
			setHeader(name, {depth: INITIAL_DEPTH, size: 0}),
			new FilePage(directoryFile, DIRECTORY_START_PAGE).use(async page =>
				new Uint8Array(page).set(new Uint8Array(bucketIndexType.valueBuffer(0)))
			)
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
	await Promise.all([
		dropCollection(name),
		removeFile(directoryFilename(name)),
		removeFile(bucketsFilename(name))
	])
}

export async function get(name: string, key: ArrayBuffer): Promise<ArrayBuffer | null> {
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
	name: string, key: ArrayBuffer, value: ArrayBuffer
): Promise<void> {
	await checkIsHash(name)
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
			const newItems = depthHash(hash, oldDepth) == depthHash(hash, newDepth)
				? items0
				: items1
			newItems.push(item)
		}
		const makeNewBucket = async () => {
			// Add a page for the bucket to the end of the bucket file
			const newBucketPage = await getPageCount(bucketsFilename(name))
			const updatePromises =
				[addBucket(name, newBucketPage, {depth: newDepth, items: items1})]
			// Update the 2 ** (header.depth - newDepth) bucket indices
			// that now point to the new bucket
			const bucketRepeatInterval = 1 << newDepth
			const maxBucketIndex = 1 << header.depth
			for (
				let bucket1 = depthHash(bucketIndex, oldDepth) | 1 << oldDepth;
				bucket1 < maxBucketIndex;
				bucket1 += bucketRepeatInterval
			) {
				updatePromises.push(setBucketPage(name, bucket1, newBucketPage))
			}
			await Promise.all(updatePromises)
		}
		await Promise.all([
			setBucket(name, bucketPage, {depth: newDepth, items: items0}),
			makeNewBucket()
		])
	}
}

export async function remove(name: string, key: ArrayBuffer): Promise<void> {
	await checkIsHash(name)
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
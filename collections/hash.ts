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
	removeFile
} from '../cache'
import {DATA_DIR} from '../constants'
import {
	Bucket,
	BucketItem,
	BUCKET_INDEX_BYTES,
	bucketIndexType,
	bucketType,
	depthType
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

function getDepth(name: string): Promise<number> {
	return new FilePage(directoryFilename(name), HEADER_PAGE).use(async page =>
		depthType.consumeValue(page, 0).value
	)
}

function setDepth(name: string, depth: number, create?: true): Promise<void> {
	return new FilePage(directoryFilename(name), HEADER_PAGE).use(async page => {
		new Uint8Array(page).set(new Uint8Array(depthType.valueBuffer(depth)))
		page.dirty = true
	}, create)
}

function addBucket(name: string, page: number, bucket: Bucket): Promise<void> {
	return new FilePage(bucketsFilename(name), page).create(async page =>
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
	return new FilePage(directoryFilename(name), page).use(async page => {
		new Uint8Array(page, offset)
			.set(new Uint8Array(bucketIndexType.valueBuffer(bucketPage)))
		page.dirty = true
	})
}

function getBucket(name: string, page: number): Promise<Bucket> {
	return new FilePage(bucketsFilename(name), page).use(async page =>
		bucketType.consumeValue(page, 0).value
	)
}

function setBucket(name: string, page: number, bucket: Bucket): Promise<void> {
	return new FilePage(bucketsFilename(name), page).use(async page => {
		new Uint8Array(page).set(new Uint8Array(bucketType.valueBuffer(bucket)))
		page.dirty = true
	})
}

async function extendDirectory(name: string, depth: number): Promise<void> {
	const doubleDirectory = async () => {
		const directoryFile = directoryFilename(name)
		const bucketIndexBytes = BUCKET_INDEX_BYTES << depth
		// Since bucketIndexBytes and PAGE_SIZE are powers of 2,
		// we are either copying within the first page, or duplicating whole pages
		if (bucketIndexBytes < PAGE_SIZE) {
			await new FilePage(directoryFile, DIRECTORY_START_PAGE)
				.use(async page => {
					new Uint8Array(page, bucketIndexBytes)
						.set(new Uint8Array(page, 0, bucketIndexBytes))
					page.dirty = true
				})
		}
		else {
			const copyPages = getPageNo(bucketIndexBytes)
			const copyPromises: Promise<void>[] = []
			for (let pageNo = 0; pageNo < copyPages; pageNo++) {
				const sourcePage = DIRECTORY_START_PAGE + pageNo
				copyPromises.push(
					new FilePage(directoryFile, sourcePage).use(async page =>
						new FilePage(directoryFile, sourcePage + copyPages)
							.create(async newPage =>
								new Uint8Array(newPage).set(new Uint8Array(page))
							)
					)
				)
			}
			await Promise.all(copyPromises)
		}
	}
	await Promise.all([doubleDirectory(), setDepth(name, depth + 1)])
}

export async function create(name: string): Promise<void> {
	await addCollection(name, {type: COLLECTION_TYPE})
	const initDirectory = async () => {
		const directoryFile = directoryFilename(name)
		await createFile(directoryFile)
		await Promise.all([
			setDepth(name, INITIAL_DEPTH, true),
			new FilePage(directoryFile, DIRECTORY_START_PAGE).create(async page =>
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
	const bucketIndex = depthHash(fullHash(key), await getDepth(name))
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
	let depth = await getDepth(name)
	const hash = fullHash(key)
	const bucketIndex = depthHash(hash, depth)
	const bucketPage = await getBucketPage(name, bucketIndex)
	const bucket = await getBucket(name, bucketPage)
	const {items} = bucket
	let oldKey = false
	for (const item of items) {
		if (equal(item.key, key)) {
			item.value = value
			oldKey = true
			break
		}
	}
	if (!oldKey) items.push({key, value})

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
		if (oldDepth === depth) {
			await extendDirectory(name, depth)
			depth++
		}

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
			// Update all 2 ** (depth - newDepth) bucket indices to point to the new bucket
			const bucketRepeatInterval = 1 << newDepth
			const maxBucketIndex = 1 << depth
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
import {createHash} from 'crypto'
import * as path from 'path'
import {addCollection, dropCollection, getCollections} from '.'
import {createFile, FilePage, getPageNo, getPageOffset, removeFile} from '../cache'
import {DATA_DIR} from '../constants'
import {
	Bucket,
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

function addBucket(name: string, page: number, depth: number): Promise<void> {
	return new FilePage(bucketsFilename(name), page).create(async page =>
		new Uint8Array(page).set(new Uint8Array(
			bucketType.valueBuffer({depth, items: []})
		))
	)
}

function setBucket(name: string, page: number, bucket: Bucket): Promise<void> {
	return new FilePage(bucketsFilename(name), page).use(async page => {
		new Uint8Array(page).set(new Uint8Array(bucketType.valueBuffer(bucket)))
		page.dirty = true
	})
}

async function getBucket(name: string, bucket: number): Promise<Bucket> {
	const bucketIndexByte = bucket * BUCKET_INDEX_BYTES
	const bucketIndexPage = DIRECTORY_START_PAGE + getPageNo(bucketIndexByte)
	const bucketPage = await new FilePage(directoryFilename(name), bucketIndexPage)
		.use(async page =>
			bucketIndexType.consumeValue(page, getPageOffset(bucketIndexByte)).value
		)
	return new FilePage(bucketsFilename(name), bucketPage).use(async page =>
		bucketType.consumeValue(page, 0).value
	)
}

export async function create(name: string): Promise<void> {
	await addCollection(name, {type: COLLECTION_TYPE})
	const initDirectory = async () => {
		const directoryFile = directoryFilename(name)
		await createFile(directoryFile)
		await Promise.all([
			new FilePage(directoryFile, HEADER_PAGE).create(async page =>
				new Uint8Array(page).set(
					new Uint8Array(depthType.valueBuffer(INITIAL_DEPTH))
				)
			),
			new FilePage(directoryFile, DIRECTORY_START_PAGE).create(async page =>
				new Uint8Array(page).set(new Uint8Array(bucketIndexType.valueBuffer(0)))
			)
		])
	}
	const initBucket = async () => {
		await createFile(bucketsFilename(name))
		await addBucket(name, 0, INITIAL_DEPTH)
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
	const {items} = await getBucket(name, bucketIndex)
	for (const item of items) {
		if (equal(item.key, key)) return item.value
	}
	return null
}

export async function set(
	name: string, key: ArrayBuffer, value: ArrayBuffer
): Promise<void> {
	await checkIsHash(name)
	const depth = await getDepth(name)
	const hash = fullHash(key)
	const bucketIndex = depthHash(hash, depth)
	const bucket = await getBucket(name, bucketIndex)
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
	try {
		await setBucket(name, bucketIndex, bucket)
	}
	catch (e) {
		// TODO: bucket is full, so split it
		throw e
	}
}
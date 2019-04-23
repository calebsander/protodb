import * as path from 'path'
import {addCollection, dropCollection, getCollections} from '.'
import {createFile, FilePage, removeFile, setFile} from '../cache'
import {DATA_DIR} from '../constants'
import {Schema} from '../sb-types/common'
import {depthType, bucketIndexType, bucketType} from '../sb-types/hash'

const COLLECTION_TYPE = 'hash'
const HEADER_PAGE = 0
const DIRECTORY_START_PAGE = 1
const INITIAL_DEPTH = 0

const filename = (name: string, fileType: string) =>
	path.join(DATA_DIR, `${name}.${COLLECTION_TYPE}.${fileType}`)
const directoryFilename = (name: string) => filename(name, 'directory')
const bucketsFilename = (name: string) => filename(name, 'buckets')

async function checkIsHash(name: string): Promise<void> {
	const collections = await getCollections()
	const collection = collections.get(name)
	if (!(collection && collection.type === COLLECTION_TYPE)) {
		throw new Error(`Collection ${name} is not a hash`)
	}
}

export async function create(
	name: string, keySchema: Schema, valueSchema: Schema
): Promise<void> {
	await addCollection(name, {type: COLLECTION_TYPE, keySchema, valueSchema})
	await Promise.all([
		(async () => {
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
		})(),
		setFile(
			bucketsFilename(name),
			bucketType.valueBuffer({depth: INITIAL_DEPTH, items: []})
		)
	])
}

export async function drop(name: string): Promise<void> {
	await checkIsHash(name)
	await Promise.all([
		dropCollection(name),
		removeFile(directoryFilename(name)),
		removeFile(bucketsFilename(name))
	])
}
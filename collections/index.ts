import * as path from 'path'
import {DATA_DIR} from '../constants'
import {getFile, setFile} from '../cache'
import {CollectionType, Collections, dbType} from '../pb/db'
import {toArrayBuffer} from '../util'

const DB_FILE = path.join(DATA_DIR, 'db')

export const getCollections = (async (): Promise<Collections> => {
	try {
		const dbFile = await getFile(DB_FILE)
		return dbType.toObject(dbType.decode(new Uint8Array(dbFile))).collections
	}
	catch (e) {
		return {}
	}
})()
async function saveCollections(): Promise<void> {
	const collections = await getCollections
	await setFile(DB_FILE, toArrayBuffer(
		dbType.encode(dbType.fromObject({collections})).finish()
	))
}
export async function addCollection(
	name: string, collection: CollectionType
): Promise<void> {
	const collections = await getCollections
	if (name in collections) {
		throw new Error(`Collection ${name} already exists`)
	}
	collections[name] = collection
	await saveCollections()
}
export async function dropCollection(name: string): Promise<void> {
	const collections = await getCollections
	if (!(name in collections)) {
		throw new Error(`Collection ${name} does not exist`)
	}
	delete collections[name]
	await saveCollections()
}
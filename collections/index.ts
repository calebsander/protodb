import * as path from 'path'
import {DATA_DIR} from '../constants'
import {getFile, setFile} from '../cache'
import {CollectionType, Collections, dbType} from '../pb/db'

const DB_FILE = path.join(DATA_DIR, 'db')

export const getCollections = getFile(DB_FILE)
	.then(file => dbType.toObject(dbType.decode(file)).collections)
	.catch<Collections>(_ => ({}))
async function saveCollections(): Promise<void> {
	const collections = await getCollections
	await setFile(DB_FILE, dbType.encode(dbType.fromObject({collections})).finish())
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
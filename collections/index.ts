import * as path from 'path'
import {DATA_DIR} from '../constants'
import {getFile, setFile} from '../cache'
import {CollectionType, Collections, dbType} from '../pb/db'

const DB_FILE = path.join(DATA_DIR, 'db')

export const getCollections = getFile(DB_FILE)
	.then(contents => {
		const message = dbType.decodeDelimited(contents)
		return dbType.toObject(message, {defaults: true}).collections
	})
	.catch<Collections>(_ => ({}))
async function saveCollections(): Promise<void> {
	const collections = await getCollections
	const message = dbType.fromObject({collections})
	await setFile(DB_FILE, dbType.encodeDelimited(message).finish())
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
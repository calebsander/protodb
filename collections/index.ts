import * as path from 'path'
import {DATA_DIR} from '../constants'
import {getFile, setFile} from '../cache'
import {CollectionType, Collections, dbType} from '../sb-types/db'

const DB_FILE = path.join(DATA_DIR, 'db')

let cachedCollections: Collections | undefined
export async function getCollections(): Promise<Collections> {
	if (cachedCollections) return cachedCollections

	let collections: Collections
	try {
		({collections} = dbType.consumeValue(await getFile(DB_FILE), 0).value)
	}
	catch (e) {
		collections = new Map
	}
	// Collections may have been updated in the meantime
	if (!cachedCollections) cachedCollections = collections
	return cachedCollections
}
function saveCollections(): Promise<void> {
	return setFile(DB_FILE, dbType.valueBuffer({collections: cachedCollections!}))
}
export async function addCollection(name: string, collection: CollectionType): Promise<void> {
	if (cachedCollections!.has(name)) {
		throw new Error(`Collection ${name} already exists`)
	}
	cachedCollections!.set(name, collection)
	return saveCollections()
}
export function dropCollection(name: string) {
	if (!cachedCollections!.has(name)) {
		throw new Error(`Collection ${name} does not exist`)
	}
	cachedCollections!.delete(name)
	return saveCollections()
}
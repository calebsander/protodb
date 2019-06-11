import path = require('path')
import {dataDir} from '../args'
import {getFile, setFile} from '../cache'
import {dbType} from '../pb/db'
import {Collections, CollectionType} from '../pb/interface'

const DB_FILE = path.join(dataDir, 'db')

// Collections are loaded once at startup, then read from an in-memory object
const loadCollections = getFile(DB_FILE)
	.then(contents => {
		const message = dbType.decodeDelimited(contents)
		return dbType.toObject(message, {defaults: true}).collections
	})
	.catch<Collections>(_ => ({})) // if file isn't created, no collections exist
export const getCollections: Promise<Partial<Collections>> = loadCollections

async function saveCollections(): Promise<void> {
	const collections = await loadCollections
	await setFile(DB_FILE, dbType.encodeDelimited({collections}).finish())
}

// Registers a new collection on the database
export async function addCollection(
	name: string, collection: CollectionType
): Promise<void> {
	const collections = await loadCollections
	if (name in collections) {
		throw new Error(`Collection ${name} already exists`)
	}
	collections[name] = collection
	await saveCollections()
}

// Removes a collection from the database
export async function dropCollection(name: string): Promise<void> {
	const collections = await loadCollections
	// istanbul ignore if
	if (!(name in collections)) {
		throw new Error(`Collection ${name} does not exist`)
	}
	delete collections[name]
	await saveCollections()
}
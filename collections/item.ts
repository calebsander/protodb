import * as path from 'path'
import {addCollection, dropCollection, getCollections} from '.'
import {getFile, removeFile, setFile} from '../cache'
import {DATA_DIR} from '../constants'
import {itemType} from '../sb-types/item'

const COLLECTION_TYPE = 'item'

const filename = (name: string) =>
	path.join(DATA_DIR, `${name}.${COLLECTION_TYPE}`)

async function checkIsItem(name: string): Promise<void> {
	const collections = await getCollections
	const collection = collections[name]
	if (!(collection && COLLECTION_TYPE in collection)) {
		throw new Error(`Collection ${name} is not an item`)
	}
}

export function create(name: string): Promise<void> {
	return addCollection(name, {[COLLECTION_TYPE]: {}})
}

export async function drop(name: string): Promise<void> {
	await checkIsItem(name)
	await Promise.all([
		dropCollection(name),
		removeFile(filename(name))
			.catch(_ => {}) // not a problem if the item was never set
	])
}

export async function get(name: string): Promise<ArrayBuffer> {
	await checkIsItem(name)
	try {
		return itemType.consumeValue(await getFile(filename(name)), 0).value
	}
	catch {
		throw new Error(`Collection ${name} has not been set`)
	}
}

export async function set(name: string, value: ArrayBuffer): Promise<void> {
	await checkIsItem(name)
	await setFile(filename(name), itemType.valueBuffer(value))
}
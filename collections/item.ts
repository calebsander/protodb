import * as path from 'path'
import {addCollection, dropCollection, getCollections} from '.'
import {getFile, removeFile, setFile} from '../cache'
import {DATA_DIR} from '../constants'
import {itemType} from '../pb/item'

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

export async function get(name: string): Promise<Uint8Array> {
	await checkIsItem(name)
	let contents: Uint8Array
	try {
		contents = await getFile(filename(name))
	}
	catch {
		throw new Error(`Collection ${name} has not been set`)
	}
	return itemType.toObject(itemType.decodeDelimited(contents)).value
}

export async function set(name: string, value: Uint8Array): Promise<void> {
	await checkIsItem(name)
	const writer = itemType.encodeDelimited(itemType.fromObject({value}))
	await setFile(filename(name), writer.finish())
}
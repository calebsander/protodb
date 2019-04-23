import * as path from 'path'
import {addCollection, dropCollection, getCollections} from '.'
import {getFile, removeFile, setFile} from '../cache'
import {DATA_DIR} from '../constants'
import {Schema} from '../sb-types/common'

const COLLECTION_TYPE = 'item'

const filename = (name: string) =>
	path.join(DATA_DIR, `${name}.${COLLECTION_TYPE}`)

async function checkIsItem(name: string): Promise<void> {
	const collections = await getCollections()
	const collection = collections.get(name)
	if (!(collection && collection.type === COLLECTION_TYPE)) {
		throw new Error(`Collection ${name} is not an item`)
	}
}

export function create(name: string, schema: Schema): Promise<void> {
	return addCollection(name, {type: COLLECTION_TYPE, schema})
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
		// TODO: trim file
		return await getFile(filename(name))
	}
	catch {
		throw new Error(`Collection ${name} has not been set`)
	}
}

export async function set(name: string, value: ArrayBuffer): Promise<void> {
	await checkIsItem(name)
	await setFile(filename(name), value)
}
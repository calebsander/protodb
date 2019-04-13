import * as path from 'path'
import {addCollection, dropCollection} from '.'
import {createFile, removeFile, setFile} from '../cache'
import {DATA_DIR} from '../constants'
import {Schema} from '../sb-types/common'
import {itemValueType} from '../sb-types/item'

const filename = (name: string) => path.join(DATA_DIR, `${name}.item`)

export async function create(name: string, schema: Schema): Promise<void> {
	await addCollection(name, {type: 'item', schema})

	const file = filename(name)
	await createFile(file)
	await setFile(file, itemValueType.valueBuffer({value: null}))
}

export async function drop(name: string): Promise<void> {
	await dropCollection(name)

	await removeFile(filename(name))
}
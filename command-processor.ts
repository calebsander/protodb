import {getCollections} from './collections'
import * as item from './collections/item'
import {
	commandType,
	ItemCreateCommand,
	ItemDropCommand,
	ListResponse,
	VoidResponse,
	listReponseType,
	voidReponseType,
	Collection
} from './sb-types/request'

const errorToString = ({name, message}: Error): string =>
	`${name}: ${message}`

async function runList(): Promise<ListResponse> {
	try {
		const dbCollections = await getCollections()
		const collections: Collection[] = []
		for (const [name, {type}] of dbCollections) collections.push({name, type})
		return {error: null, collections}
	}
	catch (e) {
		return {error: errorToString(e), collections: null}
	}
}
function runItemCreate({name, schema}: ItemCreateCommand): Promise<VoidResponse> {
	return item.create(name, schema)
		.then(_ => ({error: null}))
		.catch(e => ({error: errorToString(e)}))
}
function runItemDrop({name}: ItemDropCommand): Promise<VoidResponse> {
	return item.drop(name)
		.then(_ => ({error: null}))
		.catch(e => ({error: errorToString(e)}))
}

export async function runCommand(data: ArrayBuffer): Promise<ArrayBuffer> {
	const command = commandType.readValue(data)
	switch (command.type) {
		case 'list':
			return listReponseType.valueBuffer(await runList())
		case 'item_create':
			return voidReponseType.valueBuffer(await runItemCreate(command))
		case 'item_drop':
			return voidReponseType.valueBuffer(await runItemDrop(command))
		default:
			console.error('Unrecognized command')
			return new ArrayBuffer(0)
	}
}
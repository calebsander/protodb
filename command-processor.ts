import {getCollections} from './collections'
import * as item from './collections/item'
import * as hash from './collections/hash'
import {
	Collection,
	commandType,
	ItemCreateCommand,
	ItemDropCommand,
	ItemGetCommand,
	ItemSetCommand,
	HashCreateCommand,
	HashDropCommand,
	BytesResponse,
	ListResponse,
	VoidResponse,
	bytesResponseType,
	listReponseType,
	voidReponseType
} from './sb-types/request'

const errorToString = ({name, message}: Error) =>
	({error: `${name}: ${message}`})

async function runList(): Promise<ListResponse> {
	try {
		const dbCollections = await getCollections()
		const collections: Collection[] = []
		for (const [name, {type}] of dbCollections) collections.push({name, type})
		return {collections}
	}
	catch (e) {
		return errorToString(e)
	}
}
function runItemCreate({name, schema}: ItemCreateCommand): Promise<VoidResponse> {
	return item.create(name, schema)
		.then(_ => ({}))
		.catch(errorToString)
}
function runItemDrop({name}: ItemDropCommand): Promise<VoidResponse> {
	return item.drop(name)
		.then(_ => ({}))
		.catch(errorToString)
}
function runItemGet({name}: ItemGetCommand): Promise<BytesResponse> {
	return item.get(name)
		.then(data => ({data}))
		.catch(errorToString)
}
function runItemSet({name, value}: ItemSetCommand): Promise<VoidResponse> {
	return item.set(name, value)
		.then(_ => ({}))
		.catch(errorToString)
}
function runHashCreate({name, keySchema, valueSchema}: HashCreateCommand): Promise<VoidResponse> {
	return hash.create(name, keySchema, valueSchema)
		.then(_ => ({}))
		.catch(errorToString)
}
function runHashDrop({name}: HashDropCommand): Promise<VoidResponse> {
	return hash.drop(name)
		.then(_ => ({}))
		.catch(errorToString)
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
		case 'item_get':
			return bytesResponseType.valueBuffer(await runItemGet(command))
		case 'item_set':
			return voidReponseType.valueBuffer(await runItemSet(command))
		case 'hash_create':
			return voidReponseType.valueBuffer(await runHashCreate(command))
		case 'hash_drop':
			return voidReponseType.valueBuffer(await runHashDrop(command))
		default:
			const unreachable: never = command
			unreachable
			console.error('Unrecognized command')
			return new ArrayBuffer(0)
	}
}
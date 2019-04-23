import * as sb from 'structure-bytes'
import {literalType, Schema, schemaType} from './common'

interface ListCommand {
	type: 'list'
}
const listCommandType = new sb.StructType<ListCommand>({
	type: literalType('list')
})

export interface ItemCreateCommand {
	type: 'item_create'
	name: string
	schema: Schema
}
const itemCreateCommandType = new sb.StructType<ItemCreateCommand>({
	type: literalType('item_create'),
	name: new sb.StringType,
	schema: schemaType
})

export interface ItemDropCommand {
	type: 'item_drop'
	name: string
}
const itemDropCommandType = new sb.StructType<ItemDropCommand>({
	type: literalType('item_drop'),
	name: new sb.StringType
})

export interface ItemGetCommand {
	type: 'item_get'
	name: string
}
const itemGetCommandType = new sb.StructType<ItemGetCommand>({
	type: literalType('item_get'),
	name: new sb.StringType
})

export interface ItemSetCommand {
	type: 'item_set'
	name: string
	value: ArrayBuffer
}
const itemSetCommandType = new sb.StructType<ItemSetCommand>({
	type: literalType('item_set'),
	name: new sb.StringType,
	value: new sb.OctetsType
})

export interface HashCreateCommand {
	type: 'hash_create'
	name: string
	keySchema: Schema
	valueSchema: Schema
}
const hashCreateCommandType = new sb.StructType<HashCreateCommand>({
	type: literalType('hash_create'),
	name: new sb.StringType,
	keySchema: schemaType,
	valueSchema: schemaType
})

export interface HashDropCommand {
	type: 'hash_drop'
	name: string
}
const hashDropCommandType = new sb.StructType<HashDropCommand>({
	type: literalType('hash_drop'),
	name: new sb.StringType
})

export type Command
	= ListCommand
	| ItemCreateCommand
	| ItemDropCommand
	| ItemGetCommand
	| ItemSetCommand
	| HashCreateCommand
	| HashDropCommand
export const commandType = new sb.ChoiceType<Command>([
	listCommandType,
	itemCreateCommandType,
	itemDropCommandType,
	itemGetCommandType,
	itemSetCommandType,
	hashCreateCommandType,
	hashDropCommandType
])

type ErrorResponse<A> = {error: string} | A
const errorType = new sb.StructType({error: new sb.StringType})

export type VoidResponse = ErrorResponse<{}>
export const voidReponseType = new sb.ChoiceType<VoidResponse>([
	errorType,
	new sb.StructType({})
])

export type CollectionType
	= 'item'
	| 'hash'
export interface Collection {
	name: string
	type: CollectionType
}
interface Collections {
	collections: Collection[]
}
export type ListResponse = ErrorResponse<Collections>
export const listReponseType = new sb.ChoiceType<ListResponse>([
	errorType,
	new sb.StructType<Collections>({
		collections: new sb.ArrayType(
			new sb.StructType({
				name: new sb.StringType,
				type: new sb.EnumType({
					type: new sb.StringType as sb.Type<CollectionType>,
					values: ['item', 'hash']
				})
			})
		)
	})
])

export type BytesResponse = ErrorResponse<{data: ArrayBuffer}>
export const bytesResponseType = new sb.ChoiceType<BytesResponse>([
	errorType,
	new sb.StructType({data: new sb.OctetsType})
])
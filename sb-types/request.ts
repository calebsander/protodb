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

export type Command
	= ListCommand
	| ItemCreateCommand
	| ItemDropCommand
export const commandType = new sb.ChoiceType<Command>([
	listCommandType,
	itemCreateCommandType,
	itemDropCommandType
])

export interface VoidResponse {
	error: string | null
}
export const voidReponseType = new sb.StructType<VoidResponse>({
	error: new sb.OptionalType(new sb.StringType)
})

export type CollectionType = 'item'
export interface Collection {
	name: string
	type: CollectionType
}
export interface ListResponse extends VoidResponse {
	collections: Collection[] | null
}
export const listReponseType = new sb.StructType<ListResponse>({
	error: new sb.OptionalType(new sb.StringType),
	collections: new sb.OptionalType(
		new sb.ArrayType(
			new sb.StructType({
				name: new sb.StringType,
				type: new sb.EnumType({
					type: new sb.StringType as sb.Type<CollectionType>,
					values: ['item']
				})
			})
		)
	)
})
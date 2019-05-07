import * as sb from 'structure-bytes'
import {ITER_BYTE_LENGTH} from '../collections/hash'
import {literalType} from './common'
import {BucketItem, bucketItemType} from './hash'

interface ListCommand {
	type: 'list'
}
const listCommandType = new sb.StructType<ListCommand>({
	type: literalType('list')
})

export interface ItemCreateCommand {
	type: 'item_create'
	name: string
}
const itemCreateCommandType = new sb.StructType<ItemCreateCommand>({
	type: literalType('item_create'),
	name: new sb.StringType
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
}
const hashCreateCommandType = new sb.StructType<HashCreateCommand>({
	type: literalType('hash_create'),
	name: new sb.StringType
})

export interface HashDropCommand {
	type: 'hash_drop'
	name: string
}
const hashDropCommandType = new sb.StructType<HashDropCommand>({
	type: literalType('hash_drop'),
	name: new sb.StringType
})

export interface HashGetCommand {
	type: 'hash_get'
	name: string
	key: ArrayBuffer
}
const hashGetCommandType = new sb.StructType<HashGetCommand>({
	type: literalType('hash_get'),
	name: new sb.StringType,
	key: new sb.OctetsType
})

export interface HashSetCommand extends BucketItem {
	type: 'hash_set'
	name: string
}
const hashSetCommandType = new sb.StructType<HashSetCommand>({
	type: literalType('hash_set'),
	name: new sb.StringType,
	key: new sb.OctetsType,
	value: new sb.OctetsType
})

export interface HashDeleteCommand {
	type: 'hash_delete'
	name: string
	key: ArrayBuffer
}
const hashDeleteCommandType = new sb.StructType<HashDeleteCommand>({
	type: literalType('hash_delete'),
	name: new sb.StringType,
	key: new sb.OctetsType
})

export interface HashSizeCommand {
	type: 'hash_size'
	name: string
}
const hashSizeCommandType = new sb.StructType<HashSizeCommand>({
	type: literalType('hash_size'),
	name: new sb.StringType
})

export interface HashIterCommand {
	type: 'hash_iter'
	name: string
}
const hashIterCommandType = new sb.StructType<HashIterCommand>({
	type: literalType('hash_iter'),
	name: new sb.StringType
})

type Iter = number[]
const iterType: sb.Type<Iter> = new sb.TupleType({
	type: new sb.UnsignedByteType,
	length: ITER_BYTE_LENGTH
})

export interface HashIterNextCommand {
	type: 'hash_iter_next'
	iter: Iter
}
const hashIterNextCommandType = new sb.StructType<HashIterNextCommand>({
	type: literalType('hash_iter_next'),
	iter: iterType
})

export interface HashIterBreakCommand {
	type: 'hash_iter_break'
	iter: Iter
}
const hashIterBreakCommandType = new sb.StructType<HashIterBreakCommand>({
	type: literalType('hash_iter_break'),
	iter: iterType
})

export interface ListCreateCommand {
	type: 'list_create'
	name: string
}
const listCreateCommandType = new sb.StructType<ListCreateCommand>({
	type: literalType('list_create'),
	name: new sb.StringType
})

export interface ListDropCommand {
	type: 'list_drop'
	name: string
}
const listDropCommandType = new sb.StructType<ListDropCommand>({
	type: literalType('list_drop'),
	name: new sb.StringType
})

export interface ListGetCommand {
	type: 'list_get'
	name: string
	index: number
}
const listGetCommandType = new sb.StructType<ListGetCommand>({
	type: literalType('list_get'),
	name: new sb.StringType,
	index: new sb.FlexIntType
})

export interface ListSetCommand {
	type: 'list_set'
	name: string
	index: number
	value: ArrayBuffer
}
const listSetCommandType = new sb.StructType<ListSetCommand>({
	type: literalType('list_set'),
	name: new sb.StringType,
	index: new sb.FlexIntType,
	value: new sb.OctetsType
})

export interface ListInsertCommand {
	type: 'list_insert'
	name: string
	index: number | null
	value: ArrayBuffer
}
const listInsertCommandType = new sb.StructType<ListInsertCommand>({
	type: literalType('list_insert'),
	name: new sb.StringType,
	index: new sb.OptionalType(new sb.FlexIntType),
	value: new sb.OctetsType
})

export type Command
	= ListCommand
	| ItemCreateCommand
	| ItemDropCommand
	| ItemGetCommand
	| ItemSetCommand
	| HashCreateCommand
	| HashDropCommand
	| HashGetCommand
	| HashSetCommand
	| HashDeleteCommand
	| HashSizeCommand
	| HashIterCommand
	| HashIterNextCommand
	| HashIterBreakCommand
	| ListCreateCommand
	| ListDropCommand
	| ListGetCommand
	| ListSetCommand
	| ListInsertCommand
export const commandType = new sb.ChoiceType<Command>([
	listCommandType,
	itemCreateCommandType,
	itemDropCommandType,
	itemGetCommandType,
	itemSetCommandType,
	hashCreateCommandType,
	hashDropCommandType,
	hashGetCommandType,
	hashSetCommandType,
	hashDeleteCommandType,
	hashSizeCommandType,
	hashIterCommandType,
	hashIterNextCommandType,
	hashIterBreakCommandType,
	listCreateCommandType,
	listDropCommandType,
	listGetCommandType,
	listSetCommandType,
	listInsertCommandType
])

type ErrorResponse<A> = {error: string} | A
const errorType = new sb.StructType({error: new sb.StringType})

export type BytesResponse = ErrorResponse<{data: ArrayBuffer}>
export const bytesResponseType = new sb.ChoiceType<BytesResponse>([
	errorType,
	new sb.StructType({data: new sb.OctetsType})
])

export type IterResponse = ErrorResponse<{iter: Iter}>
export const iterResponseType = new sb.ChoiceType<IterResponse>([
	errorType,
	new sb.StructType({iter: iterType})
])

export type CollectionType
	= 'item'
	| 'hash'
	| 'list'
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
					values: ['item', 'hash', 'list']
				})
			})
		)
	})
])

export type OptionalBytesResponse = ErrorResponse<{data: ArrayBuffer | null}>
export const optionalBytesResponseType = new sb.ChoiceType<OptionalBytesResponse>([
	errorType,
	new sb.StructType<OptionalBytesResponse>({
		data: new sb.OptionalType(new sb.OctetsType)
	})
])

export type OptionalPairResponse = ErrorResponse<{item: BucketItem | null}>
export const optionalPairResponseType = new sb.ChoiceType<OptionalPairResponse>([
	errorType,
	new sb.StructType<OptionalPairResponse>({
		item: new sb.OptionalType(bucketItemType)
	})
])

export type UnsignedResponse = ErrorResponse<{value: number}>
export const unsignedResponseType = new sb.ChoiceType<UnsignedResponse>([
	errorType,
	new sb.StructType<UnsignedResponse>({value: new sb.FlexUnsignedIntType})
])

export type VoidResponse = ErrorResponse<{}>
export const voidReponseType = new sb.ChoiceType<VoidResponse>([
	errorType,
	new sb.StructType({})
])
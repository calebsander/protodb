import * as sb from 'structure-bytes'
import {literalType, Schema, schemaType} from './common'

interface ItemType {
	type: 'item'
	schema: Schema
}

const itemType = new sb.StructType<ItemType>({
	type: literalType('item'),
	schema: schemaType
})

interface HashType {
	type: 'hash'
	keySchema: Schema
	valueSchema: Schema
}

const hashType = new sb.StructType<HashType>({
	type: literalType('hash'),
	keySchema: schemaType,
	valueSchema: schemaType
})

export type CollectionType
	= ItemType
	| HashType
const collectionType = new sb.ChoiceType<CollectionType>([
	itemType,
	hashType
])

export type Collections = Map<string, CollectionType>
export interface DB {
	collections: Collections
}
export const dbType = new sb.StructType<DB>({
	collections: new sb.MapType(new sb.StringType, collectionType)
})
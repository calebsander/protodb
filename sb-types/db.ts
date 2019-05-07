import * as sb from 'structure-bytes'
import {literalType} from './common'

interface ItemType {
	type: 'item'
}
const itemType = new sb.StructType<ItemType>({type: literalType('item')})

interface HashType {
	type: 'hash'
}
const hashType = new sb.StructType<HashType>({type: literalType('hash')})

interface ListType {
	type: 'list'
}
const listType = new sb.StructType<ListType>({type: literalType('list')})

export type CollectionType
	= ItemType
	| HashType
	| ListType
const collectionType = new sb.ChoiceType<CollectionType>([
	itemType,
	hashType,
	listType
])

export type Collections = Map<string, CollectionType>
export interface DB {
	collections: Collections
}
export const dbType = new sb.StructType<DB>({
	collections: new sb.MapType(new sb.StringType, collectionType)
})
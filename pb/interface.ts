export enum CollectionType {
	ITEM,
	HASH,
	LIST,
	SORTED
}
export interface Collections {
	[name: string]: CollectionType
}
export interface DB {
	collections: Collections
}

export interface KeyValuePair {
	key: Uint8Array
	value: Uint8Array
}

export type KeyElement
	= {int: number}
	| {float: number}
	| {string: string}
	| {uniquifier: number}
export interface Key {
	elements: KeyElement[]
}
export interface SortedKeyValuePair {
	key: KeyElement[]
	value: Uint8Array
}
export type CollectionType
	= {item: {}}
	| {hash: {}}
	| {list: {}}
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
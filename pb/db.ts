import path from 'path'
import protobuf from 'protobufjs'
import {Type} from './common'

export type CollectionType
	= {item: {}}
	| {hash: {}}
	| {list: {}}

export interface Collections {
	[name: string]: CollectionType | undefined
}
export interface DB {
	collections: Collections
}

export const dbType = protobuf.loadSync(path.join(__dirname, 'db.proto'))
	.lookupType('DB') as Type<DB>
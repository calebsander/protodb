import * as path from 'path'
import * as protobuf from 'protobufjs'
import {Type} from './common'

const protoFile = protobuf.loadSync(path.join(__dirname, 'list.proto'))

export interface Child {
	size: number
	page: number
}
export const childType = protoFile.lookupType('Child') as Type<Child>

export const FREE_LIST_END = 0
export interface FreePage {
	next: number
}
export const freePageType = protoFile.lookupType('FreePage') as Type<FreePage>

export interface Header {
	child: Child
	freePage: FreePage
}
export const headerType = protoFile.lookupType('Header') as Type<Header>

export interface InnerNode {
	children: Child[]
}
export interface LeafNode {
	values: Uint8Array[]
}
export type Node
	= {inner: InnerNode}
	| {leaf: LeafNode}
export const nodeType = protoFile.lookupType('Node') as Type<Node>
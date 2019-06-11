import path = require('path')
import protobuf = require('protobufjs')
import {Type} from './common'

const protoFile = protobuf.loadSync(path.join(__dirname, 'list.proto'))

export interface Child {
	size: number
	page: number
}
export const childType = protoFile.lookupType('Child') as Type<Child>

// Value of FreePage.next at the end of the free list.
// 0 is the page containing the header, which is always in use.
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
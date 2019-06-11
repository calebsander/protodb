import path = require('path')
import protobuf = require('protobufjs')
import {Type} from './common'
import {Key} from './interface'

const protoFile = protobuf.loadSync(
	['interface.proto', 'sorted.proto'].map(file => path.join(__dirname, file))
)

// Value of FreePage.next or LeafNode.next at the end of their lists.
// 0 is the page containing the header, so there is no possibile conflict.
export const LIST_END = 0
export interface FreePage {
	next: number
}
export const freePageType = protoFile.lookupType('FreePage') as Type<FreePage>

export interface Header {
	root: number
	size: number
	freePage: FreePage
}
export const headerType = protoFile.lookupType('Header') as Type<Header>

export interface InnerNode {
	// There is always 1 more child than splitting keys.
	// Key i splits children i and i + 1.
	// Splitting key is loosely larger than all keys on the left.
	keys: Key[]
	children: number[]
}
export interface LeafNode {
	// There are always equal numbers of keys and values
	keys: Key[]
	values: Uint8Array[]
	next: number
}
export type Node
	= {inner: InnerNode}
	| {leaf: LeafNode}
export const nodeType = protoFile.lookupType('Node') as Type<Node>
import * as sb from 'structure-bytes'
import {literalType} from './common'

export interface Child {
	size: number
	page: number
}
const childType = new sb.StructType<Child>({
	size: new sb.FlexUnsignedIntType,
	page: new sb.FlexUnsignedIntType
})

export const FREE_LIST_END = 0
export const freePageType: sb.Type<number> = new sb.FlexUnsignedIntType

export interface Header {
	child: Child
	freePage: number
}
export const headerType = new sb.StructType<Header>({
	child: childType,
	freePage: freePageType
})

export interface InnerNode {
	type: 'inner'
	children: Child[]
}
export interface LeafNode {
	type: 'leaf'
	values: ArrayBuffer[]
}
export const nodeType = new sb.ChoiceType<InnerNode | LeafNode>([
	new sb.StructType<InnerNode>({
		type: literalType('inner'),
		children: new sb.ArrayType(childType)
	}),
	new sb.StructType<LeafNode>({
		type: literalType('leaf'),
		values: new sb.ArrayType(new sb.OctetsType)
	})
])
import path = require('path')
import protobuf = require('protobufjs')
import {Type} from './common'

export interface Item {
	value: Uint8Array
}

export const itemType = protobuf.loadSync(path.join(__dirname, 'item.proto'))
	.lookupType('Item') as Type<Item>
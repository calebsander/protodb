import path = require('path')
import protobuf = require('protobufjs')
import {Type} from './common'
import {DB} from './interface'

export const dbType = protobuf.loadSync(path.join(__dirname, 'interface.proto'))
	.lookupType('DB') as Type<DB>
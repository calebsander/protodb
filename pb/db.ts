import path from 'path'
import protobuf from 'protobufjs'
import {Type} from './common'
import {DB} from './interface'

export const dbType = protobuf.loadSync(path.join(__dirname, 'db.proto'))
	.lookupType('DB') as Type<DB>
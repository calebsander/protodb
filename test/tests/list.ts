import {TestInterface} from 'ava'
import {TestContext} from '../common'
import {bytesResponseType, sizeResponseType, voidResponseType} from '../../pb/request'

export default (test: TestInterface<TestContext>) => {
	test('list-stack', async t => {
		const name = 'stck'
		let result = await t.context.sendCommand(
			{listCreate: {name}},
			voidResponseType
		)
		t.deepEqual(result, {})
		const getValue = (key: number) =>
			new Uint8Array(new Int32Array(10).fill(key).buffer)
		for (let i = 0; i < 1e3; i++) {
			result = await t.context.sendCommand(
				{listInsert: {name, index: {none: {}}, value: getValue(i * 2)}},
				voidResponseType
			)
			t.deepEqual(result, {})
			result = await t.context.sendCommand(
				{listInsert: {name, index: {none: {}}, value: getValue(i * 2 + 1)}},
				voidResponseType
			)
			t.deepEqual(result, {})
			{
				const result = await t.context.sendCommand(
					{listGet: {name, index: -1}},
					bytesResponseType
				)
				t.deepEqual(result, {data: getValue(i * 2 + 1)})
			}
			result = await t.context.sendCommand(
				{listDelete: {name, index: {none: {}}}},
				voidResponseType
			)
			t.deepEqual(result, {})
			{
				const result = await t.context.sendCommand(
					{listSize: {name}},
					sizeResponseType
				)
				t.deepEqual(result, {size: i + 1})
			}
		}
		for (let i = 999; i >= 0; i--) {
			{
				const result = await t.context.sendCommand(
					{listGet: {name, index: -1}},
					bytesResponseType
				)
				t.deepEqual(result, {data: getValue(i * 2)})
			}
			result = await t.context.sendCommand(
				{listDelete: {name, index: {none: {}}}},
				voidResponseType
			)
			t.deepEqual(result, {})
			{
				const result = await t.context.sendCommand(
					{listSize: {name}},
					sizeResponseType
				)
				t.deepEqual(result, {size: i})
			}
		}
		result = await t.context.sendCommand({listDrop: {name}}, voidResponseType)
		t.deepEqual(result, {})
	})

	test('list-queue', async t => {
		const name = 'q'
		let result = await t.context.sendCommand(
			{listCreate: {name}},
			voidResponseType
		)
		t.deepEqual(result, {})
		const getValue = (key: number) =>
			new Uint8Array(new Int32Array(10).fill(key).buffer)
		for (let i = 0; i < 1e3; i++) {
			result = await t.context.sendCommand(
				{listInsert: {name, index: {none: {}}, value: getValue(i * 2)}},
				voidResponseType
			)
			t.deepEqual(result, {})
			result = await t.context.sendCommand(
				{listInsert: {name, index: {none: {}}, value: getValue(i * 2 + 1)}},
				voidResponseType
			)
			t.deepEqual(result, {})
			{
				const result = await t.context.sendCommand(
					{listGet: {name, index: 0}},
					bytesResponseType
				)
				t.deepEqual(result, {data: getValue(i)})
			}
			result = await t.context.sendCommand(
				{listDelete: {name, index: {value: 0}}},
				voidResponseType
			)
			t.deepEqual(result, {})
			{
				const result = await t.context.sendCommand(
					{listSize: {name}},
					sizeResponseType
				)
				t.deepEqual(result, {size: i + 1})
			}
		}
		for (let i = 0; i < 1e3; i++) {
			{
				const result = await t.context.sendCommand(
					{listGet: {name, index: 0}},
					bytesResponseType
				)
				t.deepEqual(result, {data: getValue(1e3 + i)})
			}
			result = await t.context.sendCommand(
				{listDelete: {name, index: {value: 0}}},
				voidResponseType
			)
			t.deepEqual(result, {})
			{
				const result = await t.context.sendCommand(
					{listSize: {name}},
					sizeResponseType
				)
				t.deepEqual(result, {size: 999 - i})
			}
		}
		result = await t.context.sendCommand({listDrop: {name}}, voidResponseType)
		t.deepEqual(result, {})
	})
}
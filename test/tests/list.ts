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
		const getValue = (i: number) =>
			new Uint8Array(new Int32Array(10).fill(i).buffer)
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
		const getValue = (i: number) =>
			new Uint8Array(new Int32Array(10).fill(i).buffer)
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

	test('list-multilevel', async t => {
		const name = 'bigger'
		let result = await t.context.sendCommand(
			{listCreate: {name}},
			voidResponseType
		)
		t.deepEqual(result, {})
		const getValue = (i: number) => new Uint8Array(3e3).fill(i * i)
		const values: number[] = []
		while (values.length < 2e3) {
			const index = (Math.random() * (values.length + 1)) | 0
			result = await t.context.sendCommand(
				{listInsert: {name, index: {value: index}, value: getValue(values.length)}},
				voidResponseType
			)
			t.deepEqual(result, {})
			values.splice(index, 0, values.length)
		}
		for (let i = 0; i < 2e3; i++) {
			const result = await t.context.sendCommand(
				{listGet: {name, index: i}},
				bytesResponseType
			)
			t.deepEqual(result, {data: getValue(values[i])})
		}
		while (values.length) {
			// Sample a random value and delete it
			const index = (Math.random() * values.length) | 0
			{
				const result = await t.context.sendCommand(
					{listGet: {name, index}},
					bytesResponseType
				)
				t.deepEqual(result, {data: getValue(values[index])})
			}
			result = await t.context.sendCommand(
				{listDelete: {name, index: {value: index}}},
				voidResponseType
			)
			t.deepEqual(result, {})
			values.splice(index, 1)
			{
				const result = await t.context.sendCommand(
					{listSize: {name}},
					sizeResponseType
				)
				t.deepEqual(result, {size: values.length})
			}
		}
	})

	// TODO: test iterators
	// TODO: test that tree and free list are properly maintained
}
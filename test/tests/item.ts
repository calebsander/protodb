import {TestInterface} from 'ava'
import {TestContext} from '../common'
import {bytesResponseType, voidResponseType} from '../../pb/request'

export default (test: TestInterface<TestContext>) => {
	test('item-get-set', async t => {
		const name = 'thing'
		let result = await t.context.sendCommand(
			{itemCreate: {name}},
			voidResponseType
		)
		t.deepEqual(result, {})
		for (let size = 0; size < 1e4; size += 10) {
			const value = new Uint8Array(size).map((_, i) => ~i)
			{
				const result = await t.context.sendCommand(
					{itemSet: {name, value}},
					voidResponseType
				)
				t.deepEqual(result, {})
			}
			{
				const result = await t.context.sendCommand(
					{itemGet: {name}},
					bytesResponseType
				)
				t.deepEqual(result, {data: value})
			}
		}
		result = await t.context.sendCommand({itemDrop: {name}}, voidResponseType)
		t.deepEqual(result, {})
	})

	test('item-unset', async t => {
		const name = 'abc'
		{
			const result = await t.context.sendCommand(
				{itemCreate: {name}},
				voidResponseType
			)
			t.deepEqual(result, {})
		}
		{
			const result = await t.context.sendCommand(
				{itemGet: {name}},
				bytesResponseType
			)
			t.deepEqual(result, {error: `Error: Collection ${name} has not been set`})
		}
		{
			const result = await t.context.sendCommand(
				{itemDrop: {name}},
				voidResponseType
			)
			t.deepEqual(result, {})
		}
	})

	test('item-check', async t => {
		const hashName = 'h', undefinedName = 'not-a-collection'
		let result = await t.context.sendCommand(
			{hashCreate: {name: hashName}},
			voidResponseType
		)
		t.deepEqual(result, {})

		await Promise.all([hashName, undefinedName].map(name => {
			const errorResult = {error: `Error: Collection ${name} is not an item`}
			return Promise.all([
				t.context.sendCommand({itemDrop: {name}}, voidResponseType)
					.then(result => t.deepEqual(result, errorResult)),
				t.context.sendCommand({itemGet: {name}}, bytesResponseType)
					.then(result => t.deepEqual(result, errorResult)),
				t.context.sendCommand(
					{itemSet: {name, value: new Uint8Array(3)}},
					voidResponseType
				)
					.then(result => t.deepEqual(result, errorResult))
			])
		}))

		result = await t.context.sendCommand(
			{itemCreate: {name: hashName}},
			voidResponseType
		)
		t.deepEqual(result, {error: `Error: Collection ${hashName} already exists`})

		const name = 'existingItem'
		result = await t.context.sendCommand({itemCreate: {name}}, voidResponseType)
		t.deepEqual(result, {})
		result = await t.context.sendCommand({itemCreate: {name}}, voidResponseType)
		t.deepEqual(result, {error: `Error: Collection ${name} already exists`})
	})
}
import {TestInterface} from 'ava'
import {TestContext} from '../common'

const toFloat = (f: number) => new Float32Array([f])[0]

export default (test: TestInterface<TestContext>) => {
	test('sorted-small', async t => {
		const name = 'lil-s'
		await t.context.client.sortedCreate(name)
		await t.context.client.sortedInsert(
			name, [{string: 'def'}], new Uint8Array([21])
		)
		await t.context.client.sortedInsert(
			name, [{string: 'ghi'}], new Uint8Array([3])
		)
		await t.context.client.sortedInsert(
			name, [{string: 'abc'}], new Uint8Array([1])
		)
		await t.context.client.sortedInsert(
			name, [{string: 'def'}], new Uint8Array([22])
		)
		await t.context.client.sortedInsert(
			name, [{string: 'def'}], new Uint8Array([23])
		)

		let result = await t.context.client.sortedGet(name, [{string: 'a'}])
		t.deepEqual(result, [])

		let key = [{string: 'abc'}]
		result = await t.context.client.sortedGet(name, key)
		t.deepEqual(result, [{key, value: new Uint8Array([1])}])

		result = await t.context.client.sortedGet(name, [{string: 'bcd'}])
		t.deepEqual(result, [])

		key = [{string: 'def'}]
		result = await t.context.client.sortedGet(name, key)
		t.deepEqual(result, [
			{key: [...key, {uniquifier: 2}], value: new Uint8Array([23])},
			{key: [...key, {uniquifier: 1}], value: new Uint8Array([22])},
			{key: [...key, {uniquifier: 0}], value: new Uint8Array([21])}
		])

		result = await t.context.client.sortedGet(name, [{string: 'efg'}])
		t.deepEqual(result, [])

		key = [{string: 'ghi'}]
		result = await t.context.client.sortedGet(name, key)
		t.deepEqual(result, [{key, value: new Uint8Array([3])}])

		result = await t.context.client.sortedGet(name, [{string: 'ghij'}])
		t.deepEqual(result, [])

		await t.context.client.sortedDrop(name)
	})
	test('sorted-split', async t => {
		const name = 'many-s'
		await t.context.client.sortedCreate(name)
		const getValue = (i: number) => new Uint8Array(50).fill(i)
		const items: {key: number, value: Uint8Array}[] = []
		for (let i = 0; i < 1e3; i++) {
			const key = toFloat(Math.random())
			const value = getValue(i)
			items.push({key, value})
			await t.context.client.sortedInsert(name, [{float: key}], value)
		}
		for (const {key, value} of items) {
			const sortedKey = [{float: key}]
			const result = await t.context.client.sortedGet(name, sortedKey)
			t.deepEqual(result, [{key: sortedKey, value}])
		}
		const result = await t.context.client.sortedGet(name, [])
		items.sort((a, b) => a.key - b.key)
		t.deepEqual(
			result,
			items.map(({key, value}) => ({key: [{float: key}], value}))
		)
		await t.context.client.sortedDrop(name)
	})
}
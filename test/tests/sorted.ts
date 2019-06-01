import {TestInterface} from 'ava'
import {TestContext} from '../common'

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
		result = await t.context.client.sortedGet(name, [{string: 'abc'}])
		t.deepEqual(result, [new Uint8Array([1])])
		result = await t.context.client.sortedGet(name, [{string: 'bcd'}])
		t.deepEqual(result, [])
		result = await t.context.client.sortedGet(name, [{string: 'def'}])
		t.deepEqual(result, [
			new Uint8Array([23]),
			new Uint8Array([22]),
			new Uint8Array([21])
		])
		result = await t.context.client.sortedGet(name, [{string: 'efg'}])
		t.deepEqual(result, [])
		result = await t.context.client.sortedGet(name, [{string: 'ghi'}])
		t.deepEqual(result, [new Uint8Array([3])])
		result = await t.context.client.sortedGet(name, [{string: 'ghij'}])
		t.deepEqual(result, [])
		await t.context.client.sortedDrop(name)
	})
	test.failing('sorted-split', async t => {
		const name = 'many-s'
		await t.context.client.sortedCreate(name)
		const getValue = (i: number) => new Uint8Array(50).fill(i)
		const keys: number[] = []
		for (let i = 0; i < 1e3; i++) {
			const float = Math.random()
			keys.push(float)
			await t.context.client.sortedInsert(name, [{float}], getValue(i))
		}
		// TODO: query the values
		await t.context.client.sortedDrop(name)
	})
}
import {TestInterface} from 'ava'
import {TestContext} from '../common'
import {ProtoDBError} from '../../client'

export default (test: TestInterface<TestContext>) => {
	test('item-get-set', async t => {
		const name = 'thing'
		await t.context.client.itemCreate(name)
		for (let size = 0; size < 1e4; size += 10) {
			const value = new Uint8Array(size).map((_, i) => ~i)
			await t.context.client.itemSet(name, value)
			t.deepEqual(await t.context.client.itemGet(name), value)
		}
		await t.context.client.itemDrop(name)
	})

	test('item-unset', async t => {
		const name = 'abc'
		await t.context.client.itemCreate(name)
		await t.throwsAsync(
			() => t.context.client.itemGet(name),
			{
				instanceOf: ProtoDBError,
				message: `Error: Item ${name} has not been set`
			}
		)
		await t.context.client.itemDrop(name)
	})

	test('item-check', async t => {
		const hashName = 'h', undefinedName = 'not-a-collection'
		await t.context.client.hashCreate(hashName)

		await Promise.all([hashName, undefinedName].map(name =>
			Promise.all(
				[
					() => t.context.client.itemDrop(name),
					() => t.context.client.itemGet(name),
					() => t.context.client.itemSet(name, new ArrayBuffer(3))
				].map(action => t.throwsAsync(action, {
					instanceOf: ProtoDBError,
					message: `Error: Collection ${name} is not an item`
				}))
			)
		))

		await t.throwsAsync(
			() => t.context.client.itemCreate(hashName),
			{
				instanceOf: ProtoDBError,
				message: `Error: Collection ${hashName} already exists`
			}
		)

		const name = 'existingItem'
		await t.context.client.itemCreate(name)
		await t.throwsAsync(
			() => t.context.client.itemCreate(name),
			{
				instanceOf: ProtoDBError,
				message: `Error: Collection ${name} already exists`
			}
		)
	})
}
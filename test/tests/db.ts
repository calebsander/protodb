import {TestInterface} from 'ava'
import {TestContext} from '../common'
import {CollectionType} from '../../pb/interface'

export default (test: TestInterface<TestContext>) => {
	test('db-restart', async t => {
		let db = await t.context.client.list()
		t.deepEqual(db, {collections: {}})

		await t.context.client.hashCreate('h')
		for (let i = 0; i < 1e3; i++) {
			await t.context.client.hashSet(
				'h', new Int32Array([i]).buffer, new Uint8Array(i)
			)
		}
		await t.context.restart()
		db = await t.context.client.list()
		t.deepEqual(db, {collections: {h: CollectionType.HASH}})
		for (let i = 0; i < 1e3; i++) {
			const result = await t.context.client.hashGet(
				'h', new Int32Array([i]).buffer
			)
			t.deepEqual(result, new Uint8Array(i))
		}

		await t.context.client.itemCreate('i')
		await t.context.client.itemSet('i', new Uint8Array([1, 2, 3]))
		await t.context.restart()
		db = await t.context.client.list()
		t.deepEqual(db, {collections: {
			h: CollectionType.HASH,
			i: CollectionType.ITEM
		}})
		const result = await t.context.client.itemGet('i')
		t.deepEqual(result, new Uint8Array([1, 2, 3]))

		await t.context.client.listCreate('l')
		for (let i = 0; i < 1e3; i++) {
			await t.context.client.listInsert('l', new Float32Array([i]).buffer, 0)
		}
		await t.context.restart()
		db = await t.context.client.list()
		t.deepEqual(db, {collections: {
			h: CollectionType.HASH,
			i: CollectionType.ITEM,
			l: CollectionType.LIST
		}})
		for (let i = 0; i < 1e3; i++) {
			const result = await t.context.client.listGet('l', i)
			t.deepEqual(result, new Uint8Array(new Float32Array([999 - i]).buffer))
		}

		await t.context.client.sortedCreate('s')
		for (let i = 1; i < 1e3; i++) {
			await t.context.client.sortedInsert(
				's', [{int: -i}], new Float64Array([i]).buffer
			)
		}
		await t.context.restart()
		db = await t.context.client.list()
		t.deepEqual(db, {collections: {
			h: CollectionType.HASH,
			i: CollectionType.ITEM,
			l: CollectionType.LIST,
			s: CollectionType.SORTED
		}})
		const iter = await t.context.client.sortedIter('s')
		for (let i = 999; i > 0; i--) {
			const result = await t.context.client.sortedIterNext(iter)
			t.deepEqual(result, {
				key: [{int: -i}],
				value: new Uint8Array(new Float64Array([i]).buffer)
			})
		}
	})
}
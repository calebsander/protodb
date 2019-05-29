import {TestInterface} from 'ava'
import {TestContext} from '../common'
import {ProtoDBError} from '../../client'
import {concat} from '../../util'

const randomBytes = (n: number) =>
	new Uint8Array(n).map(_ => Math.random() * 256)

export default (test: TestInterface<TestContext>) => {
	test('hash-small', async t => {
		const name = 'small'
		await t.context.client.hashCreate(name)
		const data = new Array(20).fill(0).map(_ =>
			({key: randomBytes(10), value: randomBytes(50)})
		)
		await Promise.all(data.map(({key, value}) =>
			t.context.client.hashSet(name, key, value)
		))
		await Promise.all(data.map(async ({key, value}) => {
			const result = await t.context.client.hashGet(name, key)
			t.deepEqual(result, value)
		}))
		// Test keys that don't match
		for (let i = 0; i < 20; i++) {
			if (i === 10) continue

			const result = await t.context.client.hashGet(name, randomBytes(i))
			t.deepEqual(result, null)
		}
		await t.context.client.hashDrop(name)
	})

	test('hash-large', async t => {
		const name = 'large'
		await t.context.client.hashCreate(name)

		const getKey = (i: number) =>
			new Uint8Array([...String(i)].map(Number))
		const getValue = (key: Uint8Array) =>
			concat(new Array<Uint8Array>(50).fill(key))
		for (let i = 0; i < 5e3; i++) {
			const key = getKey(i)
			await t.context.client.hashSet(name, key, getValue(key))
		}
		let size = await t.context.client.hashSize(name)
		t.deepEqual(size, 5e3)
		for (let i = 0; i < 6e3; i++) {
			const key = getKey(i)
			const result = await t.context.client.hashGet(name, key)
			t.deepEqual(result, i < 5e3 ? getValue(key) : null)
		}

		// Try deleting some keys
		for (let i = 3e3; i < 6e3; i++) {
			await t.context.client.hashDelete(name, getKey(i))
		}
		size = await t.context.client.hashSize(name)
		t.deepEqual(size, 3e3)
		for (let i = 0; i < 6e3; i++) {
			const key = getKey(i)
			const result = await t.context.client.hashGet(name, key)
			t.deepEqual(result, i < 3e3 ? getValue(key) : null)
		}
	})

	test('hash-overwrite', async t => {
		const name = 'h'
		await t.context.client.hashCreate(name)

		for (let key = 0; key < 1e3; key++) {
			await t.context.client.hashSet(
				name,
				new Uint8Array(new Int32Array([key]).buffer),
				new Uint8Array(key).fill(key)
			)
		}
		let size = await t.context.client.hashSize(name)
		t.deepEqual(size, 1e3)

		for (let key = 0; key < 1e3; key++) {
			await t.context.client.hashSet(
				name,
				new Uint8Array(new Int32Array([key << 1]).buffer),
				new Uint8Array(key).fill(key)
			)
		}
		size = await t.context.client.hashSize(name)
		t.deepEqual(size, 1500)
		for (let key = 0; key < 2e3; key++) {
			const result = await t.context.client.hashGet(
				name,
				new Uint8Array(new Int32Array([key]).buffer)
			)
			t.deepEqual(
				result,
				key & 1
					? key < 1e3 ? new Uint8Array(key).fill(key) : null
					: new Uint8Array(key >> 1).fill(key >> 1)
			)
		}
	})

	test('hash-iter', async t => {
		const name = 'iterable'
		await t.context.client.hashCreate(name)

		const getValue = (key: number) =>
			concat([Buffer.from('a'.repeat(50)), new Uint8Array([key])])
		const values = new Map<number, Uint8Array>()
		for (let key = 0; key < 100; key++) {
			const value = getValue(key)
			values.set(key, value)
			await t.context.client.hashSet(name, new Uint8Array(key), value)
		}
		const iter1 = await t.context.client.hashIter(name)
		const iter2 = await t.context.client.hashIter(name)

		const tryOperations = () => Promise.all(
			[
				() => t.context.client.hashDrop(name),
				() => t.context.client.hashDelete(name, new Uint8Array(3)),
				() => t.context.client.hashSet(name, new Uint8Array(3), new Uint8Array(1))
			].map(action => t.throwsAsync(action, {
				instanceOf: ProtoDBError,
				message: `Error: Collection ${name} has active iterators`
			}))
		)
		await tryOperations()

		const iterSeen = new Set<number>()
		for (let i = 0; i < 50; i++) {
			const result1 = await t.context.client.hashIterNext(iter1)
			if (!result1) throw new Error('Missing item')
			const {key, value} = result1
			t.deepEqual(value, values.get(key.length))
			const result2 = await t.context.client.hashIterNext(iter2)
			t.deepEqual(result2, result1) // both iterators should return the same order
			iterSeen.add(key.length)
		}

		// Break out of the second iterator
		await t.context.client.hashIterBreak(iter2)
		await t.throwsAsync(
			() => t.context.client.hashIterBreak(iter2),
			{
				instanceOf: ProtoDBError,
				message: 'Error: Unknown iterator'
			}
		)
		await tryOperations()

		// Iterate over the rest of the elements with the first iterator
		for (let i = 0; i < 50; i++) {
			const result = await t.context.client.hashIterNext(iter1)
			if (!result) throw new Error('Missing item')
			const {key, value} = result
			t.deepEqual(value, values.get(key.length))
			iterSeen.add(key.length)
		}
		t.deepEqual(iterSeen.size, 100)
		const result = await t.context.client.hashIterNext(iter1)
		t.deepEqual(result, null)

		// Both iterators should now be invalid
		await Promise.all([iter1, iter2].map(iter =>
			t.throwsAsync(
				() => t.context.client.hashIterNext(iter),
				{
					instanceOf: ProtoDBError,
					message: 'Error: Unknown iterator'
				}
			)
		))
	})

	test('hash-check', async t => {
		const listName = 'lst', undefinedName = 'dne'
		await t.context.client.listCreate(listName)

		await Promise.all([listName, undefinedName].map(name =>
			Promise.all(
				[
					() => t.context.client.hashDrop(name),
					() => t.context.client.hashDelete(name, new Uint8Array),
					() => t.context.client.hashGet(name, new ArrayBuffer(0)),
					() => t.context.client.hashSet(name, new Uint8Array, new ArrayBuffer(0)),
					() => t.context.client.hashSize(name),
					() => t.context.client.hashIter(name)
				].map(action => t.throwsAsync(action, {
					instanceOf: ProtoDBError,
					message: `Error: Collection ${name} is not a hash`
				}))
			)
		))

		await t.throwsAsync(
			() => t.context.client.hashCreate(listName),
			{
				instanceOf: ProtoDBError,
				message: `Error: Collection ${listName} already exists`
			}
		)

		const name = 'already_created'
		await t.context.client.hashCreate(name)
		await t.throwsAsync(
			() => t.context.client.hashCreate(name),
			{
				instanceOf: ProtoDBError,
				message: `Error: Collection ${name} already exists`
			}
		)
	})

	test('hash-split-stress', async t => {
		const name = 'bigg'
		await t.context.client.hashCreate(name)

		const value = (key: number) => new Uint8Array(3000).fill(key)
		for (let i = 0; i < 256; i++) {
			await t.context.client.hashSet(name, new Uint8Array([i]), value(i))
		}

		for (let i = 0; i < 256; i++) {
			const result = await t.context.client.hashGet(name, new Uint8Array([i]))
			t.deepEqual(result, value(i))
		}
		const result = await t.context.client.hashGet(name, new Uint8Array(2))
		t.deepEqual(result, null)
	})
}
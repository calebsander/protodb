import {promises as fs} from 'fs'
import {TestInterface} from 'ava'
import {TestContext} from '../common'
import {ProtoDBError} from '../../client'
import {PAGE_SIZE} from '../../mmap-wrapper'
import {SortedKeyValuePair} from '../../pb/interface'
import {freePageType, headerType, LIST_END, nodeType} from '../../pb/sorted'

const toFloat = (f: number) => new Float32Array([f])[0]

function shuffle<T>(arr: T[]) {
	const {length} = arr
	for (let i = 0; i < length; i++) {
		const index = (Math.random() * (length - i)) | 0
		;[arr[i], arr[index]] = [arr[index], arr[i]]
	}
}
async function getDepth(context: TestContext, name: string): Promise<number> {
	const fd = await fs.open(context.getFile(`${name}.sorted`), 'r')
	const pageBuffer = new Uint8Array(PAGE_SIZE)
	await fd.read(pageBuffer, 0, PAGE_SIZE, 0)
	let page = headerType.toObject(
		headerType.decodeDelimited(pageBuffer),
		{longs: Number}
	).root
	let depth = 0
	while (true) {
		await fd.read(pageBuffer, 0, PAGE_SIZE, page * PAGE_SIZE)
		const node = nodeType.toObject(
			nodeType.decodeDelimited(pageBuffer),
			{defaults: true, longs: Number}
		)
		if ('leaf' in node) break

		[page] = node.inner.children
		depth++
	}
	await fd.close()
	return depth
}

async function getPagesInUse(context: TestContext, name: string): Promise<number> {
	const fd = await fs.open(context.getFile(`${name}.sorted`), 'r')
	const totalPages = async () => {
		const {size} = await fd.stat()
		return size / PAGE_SIZE
	}
	const freeListLength = async () => {
		const pageBuffer = new Uint8Array(PAGE_SIZE)
		await fd.read(pageBuffer, 0, PAGE_SIZE, 0)
		let {next} = headerType.toObject(headerType.decodeDelimited(pageBuffer))
			.freePage
		let length = 0
		while (next !== LIST_END) {
			await fd.read(pageBuffer, 0, PAGE_SIZE, next * PAGE_SIZE)
			;({next} = freePageType.toObject(freePageType.decodeDelimited(pageBuffer)))
			length++
		}
		return length
	}
	const [pages, freePages] = await Promise.all([totalPages(), freeListLength()])
	await fd.close()
	return pages - freePages
}

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

	test('sorted-delete', async t => {
		const name = 'stored-sorted'
		await t.context.client.sortedCreate(name)
		const getValue = (i: number) => new Uint8Array(100).map((_, j) => i * j)
		const keys = new Array(1e3).fill(0).map((_, i) => i * 2)
		shuffle(keys)
		for (const key of keys) {
			await t.context.client.sortedInsert(name, [{int: key}], getValue(key))
		}
		let size = await t.context.client.sortedSize(name)
		t.deepEqual(size, 1e3)

		const deleteKeys = new Array(500).fill(0).map((_, i) => 500 + i)
		shuffle(deleteKeys)
		for (const key of deleteKeys) {
			const doDelete = () => t.context.client.sortedDelete(name, [{int: key}])
			if (key >= 0 && key < 2e3 && key % 2 === 0) await doDelete()
			else {
				await t.throwsAsync(
					doDelete,
					{instanceOf: ProtoDBError, message: 'Error: No matching key'}
				)
			}
		}
		await t.throwsAsync(
			() => t.context.client.sortedDelete(name, [{int: -100}]),
			{instanceOf: ProtoDBError, message: 'Error: No matching key'}
		)
		await t.throwsAsync(
			() => t.context.client.sortedDelete(name, [{int: 2100}]),
			{instanceOf: ProtoDBError, message: 'Error: No matching key'}
		)
		size = await t.context.client.sortedSize(name)
		t.deepEqual(size, 750)

		for (let i = 0; i < 2e3; i += 2) {
			const key = [{int: i}]
			const result = await t.context.client.sortedGet(name, key)
			t.deepEqual(result, i < 500 || i >= 1e3 ? [{key, value: getValue(i)}] : [])
		}
		const result = await t.context.client.sortedGet(name, [])
		t.deepEqual(
			result,
			keys
				.filter(i => i < 500 || i >= 1e3)
				.sort((a, b) => a - b)
				.map(i => ({key: [{int: i}], value: getValue(i)}))
		)
	})

	test('sorted-reclaim', async t => {
		const name = 'sordid'
		await t.context.client.sortedCreate(name)
		const getKey = (i: number) => [
			{string: String.fromCharCode('A'.charCodeAt(0) + i % 26).repeat(100)},
			{int: ~(i / 26)}
		]
		const getValue = (i: number) => new Uint8Array(100).fill(i * i)
		const elements = new Array(2e3).fill(0).map((_, i) => i)
		for (let _ = 0; _ < 2; _++) {
			for (let i = 0; i < 2e3; i++) {
				await t.context.client.sortedInsert(name, getKey(i), getValue(i))
			}
			let size = await t.context.client.sortedSize(name)
			t.deepEqual(size, 2e3)
			t.deepEqual(await getDepth(t.context, name), 2)
			t.deepEqual(await getPagesInUse(t.context, name), 198)
			shuffle(elements)
			for (const i of elements) {
				const key = getKey(i)
				const result = await t.context.client.sortedGet(name, key)
				t.deepEqual(result, [{key, value: getValue(i)}])
				await t.context.client.sortedDelete(name, key)
			}
			size = await t.context.client.sortedSize(name)
			t.deepEqual(size, 0)
			t.deepEqual(await getDepth(t.context, name), 0)
			t.deepEqual(await getPagesInUse(t.context, name), 2)
			const result = await t.context.client.sortedGet(name, [])
			t.deepEqual(result, [])
		}
	})

	test('sorted-multilevel', async t => {
		const name = 'big-sort'
		await t.context.client.sortedCreate(name)
		// Only one key will fit in each inner node
		const getKey = (i: number) =>
			[{string: String.fromCharCode(256 + i).repeat(1500)}]
		const getValue = (i: number) => new Uint8Array(new Int32Array([i]).buffer)
		const elements = new Array(1e3).fill(0).map((_, i) => i)
		for (let _ = 0; _ < 3; _++) {
			shuffle(elements)
			for (const i of elements) {
				await t.context.client.sortedInsert(name, getKey(i), getValue(i))
			}
			let size = await t.context.client.sortedSize(name)
			t.deepEqual(size, 1e3)
			shuffle(elements)
			for (const i of elements) {
				const key = getKey(i)
				const result = await t.context.client.sortedGet(name, key)
				t.deepEqual(result, [{key, value: getValue(i)}])
				await t.context.client.sortedDelete(name, key)
			}
			size = await t.context.client.sortedSize(name)
			t.deepEqual(size, 0)
			const result = await t.context.client.sortedGet(name, [])
			t.deepEqual(result, [])
		}
	})

	test('sorted-iter', async t => {
		const name = 'iterrr'
		await t.context.client.sortedCreate(name)
		const getValue = (i: number) => new Uint8Array(10).map((_, j) => i ** j)
		for (let i = 0; i < 1e3; i++) {
			await t.context.client.sortedInsert(name, [], getValue(i))
		}
		const makePairRange = (i: number, length: number) =>
			new Array(length).fill(0).map((_, j) => {
				const index = 999 - (i + j)
				return {key: [{uniquifier: index}], value: getValue(index)}
			})
		async function getAll(iter: Uint8Array) {
			const pairs: SortedKeyValuePair[] = []
			while (true) {
				const pair = await t.context.client.sortedIterNext(iter)
				if (!pair) break
				pairs.push(pair)
			}
			return pairs
		}
		const tryOperations = () => Promise.all(
			[
				() => t.context.client.sortedDrop(name),
				() => t.context.client.sortedDelete(name, []),
				() => t.context.client.sortedInsert(name, [], new ArrayBuffer(0))
			].map(action => t.throwsAsync(action, {
				instanceOf: ProtoDBError,
				message: `Error: Collection ${name} has active iterators`
			}))
		)
		let iter = await t.context.client.sortedIter(name)
		await tryOperations()
		t.deepEqual(await getAll(iter), makePairRange(0, 1e3))
		iter = await t.context.client.sortedIter(name, [{uniquifier: 499}])
		await tryOperations()
		t.deepEqual(await getAll(iter), makePairRange(500, 500))
		iter = await t.context.client.sortedIter(name, undefined, [{uniquifier: 499}])
		await tryOperations()
		t.deepEqual(await getAll(iter), makePairRange(0, 500))
		iter = await t.context.client.sortedIter(
			name, undefined, [{uniquifier: 499}], true
		)
		await tryOperations()
		t.deepEqual(await getAll(iter), makePairRange(0, 501))
		iter = await t.context.client.sortedIter(
			name, [{uniquifier: 899}], [{uniquifier: 399}]
		)
		await tryOperations()
		t.deepEqual(await getAll(iter), makePairRange(100, 500))
		iter = await t.context.client.sortedIter(
			name, [{uniquifier: 899}], [{uniquifier: 399}], true
		)
		await tryOperations()
		t.deepEqual(await getAll(iter), makePairRange(100, 501))
		iter = await t.context.client.sortedIter(name)
		await tryOperations()
		const result = await t.context.client.sortedIterNext(iter)
		t.deepEqual(result, makePairRange(0, 1)[0])
		await t.context.client.sortedIterBreak(iter)

		// Modifications should now succeed
		await t.context.client.sortedDrop(name)

		await t.throwsAsync(
			() => t.context.client.sortedIterBreak(iter),
			{
				instanceOf: ProtoDBError,
				message: 'Error: Unknown iterator'
			}
		)
	})

	test('sorted-check', async t => {
		const listName = 'lost', undefinedName = 'absent'
		await t.context.client.listCreate(listName)

		await Promise.all([listName, undefinedName].map(name =>
			Promise.all(
				[
					() => t.context.client.sortedDrop(name),
					() => t.context.client.sortedDelete(name, []),
					() => t.context.client.sortedGet(name, []),
					() => t.context.client.sortedInsert(name, [], new Uint8Array(3)),
					() => t.context.client.sortedSize(name),
					() => t.context.client.sortedIter(name)
				].map(action => t.throwsAsync(action, {
					instanceOf: ProtoDBError,
					message: `Error: Collection ${name} is not a sorted map`
				}))
			)
		))

		const name = 'existing'
		await t.context.client.sortedCreate(name)
		await t.throwsAsync(
			() => t.context.client.sortedCreate(name),
			{
				instanceOf: ProtoDBError,
				message: `Error: Collection ${name} already exists`
			}
		)

		const invalidKey = {
			instanceOf: ProtoDBError,
			message: 'Error: Key types do not match'
		}
		await t.context.client.sortedInsert(name, [{int: 2}], new Uint8Array)
		await t.throwsAsync(
			() => t.context.client.sortedInsert(name, [{float: -3}], new Uint8Array),
			invalidKey
		)
		await t.context.client.sortedDelete(name, [])

		await t.context.client.sortedInsert(name, [{float: 1.5}], new Uint8Array)
		await t.throwsAsync(
			() => t.context.client.sortedInsert(name, [{string: 'a'}], new Uint8Array),
			invalidKey
		)
		await t.context.client.sortedDelete(name, [])

		await t.context.client.sortedInsert(name, [{string: ''}], new Uint8Array)
		await t.throwsAsync(
			() => t.context.client.sortedInsert(name, [{int: 5}], new Uint8Array),
			invalidKey
		)
		await t.context.client.sortedInsert(name, [{string: ''}], new Uint8Array)
		await t.throwsAsync(
			() => t.context.client.sortedGet(name, [{string: ''}, {int: 2}]),
			invalidKey
		)

		await t.throwsAsync(
			() => t.context.client.sortedInsert(
				name, [{uniquifier: 1}, {int: 0}], new Uint8Array
			),
			{
				instanceOf: ProtoDBError,
				message: 'Error: Key cannot include uniquifier'
			}
		)
	})
}
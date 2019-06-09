import {promises as fs} from 'fs'
import {TestInterface} from 'ava'
import {TestContext} from '../common'
import {ProtoDBError} from '../../client'
import {PAGE_SIZE} from '../../mmap-wrapper'
import {FREE_LIST_END, freePageType, headerType, nodeType} from '../../pb/list'

async function getDepth(context: TestContext, name: string): Promise<number> {
	const fd = await fs.open(context.getFile(`${name}.list`), 'r')
	const pageBuffer = new Uint8Array(PAGE_SIZE)
	await fd.read(pageBuffer, 0, PAGE_SIZE, 0)
	let {page} = headerType.toObject(
		headerType.decodeDelimited(pageBuffer),
		{longs: Number}
	).child
	let depth = 0
	while (true) {
		await fd.read(pageBuffer, 0, PAGE_SIZE, page * PAGE_SIZE)
		const node = nodeType.toObject(
			nodeType.decodeDelimited(pageBuffer),
			{defaults: true, longs: Number}
		)
		if ('leaf' in node) break

		[{page}] = node.inner.children
		depth++
	}
	await fd.close()
	return depth
}

async function getPagesInUse(context: TestContext, name: string): Promise<number> {
	const fd = await fs.open(context.getFile(`${name}.list`), 'r')
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
		while (next !== FREE_LIST_END) {
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
	test('list-get-set', async t => {
		const name = 'arr'
		await t.context.client.listCreate(name)
		for (let i = 0; i < 1e3; i++) {
			await t.context.client.listInsert(name, new Uint8Array(10))
		}
		const getValue = (i: number) => new Uint8Array(20).map((_, j) => i + j)
		for (let i = 999; i >= 0; i--) {
			await t.context.client.listSet(name, i, getValue(i))
		}
		for (let i = 0; i < 1e3; i++) {
			const result = await t.context.client.listGet(name, i)
			t.deepEqual(result, getValue(i))
		}
		await t.context.client.listDrop(name)
	})

	test('list-stack', async t => {
		const name = 'stck'
		await t.context.client.listCreate(name)
		const getValue = (i: number) =>
			new Uint8Array(new Int32Array(10).fill(i).buffer)
		for (let i = 0; i < 1e3; i++) {
			await t.context.client.listInsert(name, getValue(i * 2))
			await t.context.client.listInsert(name, getValue(i * 2 + 1))
			const popped = await t.context.client.listGet(name, -1)
			t.deepEqual(popped, getValue(i * 2 + 1))
			await t.context.client.listDelete(name)
			const size = await t.context.client.listSize(name)
			t.deepEqual(size, i + 1)
		}
		for (let i = 999; i >= 0; i--) {
			const popped = await t.context.client.listGet(name, -1)
			t.deepEqual(popped, getValue(i * 2))
			await t.context.client.listDelete(name)
			const size = await t.context.client.listSize(name)
			t.deepEqual(size, i)
		}
		await t.context.client.listDrop(name)
	})

	test('list-queue', async t => {
		const name = 'q'
		await t.context.client.listCreate(name)
		const getValue = (i: number) =>
			new Uint8Array(new Int32Array(10).fill(i).buffer)
		for (let i = 0; i < 1e3; i++) {
			await t.context.client.listInsert(name, getValue(i * 2))
			await t.context.client.listInsert(name, getValue(i * 2 + 1))
			const dequeued = await t.context.client.listGet(name, 0)
			t.deepEqual(dequeued, getValue(i))
			await t.context.client.listDelete(name, 0)
			const size = await t.context.client.listSize(name)
			t.deepEqual(size, i + 1)
		}
		for (let i = 0; i < 1e3; i++) {
			const dequeued = await t.context.client.listGet(name, 0)
			t.deepEqual(dequeued, getValue(1e3 + i))
			await t.context.client.listDelete(name, 0)
			const size = await t.context.client.listSize(name)
			t.deepEqual(size, 999 - i)
		}
		await t.context.client.listDrop(name)
	})

	test('list-reclaim', async t => {
		const name = 'shrinky'
		await t.context.client.listCreate(name)
		const value = (i: number) =>
			new Uint8Array(new Float64Array([i, i + 1]).buffer)

		// Add 1000 elements to the list
		for (let i = 0; i < 1e3; i++) {
			await t.context.client.listInsert(name, value(i))
		}
		t.deepEqual(await getDepth(t.context, name), 1)
		t.deepEqual(await getPagesInUse(t.context, name), 10)
		const {size} = await fs.stat(t.context.getFile(`${name}.list`))

		// Randomly remove the elements
		for (let i = 1e3; i > 0; i--) {
			await t.context.client.listDelete(name, (Math.random() * i) | 0)
			const result = await t.context.client.listSize(name)
			t.deepEqual(result, i - 1)
		}
		t.deepEqual(await getDepth(t.context, name), 0)
		t.deepEqual(await getPagesInUse(t.context, name), 2)

		// Add elements back to the list
		for (let i = 0; i < 1e3; i++) {
			await t.context.client.listInsert(name, value(i), 0)
		}
		t.deepEqual(await getDepth(t.context, name), 1)
		t.deepEqual(await getPagesInUse(t.context, name), 10)

		// Remove elements, ensure list file hasn't grown
		for (let i = 1e3; i > 0; i--) {
			await t.context.client.listDelete(name, (Math.random() * i) | 0)
			const result = await t.context.client.listSize(name)
			t.deepEqual(result, i - 1)
		}
		t.deepEqual(await getDepth(t.context, name), 0)
		t.deepEqual(await getPagesInUse(t.context, name), 2)
		const newStat = await fs.stat(t.context.getFile(`${name}.list`))
		t.deepEqual(newStat.size, size)
	})

	test('list-multilevel', async t => {
		const name = 'bigger'
		await t.context.client.listCreate(name)
		// Only one value will fit in each leaf
		const getValue = (i: number) => new Uint8Array(3e3 + i % 10).fill(i)
		const values: number[] = []
		while (values.length < 2e3) {
			const index = (Math.random() * (values.length + 1)) | 0
			await t.context.client.listInsert(name, getValue(values.length), index)
			values.splice(index, 0, values.length)
		}
		for (let i = 0; i < 2e3; i++) {
			const result = await t.context.client.listGet(name, i)
			t.deepEqual(result, getValue(values[i]))
		}
		t.deepEqual(await getDepth(t.context, name), 2)
		t.deepEqual(await getPagesInUse(t.context, name), 2006) // 2000 leaves, 5 inner pages, and header
		while (values.length) {
			// Sample a random value and delete it
			const index = (Math.random() * values.length) | 0
			const value = await t.context.client.listGet(name, index)
			t.deepEqual(value, getValue(values[index]))
			await t.context.client.listDelete(name, index)
			values.splice(index, 1)
			const size = await t.context.client.listSize(name)
			t.deepEqual(size, values.length)
		}
		// Coalescing isn't always perfect, but it should reclaim most of the pages
		t.assert(await getDepth(t.context, name) <= 1)
		t.assert(await getPagesInUse(t.context, name) < 15)
	})

	test('list-iter', async t => {
		const name = 'lit'
		await t.context.client.listCreate(name)
		const getValue = (i: number) =>
			new Uint8Array(new Float64Array([i, i + 1]).buffer)
		for (let i = 0; i < 1e3; i++) {
			await t.context.client.listInsert(name, getValue(i))
		}

		const tryOperations = () => Promise.all(
			[
				() => t.context.client.listDrop(name),
				() => t.context.client.listDelete(name),
				() => t.context.client.listInsert(name, new ArrayBuffer(3)),
				() => t.context.client.listSet(name, 10, new Uint8Array(1))
			].map(action => t.throwsAsync(action, {
				instanceOf: ProtoDBError,
				message: `Error: Collection ${name} has active iterators`
			}))
		)

		let allIter = await t.context.client.listIter(name)
		await tryOperations()
		for (let i = 0; i < 1e3; i++) {
			const result = await t.context.client.listIterNext(allIter)
			t.deepEqual(result, getValue(i))
		}
		let result = await t.context.client.listIterNext(allIter)
		t.deepEqual(result, null)

		await Promise.all(new Array(10).fill(0).map(async (_, i) => {
			const start = i * 100, end = start + 100
			const iter = await t.context.client.listIter(name, start, end)
			await tryOperations()
			for (let i = 0; i < 100; i++) {
				const result = await t.context.client.listIterNext(iter)
				t.deepEqual(result, getValue(start + i))
			}
			const result = await t.context.client.listIterNext(iter)
			t.deepEqual(result, null)
		}))

		allIter = await t.context.client.listIter(name, 0)
		await tryOperations()
		result = await t.context.client.listIterNext(allIter)
		t.deepEqual(result, getValue(0))
		await t.context.client.listIterBreak(allIter)

		// No more active iterators, so modifications should succeed
		await t.context.client.listDelete(name)
	})

	test('list-check', async t => {
		const itemName = 'itm', undefinedName = 'not-a-thing'
		await t.context.client.itemCreate(itemName)

		await Promise.all([itemName, undefinedName].map(name =>
			Promise.all(
				[
					() => t.context.client.listDrop(name),
					() => t.context.client.listDelete(name),
					() => t.context.client.listGet(name, 0),
					() => t.context.client.listInsert(name, new Uint8Array),
					() => t.context.client.listSet(name, 0, new ArrayBuffer(0)),
					() => t.context.client.listSize(name),
					() => t.context.client.listIter(name)
				].map(action => t.throwsAsync(action, {
					instanceOf: ProtoDBError,
					message: `Error: Collection ${name} is not a list`
				}))
			)
		))

		await t.throwsAsync(
			() => t.context.client.listCreate(itemName),
			{
				instanceOf: ProtoDBError,
				message: `Error: Collection ${itemName} already exists`
			}
		)

		const name = 'existing'
		await t.context.client.listCreate(name)
		await t.throwsAsync(
			() => t.context.client.listCreate(name),
			{
				instanceOf: ProtoDBError,
				message: `Error: Collection ${name} already exists`
			}
		)
	})

	test('list-valid-indices', async t => {
		const name = 'boundaries'
		const length = 3
		const getValue = (i: number) => new Uint8Array([i])
		const withList = async (run: () => Promise<void>) => {
			await t.context.client.listCreate(name)
			for (let i = 0; i < length; i++) {
				await t.context.client.listInsert(name, getValue(i))
			}
			await run()
			await t.context.client.listDrop(name)
		}

		await withList(async () => {
			for (let i = length * -2; i < -length; i++) {
				await t.throwsAsync(
					() => t.context.client.listGet(name, i),
					{
						instanceOf: ProtoDBError,
						message: `Error: Index ${i} is out of bounds in list of size ${length}`
					}
				)
			}
			for (let i = -length; i < 0; i++) {
				const result = await t.context.client.listGet(name, i)
				t.deepEqual(result, getValue(length + i))
			}
			for (let i = 0; i < length; i++) {
				const result = await t.context.client.listGet(name, i)
				t.deepEqual(result, getValue(i))
			}
			for (let i = length; i < length * 2; i++) {
				await t.throwsAsync(
					() => t.context.client.listGet(name, i),
					{
						instanceOf: ProtoDBError,
						message: `Error: Index ${i} is out of bounds in list of size ${length}`
					}
				)
			}
		})

		await withList(async () => {
			for (let i = length * -2; i < -length; i++) {
				await t.throwsAsync(
					() => t.context.client.listSet(name, i, new Uint8Array),
					{
						instanceOf: ProtoDBError,
						message: `Error: Index ${i} is out of bounds in list of size ${length}`
					}
				)
			}
			for (let i = -length; i < length; i++) {
				await t.context.client.listSet(name, i, new Uint8Array)
			}
			for (let i = length; i < length * 2; i++) {
				await t.throwsAsync(
					() => t.context.client.listSet(name, i, new Uint8Array),
					{
						instanceOf: ProtoDBError,
						message: `Error: Index ${i} is out of bounds in list of size ${length}`
					}
				)
			}
		})

		await withList(async () => {
			for (let i = length * -2; i < -length; i++) {
				await t.throwsAsync(
					() => t.context.client.listInsert(name, new Uint8Array, i),
					{
						instanceOf: ProtoDBError,
						message: `Error: Index ${i} is out of bounds in list of size ${length}`
					}
				)
			}
			let newLength = length
			await t.context.client.listInsert(name, new Uint8Array, -newLength++)
			await t.context.client.listInsert(name, new Uint8Array, -1)
			newLength++
			await t.context.client.listInsert(name, new Uint8Array, 0)
			newLength++
			await t.context.client.listInsert(name, new Uint8Array, newLength++)
			for (let i = newLength + 1; i < newLength * 2; i++) {
				await t.throwsAsync(
					() => t.context.client.listInsert(name, new Uint8Array, i),
					{
						instanceOf: ProtoDBError,
						message: `Error: Index ${i} is out of bounds in list of size ${newLength}`
					}
				)
			}
		})
	})
}
import fs from 'fs'
import {promisify} from 'util'
import {TestInterface} from 'ava'
import {TestContext} from '../common'
import {PAGE_SIZE} from '../../mmap-wrapper'
import {FREE_LIST_END, freePageType, headerType, nodeType} from '../../pb/list'
import {
	bytesResponseType,
	iterResponseType,
	optionalBytesResponseType,
	sizeResponseType,
	voidResponseType
} from '../../pb/request'

const close = promisify(fs.close),
      fstat = promisify(fs.fstat),
      open = promisify(fs.open),
      read = promisify(fs.read),
      stat = promisify(fs.stat)

async function getDepth(context: TestContext, name: string): Promise<number> {
	const fd = await open(context.getFile(`${name}.list`), 'r')
	const pageBuffer = new Uint8Array(PAGE_SIZE)
	await read(fd, pageBuffer, 0, PAGE_SIZE, 0)
	let {page} = headerType.toObject(
		headerType.decodeDelimited(pageBuffer),
		{longs: Number}
	).child
	let depth = 0
	while (true) {
		await read(fd, pageBuffer, 0, PAGE_SIZE, page * PAGE_SIZE)
		const node = nodeType.toObject(
			nodeType.decodeDelimited(pageBuffer),
			{defaults: true, longs: Number}
		)
		if ('leaf' in node) break

		[{page}] = node.inner.children
		depth++
	}
	await close(fd)
	return depth
}

async function getPagesInUse(context: TestContext, name: string): Promise<number> {
	const fd = await open(context.getFile(`${name}.list`), 'r')
	const totalPages = async () => {
		const {size} = await fstat(fd)
		return size / PAGE_SIZE
	}
	const freeListLength = async () => {
		const pageBuffer = new Uint8Array(PAGE_SIZE)
		await read(fd, pageBuffer, 0, PAGE_SIZE, 0)
		let {next} = headerType.toObject(
			headerType.decodeDelimited(pageBuffer),
			{longs: Number}
		).freePage
		let length = 0
		while (next !== FREE_LIST_END) {
			await read(fd, pageBuffer, 0, PAGE_SIZE, next * PAGE_SIZE)
			;({next} = freePageType.toObject(freePageType.decodeDelimited(pageBuffer)))
			length++
		}
		return length
	}
	const [pages, freePages] = await Promise.all([totalPages(), freeListLength()])
	await close(fd)
	return pages - freePages
}

export default (test: TestInterface<TestContext>) => {
	test('list-get-set', async t => {
		const name = 'arr'
		let result = await t.context.sendCommand(
			{listCreate: {name}},
			voidResponseType
		)
		t.deepEqual(result, {})
		for (let i = 0; i < 1e3; i++) {
			result = await t.context.sendCommand(
				{listInsert: {name, index: {none: {}}, value: new Uint8Array(10)}},
				voidResponseType
			)
			t.deepEqual(result, {})
		}
		const getValue = (i: number) => new Uint8Array(20).map((_, j) => i + j)
		for (let i = 999; i >= 0; i--) {
			result = await t.context.sendCommand(
				{listSet: {name, index: i, value: getValue(i)}},
				voidResponseType
			)
			t.deepEqual(result, {})
		}
		for (let i = 0; i < 1e3; i++) {
			const result = await t.context.sendCommand(
				{listGet: {name, index: i}},
				bytesResponseType
			)
			t.deepEqual(result, {data: getValue(i)})
		}
		result = await t.context.sendCommand({listDrop: {name}}, voidResponseType)
		t.deepEqual(result, {})
	})

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

	test('list-reclaim', async t => {
		const name = 'shrinky'
		let result = await t.context.sendCommand(
			{listCreate: {name}},
			voidResponseType
		)
		t.deepEqual(result, {})
		const value = (i: number) =>
			new Uint8Array(new Float64Array([i, i + 1]).buffer)

		// Add 1000 elements to the list
		for (let i = 0; i < 1e3; i++) {
			result = await t.context.sendCommand(
				{listInsert: {name, index: {none: {}}, value: value(i)}},
				voidResponseType
			)
			t.deepEqual(result, {})
		}
		t.deepEqual(await getDepth(t.context, name), 1)
		t.deepEqual(await getPagesInUse(t.context, name), 10)
		const {size} = await stat(t.context.getFile(`${name}.list`))

		// Randomly remove the elements
		for (let i = 1e3; i > 0; i--) {
			result = await t.context.sendCommand(
				{listDelete: {name, index: {value: (Math.random() * i) | 0}}},
				voidResponseType
			)
			t.deepEqual(result, {})
			{
				const result = await t.context.sendCommand(
					{listSize: {name}},
					sizeResponseType
				)
				t.deepEqual(result, {size: i - 1})
			}
		}
		t.deepEqual(await getDepth(t.context, name), 0)
		t.deepEqual(await getPagesInUse(t.context, name), 2)

		// Add elements back to the list
		for (let i = 0; i < 1e3; i++) {
			result = await t.context.sendCommand(
				{listInsert: {name, index: {value: 0}, value: value(i)}},
				voidResponseType
			)
			t.deepEqual(result, {})
		}
		t.deepEqual(await getDepth(t.context, name), 1)
		t.deepEqual(await getPagesInUse(t.context, name), 10)

		// Remove elements, ensure list file hasn't grown
		for (let i = 1e3; i > 0; i--) {
			result = await t.context.sendCommand(
				{listDelete: {name, index: {value: (Math.random() * i) | 0}}},
				voidResponseType
			)
			t.deepEqual(result, {})
			{
				const result = await t.context.sendCommand(
					{listSize: {name}},
					sizeResponseType
				)
				t.deepEqual(result, {size: i - 1})
			}
		}
		t.deepEqual(await getDepth(t.context, name), 0)
		t.deepEqual(await getPagesInUse(t.context, name), 2)
		const newStat = await stat(t.context.getFile(`${name}.list`))
		t.deepEqual(newStat.size, size)
	})

	test('list-multilevel', async t => {
		const name = 'bigger'
		let result = await t.context.sendCommand(
			{listCreate: {name}},
			voidResponseType
		)
		t.deepEqual(result, {})
		// Only one value will fit in each leaf
		const getValue = (i: number) => new Uint8Array(3e3 + i % 10).fill(i)
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
		t.deepEqual(await getDepth(t.context, name), 2)
		t.deepEqual(await getPagesInUse(t.context, name), 2006) // 2000 leaves, 5 inner pages, and header
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
		// Coalescing isn't always perfect, but it should reclaim most of the pages
		t.assert(await getDepth(t.context, name) <= 1)
		t.assert(await getPagesInUse(t.context, name) < 15)
	})

	test('list-iter', async t => {
		const name = 'lit'
		let result = await t.context.sendCommand(
			{listCreate: {name}},
			voidResponseType
		)
		t.deepEqual(result, {})
		const value = (i: number) =>
			new Uint8Array(new Float64Array([i, i + 1]).buffer)
		for (let i = 0; i < 1e3; i++) {
			result = await t.context.sendCommand(
				{listInsert: {name, index: {none: {}}, value: value(i)}},
				voidResponseType
			)
			t.deepEqual(result, {})
		}

		const tryOperations = async () => {
			result = await t.context.sendCommand(
				{listDelete: {name, index: {none: {}}}},
				voidResponseType
			)
			t.deepEqual(result, {error: `Error: Collection ${name} has active iterators`})
			result = await t.context.sendCommand(
				{listInsert: {name, index: {none: {}}, value: new Uint8Array(3)}},
				voidResponseType
			)
			t.deepEqual(result, {error: `Error: Collection ${name} has active iterators`})
			result = await t.context.sendCommand(
				{listSet: {name, index: 10, value: new Uint8Array(1)}},
				voidResponseType
			)
			t.deepEqual(result, {error: `Error: Collection ${name} has active iterators`})
		}

		let allIter: Uint8Array
		{
			const result = await t.context.sendCommand(
				{listIter: {name, start: {none: {}}, end: {none: {}}}},
				iterResponseType
			)
			if ('error' in result) throw new Error(`Iter failed: ${result.error}`)
			allIter = result.iter
		}
		await tryOperations()
		for (let i = 0; i < 1e3; i++) {
			const result = await t.context.sendCommand(
				{listIterNext: {iter: allIter}},
				optionalBytesResponseType
			)
			t.deepEqual(result, {data: value(i)})
		}
		{
			const result = await t.context.sendCommand(
				{listIterNext: {iter: allIter}},
				optionalBytesResponseType
			)
			t.deepEqual(result, {none: {}})
		}

		await Promise.all(new Array(10).fill(0).map(async (_, i) => {
			const start = i * 100, end = start + 100
			let iter: Uint8Array
			{
				const result = await t.context.sendCommand(
					{listIter: {name, start: {value: start}, end: {value: end}}},
					iterResponseType
				)
				if ('error' in result) throw new Error(`Iter failed: ${result.error}`)
				;({iter} = result)
			}
			await tryOperations()
			for (let i = 0; i < 100; i++) {
				const result = await t.context.sendCommand(
					{listIterNext: {iter}},
					optionalBytesResponseType
				)
				t.deepEqual(result, {data: value(start + i)})
			}
			{
				const result = await t.context.sendCommand(
					{listIterNext: {iter}},
					optionalBytesResponseType
				)
				t.deepEqual(result, {none: {}})
			}
		}))

		{
			const result = await t.context.sendCommand(
				{listIter: {name, start: {value: 0}, end: {none: {}}}},
				iterResponseType
			)
			if ('error' in result) throw new Error(`Iter failed: ${result.error}`)
			allIter = result.iter
		}
		await tryOperations()
		{
			const result = await t.context.sendCommand(
				{listIterNext: {iter: allIter}},
				optionalBytesResponseType
			)
			t.deepEqual(result, {data: value(0)})
		}
		result = await t.context.sendCommand(
			{listIterBreak: {iter: allIter}},
			voidResponseType
		)
		t.deepEqual(result, {})

		// No more active iterators, so modifications should succeed
		result = await t.context.sendCommand(
			{listDelete: {name, index: {none: {}}}},
			voidResponseType
		)
		t.deepEqual(result, {})
	})

	test('list-check', async t => {
		const itemName = 'itm', undefinedName = 'not-a-thing'
		let result = await t.context.sendCommand(
			{itemCreate: {name: itemName}},
			voidResponseType
		)
		t.deepEqual(result, {})

		await Promise.all([itemName, undefinedName].map(name => {
			const errorResult = {error: `Error: Collection ${name} is not a list`}
			return Promise.all([
				t.context.sendCommand({listDrop: {name}}, voidResponseType)
					.then(result => t.deepEqual(result, errorResult)),
				t.context.sendCommand(
					{listDelete: {name, index: {none: {}}}},
					voidResponseType
				)
					.then(result => t.deepEqual(result, errorResult)),
				t.context.sendCommand(
					{listGet: {name, index: 0}},
					bytesResponseType
				)
					.then(result => t.deepEqual(result, errorResult)),
				t.context.sendCommand(
					{listInsert: {name, index: {none: {}}, value: new Uint8Array}},
					voidResponseType
				)
					.then(result => t.deepEqual(result, errorResult)),
				t.context.sendCommand(
					{listSet: {name, index: 0, value: new Uint8Array}},
					voidResponseType
				)
					.then(result => t.deepEqual(result, errorResult)),
				t.context.sendCommand({listSize: {name}}, sizeResponseType)
					.then(result => t.deepEqual(result, errorResult)),
				t.context.sendCommand(
					{listIter: {name, start: {none: {}}, end: {none: {}}}},
					iterResponseType
				)
					.then(result => t.deepEqual(result, errorResult))
			])
		}))

		result = await t.context.sendCommand(
			{listCreate: {name: itemName}},
			voidResponseType
		)
		t.deepEqual(result, {error: `Error: Collection ${itemName} already exists`})

		const name = 'existing'
		result = await t.context.sendCommand({listCreate: {name}}, voidResponseType)
		t.deepEqual(result, {})
		result = await t.context.sendCommand({listCreate: {name}}, voidResponseType)
		t.deepEqual(result, {error: `Error: Collection ${name} already exists`})
	})

	test('list-valid-indices', async t => {
		const name = 'boundaries'
		const length = 3
		const getValue = (i: number) => new Uint8Array([i])
		const withList = async (run: () => Promise<void>) => {
			let result = await t.context.sendCommand(
				{listCreate: {name}},
				voidResponseType
			)
			t.deepEqual(result, {})
			for (let i = 0; i < length; i++) {
				const result = await t.context.sendCommand(
					{listInsert: {name, index: {none: {}}, value: getValue(i)}},
					voidResponseType
				)
				t.deepEqual(result, {})
			}
			await run()
			result = await t.context.sendCommand({listDrop: {name}}, voidResponseType)
			t.deepEqual(result, {})
		}

		await withList(async () => {
			for (let i = length * -2; i < -length; i++) {
				const result = await t.context.sendCommand(
					{listGet: {name, index: i}},
					bytesResponseType
				)
				t.deepEqual(result, {
					error: `Error: Index ${i} is out of bounds in list of size ${length}`
				})
			}
			for (let i = -length; i < 0; i++) {
				const result = await t.context.sendCommand(
					{listGet: {name, index: i}},
					bytesResponseType
				)
				t.deepEqual(result, {data: getValue(i + length)})
			}
			for (let i = 0; i < length; i++) {
				const result = await t.context.sendCommand(
					{listGet: {name, index: i}},
					bytesResponseType
				)
				t.deepEqual(result, {data: getValue(i)})
			}
			for (let i = length; i < length * 2; i++) {
				const result = await t.context.sendCommand(
					{listGet: {name, index: i}},
					bytesResponseType
				)
				t.deepEqual(result, {
					error: `Error: Index ${i} is out of bounds in list of size ${length}`
				})
			}
		})

		await withList(async () => {
			for (let i = length * -2; i < -length; i++) {
				const result = await t.context.sendCommand(
					{listSet: {name, index: i, value: new Uint8Array}},
					voidResponseType
				)
				t.deepEqual(result, {
					error: `Error: Index ${i} is out of bounds in list of size ${length}`
				})
			}
			for (let i = -length; i < length; i++) {
				const result = await t.context.sendCommand(
					{listSet: {name, index: i, value: new Uint8Array}},
					voidResponseType
				)
				t.deepEqual(result, {})
			}
			for (let i = length; i < length * 2; i++) {
				const result = await t.context.sendCommand(
					{listSet: {name, index: i, value: new Uint8Array}},
					voidResponseType
				)
				t.deepEqual(result, {
					error: `Error: Index ${i} is out of bounds in list of size ${length}`
				})
			}
		})

		await withList(async () => {
			for (let i = length * -2; i < -length; i++) {
				const result = await t.context.sendCommand(
					{listInsert: {name, index: {value: i}, value: new Uint8Array}},
					voidResponseType
				)
				t.deepEqual(result, {
					error: `Error: Index ${i} is out of bounds in list of size ${length}`
				})
			}
			let newLength = length
			let result = await t.context.sendCommand(
				{listInsert: {name, index: {value: -newLength++}, value: new Uint8Array}},
				voidResponseType
			)
			t.deepEqual(result, {})
			result = await t.context.sendCommand(
				{listInsert: {name, index: {value: -1}, value: new Uint8Array}},
				voidResponseType
			)
			t.deepEqual(result, {})
			newLength++
			result = await t.context.sendCommand(
				{listInsert: {name, index: {value: 0}, value: new Uint8Array}},
				voidResponseType
			)
			t.deepEqual(result, {})
			newLength++
			result = await t.context.sendCommand(
				{listInsert: {name, index: {value: newLength++}, value: new Uint8Array}},
				voidResponseType
			)
			t.deepEqual(result, {})
			for (let i = newLength + 1; i < newLength * 2; i++) {
				const result = await t.context.sendCommand(
					{listInsert: {name, index: {value: i}, value: new Uint8Array}},
					voidResponseType
				)
				t.deepEqual(result, {
					error: `Error: Index ${i} is out of bounds in list of size ${newLength}`
				})
			}
		})
	})
}
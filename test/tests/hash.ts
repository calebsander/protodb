import {TestInterface} from 'ava'
import {TestContext} from '../common'
import {
	iterResponseType,
	optionalBytesResponseType,
	optionalPairResponseType,
	sizeResponseType,
	voidResponseType
} from '../../pb/request'
import {concat} from '../../util'

const randomBytes = (n: number) =>
	new Uint8Array(n).map(_ => Math.random() * 256)

export default (test: TestInterface<TestContext>) => {
	test('hash-small', async t => {
		const name = 'small'
		let result = await t.context.sendCommand(
			{hashCreate: {name}},
			voidResponseType
		)
		t.deepEqual(result, {})
		const data = new Array(20).fill(0).map(_ =>
			({key: randomBytes(10), value: randomBytes(50)})
		)
		await Promise.all(data.map(({key, value}) =>
			t.context.sendCommand(
				{hashSet: {name, key, value}},
				voidResponseType
			)
				.then(result => t.deepEqual(result, {}))
		))
		await Promise.all(data.map(({key, value}) =>
			t.context.sendCommand(
				{hashGet: {name, key}},
				optionalBytesResponseType
			)
				.then(result => t.deepEqual(result, {data: value}))
		))
		// Test keys that don't match
		for (let i = 0; i < 20; i++) {
			if (i === 10) continue

			const result = await t.context.sendCommand(
				{hashGet: {name, key: randomBytes(i)}},
				optionalBytesResponseType
			)
			t.deepEqual(result, {none: {}})
		}
		result = await t.context.sendCommand(
			{hashDrop: {name}},
			voidResponseType
		)
		t.deepEqual(result, {})
	})

	test('hash-large', async t => {
		const name = 'large'
		const result = await t.context.sendCommand(
			{hashCreate: {name}},
			voidResponseType
		)
		t.deepEqual(result, {})

		const getValue = (key: Uint8Array) =>
			concat(new Array<Uint8Array>(50).fill(key))
		for (let i = 0; i < 5e3; i++) {
			const key = new Uint8Array([...String(i)].map(Number))
			const result = await t.context.sendCommand(
				{hashSet: {name, key, value: getValue(key)}},
				voidResponseType
			)
			t.deepEqual(result, {})
		}
		{
			const result = await t.context.sendCommand(
				{hashSize: {name}},
				sizeResponseType
			)
			t.deepEqual(result, {size: 5e3})
		}
		for (let i = 0; i < 6e3; i++) {
			const key = new Uint8Array([...String(i)].map(Number))
			const result = await t.context.sendCommand(
				{hashGet: {name, key}},
				optionalBytesResponseType
			)
			t.deepEqual(result, i < 5e3 ? {data: getValue(key)} : {none: {}})
		}

		// Try deleting some keys
		for (let i = 3e3; i < 6e3; i++) {
			const key = new Uint8Array([...String(i)].map(Number))
			const result = await t.context.sendCommand(
				{hashDelete: {name, key}},
				voidResponseType
			)
			t.deepEqual(result, {})
		}
		{
			const result = await t.context.sendCommand(
				{hashSize: {name}},
				sizeResponseType
			)
			t.deepEqual(result, {size: 3e3})
		}
		for (let i = 0; i < 6e3; i++) {
			const key = new Uint8Array([...String(i)].map(Number))
			const result = await t.context.sendCommand(
				{hashGet: {name, key}},
				optionalBytesResponseType
			)
			t.deepEqual(result, i < 3e3 ? {data: getValue(key)} : {none: {}})
		}
	})

	test('hash-iter', async t => {
		const name = 'iterable'
		let result = await t.context.sendCommand(
			{hashCreate: {name}},
			voidResponseType
		)
		t.deepEqual(result, {})

		const getValue = (key: number) =>
			concat([Buffer.from('a'.repeat(50)), new Uint8Array([key])])
		const values = new Map<number, Uint8Array>()
		for (let key = 0; key < 100; key++) {
			const value = getValue(key)
			values.set(key, value)
			const result = await t.context.sendCommand(
				{hashSet: {name, key: new Uint8Array(key), value}},
				voidResponseType
			)
			t.deepEqual(result, {})
		}
		let iter1: Uint8Array, iter2: Uint8Array
		{
			let result = await t.context.sendCommand(
				{hashIter: {name}},
				iterResponseType
			)
			if ('error' in result) throw new Error(`Iter failed: ${result.error}`)
			iter1 = result.iter

			result = await t.context.sendCommand(
				{hashIter: {name}},
				iterResponseType
			)
			if ('error' in result) throw new Error(`Iter failed: ${result.error}`)
			iter2 = result.iter
		}

		const tryOperations = async () => {
			let result = await t.context.sendCommand(
				{hashDrop: {name}},
				voidResponseType
			)
			t.deepEqual(result, {error: `Error: Collection ${name} has active iterators`})
			result = await t.context.sendCommand(
				{hashDelete: {name, key: new Uint8Array(3)}},
				voidResponseType
			)
			t.deepEqual(result, {error: `Error: Collection ${name} has active iterators`})
			result = await t.context.sendCommand(
				{hashSet: {name, key: new Uint8Array(3), value: new Uint8Array(1)}},
				voidResponseType
			)
			t.deepEqual(result, {error: `Error: Collection ${name} has active iterators`})
		}
		await tryOperations()

		const iterSeen = new Set<number>()
		for (let i = 0; i < 50; i++) {
			const result1 = await t.context.sendCommand(
				{hashIterNext: {iter: iter1}},
				optionalPairResponseType
			)
			if (!('item' in result1 && result1.item)) throw new Error('Missing item')
			const {key, value} = result1.item
			t.deepEqual(value, values.get(key.length))
			const result2 = await t.context.sendCommand(
				{hashIterNext: {iter: iter2}},
				optionalPairResponseType
			)
			t.deepEqual(result2, result1) // both iterators should return the same order
			iterSeen.add(key.length)
		}

		// Break out of the second iterator
		result = await t.context.sendCommand(
			{hashIterBreak: {iter: iter2}},
			voidResponseType
		)
		t.deepEqual(result, {})
		result = await t.context.sendCommand(
			{hashIterBreak: {iter: iter2}},
			voidResponseType
		)
		t.deepEqual(result, {error: 'Error: Unknown iterator'})
		await tryOperations()

		// Iterate over the rest of the elements with the first iterator
		for (let i = 0; i < 50; i++) {
			const result = await t.context.sendCommand(
				{hashIterNext: {iter: iter1}},
				optionalPairResponseType
			)
			if (!('item' in result && result.item)) throw new Error('Missing item')
			const {key, value} = result.item
			t.deepEqual(value, values.get(key.length))
			iterSeen.add(key.length)
		}
		t.deepEqual(iterSeen.size, 100)
		{
			const result = await t.context.sendCommand(
				{hashIterNext: {iter: iter1}},
				optionalPairResponseType
			)
			t.deepEqual(result, {})
		}

		// Both iterators should now be invalid
		for (const iter of [iter1, iter2]) {
			const result = await t.context.sendCommand(
				{hashIterNext: {iter}},
				optionalPairResponseType
			)
			t.deepEqual(result, {error: 'Error: Unknown iterator'})
		}
	})

	test('hash-check', async t => {
		const listName = 'lst', undefinedName = 'dne'
		let result = await t.context.sendCommand(
			{listCreate: {name: listName}},
			voidResponseType
		)
		t.deepEqual(result, {})

		await Promise.all([listName, undefinedName].map(name => {
			const errorResult = {error: `Error: Collection ${name} is not a hash`}
			return Promise.all([
				t.context.sendCommand({hashDrop: {name}}, voidResponseType)
					.then(result => t.deepEqual(result, errorResult)),
				t.context.sendCommand(
					{hashDelete: {name, key: new Uint8Array}},
					voidResponseType
				)
					.then(result => t.deepEqual(result, errorResult)),
				t.context.sendCommand(
					{hashGet: {name, key: new Uint8Array}},
					optionalBytesResponseType
				)
					.then(result => t.deepEqual(result, errorResult)),
				t.context.sendCommand(
					{hashSet: {name, key: new Uint8Array, value: new Uint8Array}},
					voidResponseType
				)
					.then(result => t.deepEqual(result, errorResult)),
				t.context.sendCommand({hashSize: {name}}, sizeResponseType)
					.then(result => t.deepEqual(result, errorResult)),
				t.context.sendCommand({hashIter: {name}}, iterResponseType)
					.then(result => t.deepEqual(result, errorResult))
			])
		}))

		result = await t.context.sendCommand(
			{hashCreate: {name: listName}},
			voidResponseType
		)
		t.deepEqual(result, {error: `Error: Collection ${listName} already exists`})

		const name = 'already_created'
		result = await t.context.sendCommand({hashCreate: {name}}, voidResponseType)
		t.deepEqual(result, {})
		result = await t.context.sendCommand({hashCreate: {name}}, voidResponseType)
		t.deepEqual(result, {error: `Error: Collection ${name} already exists`})
	})

	test('hash-split-stress', async t => {
		const name = 'bigg'
		const result = await t.context.sendCommand(
			{hashCreate: {name}},
			voidResponseType
		)
		t.deepEqual(result, {})

		const value = (key: number) => new Uint8Array(3000).fill(key)
		for (let i = 0; i < 256; i++) {
			const result = await t.context.sendCommand(
				{hashSet: {name, key: new Uint8Array([i]), value: value(i)}},
				voidResponseType
			)
			t.deepEqual(result, {})
		}

		for (let i = 0; i < 256; i++) {
			const result = await t.context.sendCommand(
				{hashGet: {name, key: new Uint8Array([i])}},
				optionalBytesResponseType
			)
			t.deepEqual(result, {data: value(i)})
		}
		{
			const result = await t.context.sendCommand(
				{hashGet: {name, key: new Uint8Array(2)}},
				optionalBytesResponseType
			)
			t.deepEqual(result, {none: {}})
		}
	})
}
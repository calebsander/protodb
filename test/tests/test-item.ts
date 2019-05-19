import anyTest, {TestInterface} from 'ava'
import {TestContext} from '../common'
import {bytesResponseType, voidResponseType} from '../../pb/request'

const test = anyTest as TestInterface<TestContext>

test.beforeEach(t => {
	t.context = new TestContext
})

test('unset', async t => {
	const name = 'abc'
	{
		const result = await t.context.sendCommand({itemCreate: {name}}, voidResponseType)
		t.deepEqual(result, {})
	}
	{
		const result = await t.context.sendCommand({itemGet: {name}}, bytesResponseType)
		t.deepEqual(result, {error: `Error: Collection ${name} has not been set`})
	}
})

test.afterEach.always(t => t.context.close())
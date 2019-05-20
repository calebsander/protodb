import anyTest, {TestInterface} from 'ava'
import {TestContext} from './common'
import itemTest from './tests/item'
import hashTest from './tests/hash'

const test = anyTest as TestInterface<TestContext>

test.beforeEach(t => {
	t.context = new TestContext
})

itemTest(test)
hashTest(test)

test.afterEach.always(t => t.context.close())
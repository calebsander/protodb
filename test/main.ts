import anyTest, {TestInterface} from 'ava'
import {TestContext} from './common'
import itemTest from './tests/item'
import hashTest from './tests/hash'
import listTest from './tests/list'

const test = anyTest as TestInterface<TestContext>

test.beforeEach(t => (t.context = new TestContext).ready)

itemTest(test)
hashTest(test)
listTest(test)

test.afterEach.always(t => t.context.close())
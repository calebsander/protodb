import anyTest, {TestInterface} from 'ava'
import {TestContext} from './common'
import itemTest from './tests/item'
import hashTest from './tests/hash'
import listTest from './tests/list'
import sortedTest from './tests/sorted'

const test = anyTest as TestInterface<TestContext>

test.beforeEach(t => (t.context = new TestContext).ready)

itemTest(test)
hashTest(test)
listTest(test)
sortedTest(test)

test.afterEach.always(t => t.context.close())
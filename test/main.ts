import anyTest, {TestInterface} from 'ava'
import {TestContext} from './common'
import dbTest from './tests/db'
import hashTest from './tests/hash'
import itemTest from './tests/item'
import listTest from './tests/list'
import sortedTest from './tests/sorted'

const test = anyTest as TestInterface<TestContext>

test.beforeEach(t => (t.context = new TestContext).ready)

dbTest(test)
hashTest(test)
itemTest(test)
listTest(test)
sortedTest(test)

test.afterEach.always(t => t.context.close())
import path from 'path'
import {Reader} from 'protobufjs'
import {addCollection, dropCollection, getCollections} from '.'
import {dataDir} from '../args'
import {createFile, FilePage, getPageCount, PAGE_SIZE, removeFile, setPageCount} from '../cache'
import {Iterators} from '../iterator'
import {
	Child,
	FREE_LIST_END,
	freePageType,
	Header,
	headerType,
	Node,
	nodeType
} from '../pb/list'

const COLLECTION_TYPE = 'list'
const HEADER_PAGE = 0
const INITIAL_ROOT_PAGE = 1
const MIN_NODE_LENGTH = Math.floor(PAGE_SIZE * 0.4)

const filename = (name: string) =>
	path.join(dataDir, `${name}.${COLLECTION_TYPE}`)

async function checkIsList(name: string): Promise<void> {
	const collections = await getCollections
	const collection = collections[name]
	if (!(collection && COLLECTION_TYPE in collection)) {
		throw new Error(`Collection ${name} is not a list`)
	}
}

interface PathItem {
	page: number
	index: number
	node: Node
}

const getHeader = (name: string): Promise<Header> =>
	new FilePage(filename(name), HEADER_PAGE).use(async page =>
		headerType.toObject(
			headerType.decodeDelimited(new Uint8Array(page)),
			{longs: Number}
		)
	)
const setHeader = (name: string, header: Header): Promise<void> =>
	new FilePage(filename(name), HEADER_PAGE).use(async page =>
		new Uint8Array(page).set(headerType.encodeDelimited(header).finish())
	)
const getNode = (name: string, page: number): Promise<Node> =>
	new FilePage(filename(name), page).use(async page =>
		nodeType.toObject(
			nodeType.decodeDelimited(new Uint8Array(page)),
			{defaults: true, longs: Number}
		)
	)
const getNodeLength = (name: string, page: number): Promise<number> =>
	new FilePage(filename(name), page).use(async page =>
		new Reader(new Uint8Array(page)).uint32()
	)
const setNode = (name: string, page: number, node: Node): Promise<void> =>
	new FilePage(filename(name), page).use(async page =>
		new Uint8Array(page).set(nodeType.encodeDelimited(node).finish())
	)

async function lookup(name: string, index?: number, insert = false): Promise<PathItem[]> {
	const {child: {size, page}} = await getHeader(name)
	if (index === undefined) index = insert ? size : size - 1
	if (index < 0) index += size
	if (index < 0 || !(index < size || insert && index === size)) {
		throw new Error(`Index ${index} is out of bounds in list of size ${size}`)
	}

	const path: PathItem[] = []
	let lookupPage = page
	while (true) {
		const node = await getNode(name, lookupPage)
		const pathItem = {page: lookupPage, index, node}
		path.push(pathItem)
		if ('leaf' in node) break

		let nodeIndex = 0
		for (const {size, page} of node.inner.children) {
			if (index < size || insert && index === size) {
				lookupPage = page
				break
			}

			index -= size
			nodeIndex++
		}
		pathItem.index = nodeIndex
	}
	return path
}

async function getFreePage(name: string, header: Header): Promise<number> {
	const file = filename(name)
	const freePage = header.freePage.next
	if (freePage === FREE_LIST_END) {
		const pages = await getPageCount(file)
		await setPageCount(file, pages + 1)
		return pages
	}
	else {
		header.freePage = await new FilePage(file, freePage).use(async page =>
			freePageType.toObject(freePageType.decodeDelimited(new Uint8Array(page)))
		)
		return freePage
	}
}
const addFreePage = (name: string, header: Header, pageNo: number): Promise<void> =>
	new FilePage(filename(name), pageNo).use(async page => {
		const {freePage} = header
		new Uint8Array(page).set(freePageType.encodeDelimited(freePage).finish())
		freePage.next = pageNo
	})

const split = <T>(arr: T[]): T[] => arr.splice(arr.length >> 1)
function concat<T>(arr: T[], other: T[], left: boolean): void {
	if (left) arr.splice(0, 0, ...other)
	else arr.push(...other)
}

const nodeSize = (node: Node) =>
	'inner' in node
		? node.inner.children.reduce((totalSize, {size}) => totalSize + size, 0)
		: node.leaf.values.length

function getParent(path: PathItem[]) {
	const [parent]: (PathItem | undefined)[] = path.slice(-1)
	if (!parent) return undefined

	const {node: parentNode, index} = parent
	if ('leaf' in parentNode) throw new Error('Parent is not an inner node?')
	const {children} = parentNode.inner
	return {children, index}
}

interface Sibling {
	left: boolean
	siblingIndex: number
	sibling: Child
}
async function tryCoalesce(
	name: string, node: Node, path: PathItem[], header: Header
): Promise<boolean> {
	const parent = getParent(path)
	if (!parent) return false // root node can't be coalesced
	const {len} = nodeType.encodeDelimited(node)
	if (len >= MIN_NODE_LENGTH) return false // ensure node is sufficiently empty
	const {children, index} = parent
	const siblings = [true, false]
		.map(left => {
			const siblingIndex = left ? index - 1 : index + 1
			return {left, siblingIndex, sibling: children[siblingIndex]}
		})
		.filter(({sibling}) => sibling) // skip siblings that don't exist
	const lengths = await Promise.all(siblings.map(
		({sibling}) => getNodeLength(name, sibling.page)
	))
	// TODO: should try to coalesce with other sibling afterwards
	let coalesceSibling: Sibling | undefined
	let maxSiblingLength = 0
	lengths.forEach((length, i) => {
		if (length < MIN_NODE_LENGTH && length > maxSiblingLength) {
			coalesceSibling = siblings[i]
			maxSiblingLength = length
		}
	})
	if (!coalesceSibling) return false // node must have a sibling with free space

	// Coalesce with selected sibling
	const {left, siblingIndex, sibling: {page: siblingPage}} = coalesceSibling
	const siblingNode = await getNode(name, siblingPage)
	if ('inner' in node) {
		if ('leaf' in siblingNode) throw new Error('Invalid sibling')
		concat(node.inner.children, siblingNode.inner.children, left)
	}
	else {
		if ('inner' in siblingNode) throw new Error('Invalid sibling')
		concat(node.leaf.values, siblingNode.leaf.values, left)
	}
	const thisChild = children[index]
	const promises = [(async () => {
		await setNode(name, thisChild.page, node)
		// Must wait to overwrite sibling until new page has been written since
		// leaf values are slices of the sibling page
		await addFreePage(name, header, siblingPage)
	})()]

	// Update parent
	thisChild.size = nodeSize(node)
	children.splice(siblingIndex, 1)

	// Make grandparent point directly to this page if it has no more siblings
	if (children.length === 1) {
		path.pop()
		const grandParent = getParent(path)
		const child = grandParent
			? grandParent.children[grandParent.index]
			: header.child
		promises.push(addFreePage(name, header, child.page))
		Object.assign(child, thisChild)
	}
	await Promise.all(promises)
	return true
}

async function* sublistEntries(
	name: string, page: number, start?: number, end?: number
): AsyncIterableIterator<Uint8Array> {
	const node = await getNode(name, page)
	if ('inner' in node) {
		const hasStart = start !== undefined, hasEnd = end !== undefined
		for (const {page, size} of node.inner.children) {
			if (hasEnd && end! <= (hasStart ? start! : 0)) break

			yield* sublistEntries(name, page, start, end)
			if (hasStart) start = Math.max(start! - size, 0)
			if (hasEnd) end! -= size
		}
	}
	else {
		const {values} = node.leaf
		if (end === undefined) end = values.length
		for (let i = start || 0; i < end; i++) yield values[i]
	}
}
async function* listEntries(
	name: string, start?: number, end?: number
): AsyncIterator<Uint8Array> {
	const {child} = await getHeader(name)
	yield *sublistEntries(name, child.page, start, end)
}

const iterators = new Iterators<AsyncIterator<Uint8Array>>()

export async function create(name: string): Promise<void> {
	await addCollection(name, {[COLLECTION_TYPE]: {}})
	const file = filename(name)
	await createFile(file)
	await setPageCount(file, 2)
	await Promise.all([
		setHeader(name, {
			child: {size: 0, page: INITIAL_ROOT_PAGE},
			freePage: {next: FREE_LIST_END}
		}),
		setNode(name, INITIAL_ROOT_PAGE, {leaf: {values: []}})
	])
}

export async function drop(name: string): Promise<void> {
	await checkIsList(name)
	await Promise.all([dropCollection(name), removeFile(filename(name))])
}

// "delete" is a reserved name, so we use "remove" instead
export async function remove(name: string, listIndex?: number): Promise<void> {
	await checkIsList(name)
	iterators.checkNoIterators(name)
	const path = await lookup(name, listIndex)
	const [{index, node}] = path.slice(-1)
	if ('inner' in node) throw new Error('Path does not end in a leaf?')
	node.leaf.values.splice(index, 1)
	const header = await getHeader(name)
	while (path.length) {
		const {page, node} = path.pop()!
		const coalesced = await tryCoalesce(name, node, path, header)
		if (!coalesced) {
			await setNode(name, page, node)
			const parent = getParent(path)
			const parentChild = parent
				? parent.children[parent.index]
				: header.child
			parentChild.size--
		}
	}
	await setHeader(name, header)
}

export async function get(name: string, listIndex: number): Promise<Uint8Array> {
	await checkIsList(name)
	const path = await lookup(name, listIndex)
	const [{index, node}] = path.slice(-1)
	if ('inner' in node) throw new Error('Path does not end in a leaf?')
	return node.leaf.values[index]
}

export async function insert(
	name: string, listIndex: number | undefined, value: Uint8Array
): Promise<void> {
	await checkIsList(name)
	iterators.checkNoIterators(name)
	const path = await lookup(name, listIndex, true)
	const [{index, node}] = path.slice(-1)
	if ('inner' in node) throw new Error('Path does not end in a leaf?')
	node.leaf.values.splice(index, 0, value)
	const header = await getHeader(name)
	while (path.length) {
		const {page, node} = path.pop()!
		const parent = getParent(path)
		try {
			await setNode(name, page, node)
			if (parent) {
				const {children, index} = parent
				children[index].size++
			}
		}
		catch (e) {
			// Node overflowed
			if (!(e instanceof RangeError && e.message === 'Source is too large')) {
				throw e // unexpected error; rethrow it
			}

			const newNode: Node = 'inner' in node
				? {inner: {children: split(node.inner.children)}}
				: {leaf: {values: split(node.leaf.values)}}
			const newPage = await getFreePage(name, header)
			const oldChild = {size: nodeSize(node), page},
						newChild = {size: nodeSize(newNode), page: newPage}
			const promises = [(async () => {
				await setNode(name, newPage, newNode)
				// Need to wait to overwrite the old node until the new node is written
				// because leaf values are slices of the old page
				await setNode(name, page, node)
			})()]
			if (parent) {
				const {children, index} = parent
				children.splice(index, 1, oldChild, newChild)
			}
			else { // splitting the root node
				const makeNewRoot = async () => {
					const rootPage = await getFreePage(name, header)
					header.child.page = rootPage
					await setNode(
						name, rootPage, {inner: {children: [oldChild, newChild]}}
					)
				}
				promises.push(makeNewRoot())
			}
			await Promise.all(promises)
		}
	}
	header.child.size++
	await setHeader(name, header)
}

export async function set(
	name: string, listIndex: number, value: Uint8Array
): Promise<void> {
	await checkIsList(name)
	iterators.checkNoIterators(name)
	const path = await lookup(name, listIndex)
	const [{page, index, node}] = path.slice(-1)
	if ('inner' in node) throw new Error('Path does not end in a leaf?')
	node.leaf.values[index] = value
	await setNode(name, page, node)
}

export async function size(name: string): Promise<number> {
	await checkIsList(name)
	const {child: {size}} = await getHeader(name)
	return size
}

export async function iter(
	name: string, start?: number, end?: number
): Promise<Uint8Array> {
	await checkIsList(name)
	return iterators.registerIterator(name, listEntries(name, start, end))
}

export function iterBreak(iter: Uint8Array): void {
	iterators.closeIterator(iter)
}

export async function iterNext(iter: Uint8Array): Promise<Uint8Array | null> {
	const iterator = iterators.getIterator(iter)
	const {value, done} = await iterator.next()
	if (done) {
		iterators.closeIterator(iter)
		return null
	}
	return value
}
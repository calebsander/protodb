import path = require('path')
import {addCollection, dropCollection, getCollections} from '.'
import {dataDir} from '../args'
import {createFile, FilePage, getPageCount, PAGE_SIZE, removeFile, setPageCount} from '../cache'
import {Iterators} from '../iterator'
import {CollectionType} from '../pb/interface'
import {
	FREE_LIST_END,
	freePageType,
	Header,
	headerType,
	Node,
	nodeType
} from '../pb/list'
import {argmin, ensureOverflowError, getNodeLength} from '../util'

const HEADER_PAGE = 0
const INITIAL_ROOT_PAGE = 1
const MIN_NODE_LENGTH = PAGE_SIZE >> 1

const filename = (name: string) => path.join(dataDir, `${name}.list`)

async function checkIsList(name: string): Promise<void> {
	const collections = await getCollections
	const collection = collections[name]
	if (collection !== CollectionType.LIST) {
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
const setNode = (name: string, page: number, node: Node): Promise<void> =>
	new FilePage(filename(name), page).use(async page =>
		new Uint8Array(page).set(nodeType.encodeDelimited(node).finish())
	)

async function lookup(name: string, index?: number, insert = false): Promise<PathItem[]> {
	const {child: {size, page}} = await getHeader(name)
	if (index === undefined) index = insert ? size : size - 1
	else {
		if (index < -size || index >= size + Number(insert)) {
			throw new Error(`Index ${index} is out of bounds in list of size ${size}`)
		}
		if (index < 0) index += size
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
	if (left) arr.unshift(...other)
	else arr.push(...other)
}

const nodeSize = (node: Node): number =>
	'inner' in node
		? node.inner.children.reduce((totalSize, {size}) => totalSize + size, 0)
		: node.leaf.values.length

function getParent(path: PathItem[]) {
	const [parent] = path.slice(-1) as (PathItem | undefined)[]
	if (!parent) return undefined

	const {node: parentNode, index} = parent
	// istanbul ignore if
	if ('leaf' in parentNode) throw new Error('Parent is not an inner node?')
	const {children} = parentNode.inner
	return {children, index}
}

async function saveWithOverflow(name: string, path: PathItem[], insert: boolean): Promise<void> {
	const header = await getHeader(name)
	while (path.length) {
		const {page, node} = path.pop()!
		const parent = getParent(path)
		try {
			await setNode(name, page, node)

			// Saved node without overflowing
			if (insert) {
				if (parent) {
					const {children, index} = parent
					children[index].size++
				}
			}
			else break
		}
		catch (e) {
			// Node overflowed
			ensureOverflowError(e)

			// TODO: this doesn't split leaves evenly
			const newNode: Node = 'inner' in node
				? {inner: {children: split(node.inner.children)}}
				// Make copies of values since they are slices of the old page,
				// which will be overwritten
				: {leaf: {values: split(node.leaf.values).map(value => value.slice())}}
			const newPage = await getFreePage(name, header)
			const children = [
				{size: nodeSize(node), page},
				{size: nodeSize(newNode), page: newPage}
			]
			const promises = [
				setNode(name, page, node),
				setNode(name, newPage, newNode)
			]
			if (parent) {
				parent.children.splice(parent.index, 1, ...children)
			}
			else { // splitting the root node
				promises.push((async () => {
					const rootPage = await getFreePage(name, header)
					header.child.page = rootPage
					await setNode(name, rootPage, {inner: {children}})
				})())
			}
			await Promise.all(promises)
		}
	}
	if (insert) header.child.size++
	await setHeader(name, header)
}

async function tryCoalesce(
	name: string, node: Node, path: PathItem[], header: Header
): Promise<boolean> {
	const parent = getParent(path)
	if (!parent) return false // root node can't be coalesced
	const {len} = nodeType.encodeDelimited(node)
	if (len >= MIN_NODE_LENGTH) return false // ensure node is sufficiently empty

	const {children, index} = parent
	const thisChild = children[index]
	const file = filename(name)
	const siblingLengths = await Promise.all(
		[true, false]
			.map(left => {
				const siblingIndex = left ? index - 1 : index + 1
				return {left, siblingIndex, sibling: children[siblingIndex]}
			})
			.filter(({sibling}) => sibling) // skip siblings that don't exist
			.map(async sibling => {
				const length = await getNodeLength(file, sibling.sibling.page)
				return {sibling, length}
			})
	)
	// Coalescing is only possible with less than half-full siblings
	const coalesceCandidates =
		siblingLengths.filter(({length}) => length < MIN_NODE_LENGTH)
	if (!coalesceCandidates.length) return false

	const newFreePages: number[] = []
	while (true) {
		// Choose the smaller of the candidate siblings to coalesce
		const coalesceSibling = argmin(coalesceCandidates, ({length}) => length)

		// Coalesce with selected sibling
		const [{sibling}] = coalesceCandidates.splice(coalesceSibling, 1)
		const {left, siblingIndex, sibling: {page: siblingPage}} = sibling
		const siblingNode = await getNode(name, siblingPage)
		if ('inner' in node) {
			// istanbul ignore if
			if ('leaf' in siblingNode) throw new Error('Invalid sibling?')
			concat(node.inner.children, siblingNode.inner.children, left)
		}
		else {
			// istanbul ignore if
			if ('inner' in siblingNode) throw new Error('Invalid sibling?')
			// Copy sibling's values because they are slices of its page,
			// which will be overwritten when it gets added to the free list
			concat(
				node.leaf.values,
				siblingNode.leaf.values.map(value => value.slice()),
				left
			)
		}
		newFreePages.push(siblingPage)

		// Remove sibling from parent
		children.splice(siblingIndex, 1)

		// See if it is possible to coalesce with the other sibling
		const coalesceAgain = coalesceCandidates.length &&
			nodeType.encodeDelimited(node).len < MIN_NODE_LENGTH
		if (coalesceAgain) {
			// The index of the right sibling goes down if the left sibling is removed
			if (left) coalesceCandidates[0].sibling.siblingIndex--
		}
		else break
	}

	// Update sublist's size
	thisChild.size = nodeSize(node)

	// Make this the new root if it is the only child of the root node
	if (path.length === 1 && children.length === 1) {
		path.pop()
		newFreePages.push(header.child.page)
		header.child = thisChild
	}

	const promises = newFreePages.map(page => addFreePage(name, header, page))
	promises.push(setNode(name, thisChild.page, node))
	await Promise.all(promises)
	return true
}

async function* sublistEntries(
	name: string, page: number, start: number, end: number
): AsyncIterableIterator<Uint8Array> {
	const node = await getNode(name, page)
	if ('inner' in node) {
		for (const {page, size} of node.inner.children) {
			if (end <= 0) break

			yield* sublistEntries(name, page, start, end)
			start -= size
			end -= size
		}
	}
	else {
		const {values} = node.leaf
		start = Math.max(start, 0)
		end = Math.min(end, values.length)
		for (let i = start; i < end; i++) yield values[i]
	}
}
async function* listEntries(
	name: string, start?: number, end?: number
): AsyncIterator<Uint8Array> {
	const {child: {page, size}} = await getHeader(name)
	start = start || 0
	if (end === undefined) end = size
	yield *sublistEntries(name, page, start, end)
}

const iterators = new Iterators<AsyncIterator<Uint8Array>>()

export async function create(name: string): Promise<void> {
	await addCollection(name, CollectionType.LIST)
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
	iterators.checkNoIterators(name)
	await Promise.all([dropCollection(name), removeFile(filename(name))])
}

// "delete" is a reserved name, so we use "remove" instead
export async function remove(name: string, listIndex?: number): Promise<void> {
	await checkIsList(name)
	iterators.checkNoIterators(name)
	const path = await lookup(name, listIndex)
	const [{index, node}] = path.slice(-1)
	// istanbul ignore if
	if ('inner' in node) throw new Error('Path does not end in a leaf?')
	node.leaf.values.splice(index, 1)
	const header = await getHeader(name)
	let coalesced = true
	while (path.length) {
		const {page, node} = path.pop()!
		// Only coalesce if child was coalesced
		coalesced = coalesced && await tryCoalesce(name, node, path, header)
		if (!coalesced) { // did not coalesce, so save node
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
	// istanbul ignore if
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
	// istanbul ignore if
	if ('inner' in node) throw new Error('Path does not end in a leaf?')
	node.leaf.values.splice(index, 0, value)
	await saveWithOverflow(name, path, true)
}

export async function set(
	name: string, listIndex: number, value: Uint8Array
): Promise<void> {
	await checkIsList(name)
	iterators.checkNoIterators(name)
	const path = await lookup(name, listIndex)
	const [{index, node}] = path.slice(-1)
	// istanbul ignore if
	if ('inner' in node) throw new Error('Path does not end in a leaf?')
	node.leaf.values[index] = value
	await saveWithOverflow(name, path, false)
}

export async function size(name: string): Promise<number> {
	await checkIsList(name)
	const {child} = await getHeader(name)
	return child.size
}

export async function iter(
	name: string, start?: number, end?: number
): Promise<Uint8Array> {
	await checkIsList(name)
	return iterators.registerIterator(name, listEntries(name, start, end))
}

export const iterBreak = (iter: Uint8Array): void =>
	iterators.closeIterator(iter)

export async function iterNext(iter: Uint8Array): Promise<Uint8Array | null> {
	const iterator = iterators.getIterator(iter)
	const {value, done} = await iterator.next()
	if (done) {
		iterators.closeIterator(iter)
		return null
	}
	return value
}
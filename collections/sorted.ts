import path = require('path')
import {addCollection, dropCollection, getCollections} from '.'
import {dataDir} from '../args'
import {createFile, FilePage, getPageCount, PAGE_SIZE, removeFile, setPageCount} from '../cache'
import {Iterators} from '../iterator'
import {CollectionType, Key, KeyElement, SortedKeyValuePair} from '../pb/interface'
import {
	freePageType,
	Header,
	headerType,
	LIST_END,
	Node,
	nodeType
} from '../pb/sorted'
import {argmin, ensureOverflowError, getNodeLength} from '../util'

const HEADER_PAGE = 0
const INITIAL_ROOT_PAGE = 1
// Slightly less than half because joining inner nodes requires
// pulling down the split key from their parent
const MIN_NODE_LENGTH = (PAGE_SIZE * 0.45) | 0

interface PathItem {
	page: number
	node: Node
	index: number
}

const filename = (name: string) => path.join(dataDir, `${name}.sorted`)

async function checkIsSorted(name: string): Promise<void> {
	const collections = await getCollections
	const collection = collections[name]
	if (collection !== CollectionType.SORTED) {
		throw new Error(`Collection ${name} is not a sorted map`)
	}
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

async function getFreePage(name: string, header: Header): Promise<number> {
	const file = filename(name)
	const freePage = header.freePage.next
	if (freePage === LIST_END) {
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

function getUniquifier(key: KeyElement[]): number | undefined {
	const [lastElement] = key.slice(-1) as [KeyElement] | []
	return lastElement && 'uniquifier' in lastElement
		? lastElement.uniquifier
		: undefined
}
function compareKeys(key1: KeyElement[], key2: KeyElement[]): number {
	const minLength = Math.min(key1.length, key2.length)
	for (let i = 0; i < minLength; i++) {
		const element1 = key1[i], element2 = key2[i]
		let diff: number
		if ('int' in element1) {
			if (!('int' in element2)) throw new Error('Key types do not match')
			diff = element1.int - element2.int
		}
		else if ('float' in element1) {
			if (!('float' in element2)) throw new Error('Key types do not match')
			diff = element1.float - element2.float
		}
		else if ('string' in element1) {
			if (!('string' in element2)) throw new Error('Key types do not match')
			const string1 = element1.string, string2 = element2.string
			diff = string1 < string2 ? -1 : string1 > string2 ? +1 : 0
		}
		else {
			if (!('uniquifier' in element2)) throw new Error('Key types do not match')
			// Uniquifier order is reversed so the leftmost equivalent key
			// has the largest uniquifier
			diff = element2.uniquifier - element1.uniquifier
		}
		if (diff) return diff
	}
	return 0
}
function lookupKey(key: KeyElement[], keys: Key[]): number {
	const {length} = keys
	let i: number
	for (i = 0; i < length; i++) {
		const comparison = compareKeys(keys[i].elements, key)
		if (comparison >= 0) break
	}
	return i
}
async function lookup(name: string, key: KeyElement[]): Promise<PathItem[]> {
	let {root: page} = await getHeader(name)
	const path: PathItem[] = []
	while (true) {
		const node = await getNode(name, page)
		let keys: Key[]
		let children: number[] | undefined
		if ('leaf' in node) ({keys} = node.leaf)
		else ({keys, children} = node.inner)
		const index = lookupKey(key, keys)
		path.push({page, node, index})
		if (children) page = children[index]
		else break
	}
	return path
}

async function saveWithOverflow(
	name: string, key: KeyElement[], path: PathItem[], header: Header
): Promise<void> {
	let saving = true, newMaxKey = true
	do {
		const {page, node, index} = path.pop()!
		const [parent] = path.slice(-1) as [PathItem] | []
		if (newMaxKey) {
			newMaxKey = false
			if (parent) {
				const children =
					'leaf' in node ? node.leaf.values : node.inner.children
				if (index === children.length - 1) { // changing the maximum element
					const {node: parentNode, index: parentIndex} = parent
					if ('leaf' in parentNode) throw new Error('Parent is not an inner node?')
					const {keys} = parentNode.inner
					if (parentIndex < keys.length) {
						keys[parentIndex] = {elements: key}
						newMaxKey = true
					}
				}
			}
		}
		try {
			await setNode(name, page, node)
			// Saved node without overflowing
			saving = newMaxKey
		}
		catch (e) {
			// Node overflowed
			ensureOverflowError(e)

			const newPage = await getFreePage(name, header)
			let newNode: Node
			let promotedKey: Key
			// TODO: this doesn't split leaves evenly
			if ('leaf' in node) {
				const {leaf} = node
				const {keys, values, next} = leaf
				const splitIndex = keys.length >> 1
				if (!splitIndex) throw new Error('Item is too large to store')
				newNode = {leaf: {
					keys: keys.splice(splitIndex),
					// Make copies of values since they are slices of the old page,
					// which will be overwritten
					values: values.splice(splitIndex).map(value => value.slice()),
					next
				}}
				;[promotedKey] = keys.slice(-1)
				leaf.next = newPage
			}
			else {
				const {keys, children} = node.inner
				if (keys.length < 2) throw new Error('Item is too large to store')
				const splitIndex = (keys.length >> 1) + 1
				newNode = {inner: {
					keys: keys.splice(splitIndex),
					children: children.splice(splitIndex)
				}}
				promotedKey = keys.pop()!
			}
			const promises = [
				setNode(name, page, node),
				setNode(name, newPage, newNode)
			]
			// Promote the new key and page to the parent node
			if (parent) {
				const parentNode = parent.node
				if ('leaf' in parentNode) throw new Error('Parent is not an inner node?')
				const {keys, children} = parentNode.inner
				const insertIndex = parent.index
				keys.splice(insertIndex, 0, promotedKey)
				children.splice(insertIndex + 1, 0, newPage)
			}
			else { // splitting the root node
				promises.push((async () => {
					const rootPage = await getFreePage(name, header)
					header.root = rootPage
					await setNode(name, rootPage, {inner: {
						keys: [promotedKey],
						children: [page, newPage]
					}})
				})())
			}
			await Promise.all(promises)
		}
	} while (path.length && saving)
}

async function tryCoalesce(
	name: string, node: Node, path: PathItem[], header: Header
): Promise<boolean> {
	if (!path.length) return false // root node can't be coalesced
	const {len} = nodeType.encodeDelimited(node)
	if (len >= MIN_NODE_LENGTH) return false // ensure node is sufficiently empty

	const [{node: parentNode, index}] = path.slice(-1)
	if ('leaf' in parentNode) throw new Error('Parent is not a leaf?')
	const {keys, children} = parentNode.inner
	let thisPage = children[index]
	const file = filename(name)
	const siblingLengths = await Promise.all(
		[true, false]
			.map(left => {
				const siblingIndex = left ? index - 1 : index + 1
				return {left, siblingIndex, sibling: children[siblingIndex]}
			})
			.filter(({sibling}) => sibling) // skip siblings that don't exist
			.map(async sibling => {
				const length = await getNodeLength(file, sibling.sibling)
				return {sibling, length}
			})
	)
	// Coalescing is only possible with less than half-full siblings
	const coalesceCandidates =
		siblingLengths.filter(({length}) => length < MIN_NODE_LENGTH)
	if (!coalesceCandidates.length) return false

	const originalNode = node
	const newFreePages: number[] = []
	let coalesceAgain: boolean
	do {
		// Choose the smaller of the candidate siblings to coalesce
		const coalesceSibling = argmin(coalesceCandidates, ({length}) => length)

		// Coalesce with selected sibling
		const [{sibling}] = coalesceCandidates.splice(coalesceSibling, 1)
		const {left, siblingIndex, sibling: siblingPage} = sibling
		const siblingNode = await getNode(name, siblingPage)
		// We always coalesce into the left sibling so that if it is a leaf,
		// we don't have to change the "next" value of the previous leaf
		let leftNode: Node, rightNode: Node
		let leftIndex = siblingIndex
		if (left) {
			leftNode = siblingNode
			rightNode = node
			newFreePages.push(thisPage)
			thisPage = siblingPage
		}
		else {
			leftNode = node
			rightNode = siblingNode
			leftIndex-- // sibling is to the right, one index higher
			newFreePages.push(siblingPage)
		}
		let newSize: number | undefined
		if ('inner' in leftNode) {
			if ('leaf' in rightNode) throw new Error('Invalid sibling?')
			const leftInner = leftNode.inner, rightInner = rightNode.inner
			let newNode = {inner: {
				keys: [...leftInner.keys, keys[leftIndex], ...rightInner.keys],
				children: [...leftInner.children, ...rightInner.children]
			}}
			// Ensure that this node is not too big (since it includes the split key)
			newSize = nodeType.encodeDelimited(newNode).len
			if (newSize > PAGE_SIZE) break
			node = newNode
		}
		else {
			if ('inner' in rightNode) throw new Error('Invalid sibling?')
			const leftLeaf = leftNode.leaf, rightLeaf = rightNode.leaf
			const values = leftLeaf.values.slice()
			// Copy right node's values because they are slices of its page,
			// which will be overwritten when it gets added to the free list
			for (const value of rightLeaf.values) values.push(value.slice())
			node = {leaf: {
				keys: [...leftLeaf.keys, ...rightLeaf.keys],
				values,
				next: rightLeaf.next
			}}
		}

		// Remove left sibling's key and right sibling's page from parent
		keys.splice(leftIndex, 1)
		children.splice(leftIndex + 1, 1)

		// See if it is possible to coalesce with the other sibling
		if (coalesceCandidates.length) {
			if (newSize === undefined) newSize = nodeType.encodeDelimited(node).len
			coalesceAgain = newSize < MIN_NODE_LENGTH
			if (coalesceAgain) {
				// The index of the right sibling goes down if the left sibling is removed
				if (left) coalesceCandidates[0].sibling.siblingIndex--
			}
		}
		else coalesceAgain = false
	} while (coalesceAgain)
	if (node === originalNode) return false

	// Make this the new root if it is the only child of the root node
	if (path.length === 1 && children.length === 1) {
		path.pop()
		newFreePages.push(header.root)
		header.root = thisPage
	}

	const promises = newFreePages.map(page => addFreePage(name, header, page))
	promises.push(setNode(name, thisPage, node))
	await Promise.all(promises)
	return true
}

async function* pairsFrom(
	name: string, inclusive: boolean, start: KeyElement[], end?: KeyElement[]
): AsyncIterableIterator<SortedKeyValuePair> {
	const path = await lookup(name, start)
	let [{node, index}] = path.slice(-1)
	while (true) {
		if ('inner' in node) throw new Error('Not a leaf?')
		const {keys, values, next} = node.leaf
		while (index < keys.length) {
			const key = keys[index].elements
			if (end) {
				const comparison = compareKeys(key, end)
				if (comparison > 0 || !(inclusive || comparison)) return
			}
			yield {key, value: values[index]}
			index++
		}
		if (next === LIST_END) break

		node = await getNode(name, next)
		index = 0
	}
}
const iterators = new Iterators<AsyncIterator<SortedKeyValuePair>>()

export async function create(name: string): Promise<void> {
	await addCollection(name, CollectionType.SORTED)
	const file = filename(name)
	await createFile(file)
	await setPageCount(file, 2)
	await Promise.all([
		setHeader(name, {
			root: INITIAL_ROOT_PAGE,
			size: 0,
			freePage: {next: LIST_END}
		}),
		setNode(name, INITIAL_ROOT_PAGE, {
			leaf: {keys: [], values: [], next: LIST_END}
		})
	])
}

export async function drop(name: string): Promise<void> {
	await checkIsSorted(name)
	iterators.checkNoIterators(name)
	await Promise.all([dropCollection(name), removeFile(filename(name))])
}

export async function remove(name: string, searchKey: KeyElement[]): Promise<void> {
	await checkIsSorted(name)
	iterators.checkNoIterators(name)
	const path = await lookup(name, searchKey)
	const [{node, index}] = path.slice(-1)
	if ('inner' in node) throw new Error('Path does not end in a leaf?')
	const {keys, values} = node.leaf
	const oldKey = keys[index] as Key | undefined
	// If key doesn't match, deletion can't be performed
	if (!oldKey || compareKeys(oldKey.elements, searchKey)) {
		throw new Error('No matching key')
	}

	keys.splice(index, 1)
	values.splice(index, 1)
	const header = await getHeader(name)
	header.size--
	let coalesced: boolean
	do {
		const {page, node} = path.pop()!
		// Only coalesce if child was coalesced
		coalesced = await tryCoalesce(name, node, path, header)
		// If node couldn't be coalesced, save it as-is
		if (!coalesced) await setNode(name, page, node)
	} while (path.length && coalesced)
	await setHeader(name, header)
}

export async function get(
	name: string, searchKey: KeyElement[]
): Promise<SortedKeyValuePair[]> {
	await checkIsSorted(name)
	const pairs: SortedKeyValuePair[] = []
	for await (const pair of pairsFrom(name, true, searchKey, searchKey)) {
		pairs.push(pair)
	}
	return pairs
}

export async function insert(
	name: string, key: KeyElement[], value: Uint8Array
): Promise<void> {
	if (getUniquifier(key) !== undefined) {
		throw new Error('Key cannot include uniquifier')
	}
	await checkIsSorted(name)
	iterators.checkNoIterators(name)
	const path = await lookup(name, key)
	const [{node, index}] = path.slice(-1)
	if ('inner' in node) throw new Error('Path does not end in a leaf?')
	const {keys, values} = node.leaf
	const oldKey = keys[index] as Key | undefined
	if (oldKey) {
		const {elements} = oldKey
		if (!compareKeys(key, elements)) {
			let oldUniquifier = getUniquifier(elements)
			if (oldUniquifier === undefined) {
				oldUniquifier = 0
				elements.push({uniquifier: oldUniquifier})
			}
			key.push({uniquifier: oldUniquifier + 1})
		}
	}
	keys.splice(index, 0, {elements: key})
	values.splice(index, 0, value)
	const header = await getHeader(name)
	header.size++
	await saveWithOverflow(name, key, path, header)
	await setHeader(name, header)
}

export async function size(name: string): Promise<number> {
	await checkIsSorted(name)
	const {size} = await getHeader(name)
	return size
}

export async function iter(
	name: string, inclusive: boolean, start?: KeyElement[], end?: KeyElement[]
): Promise<Uint8Array> {
	await checkIsSorted(name)
	const iterator = pairsFrom(name, inclusive, start || [], end)
	return iterators.registerIterator(name, iterator)
}

export const iterBreak = (iter: Uint8Array): void =>
	iterators.closeIterator(iter)

export async function iterNext(iter: Uint8Array): Promise<SortedKeyValuePair | null> {
	const iterator = iterators.getIterator(iter)
	const {value, done} = await iterator.next()
	if (done) {
		iterators.closeIterator(iter)
		return null
	}
	return value
}
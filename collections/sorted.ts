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

const filename = (name: string): string => path.join(dataDir, `${name}.sorted`)

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

// Obtains a free page from the free list, or by extending the file.
// Not atomic, so only one call should be performed at a time.
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
// Atomically adds a free page to the front of the free list
const addFreePage = (name: string, header: Header, pageNo: number): Promise<void> =>
	new FilePage(filename(name), pageNo).use(async page => {
		const {freePage} = header
		new Uint8Array(page).set(freePageType.encodeDelimited(freePage).finish())
		freePage.next = pageNo
	})

// Gets the uniquifier of a key
function getUniquifier(key: KeyElement[]): number | undefined {
	const [lastElement] = key.slice(-1) as [KeyElement] | []
	return lastElement && 'uniquifier' in lastElement
		? lastElement.uniquifier
		: undefined
}
// Compares key tuples in lexicographic order
function compareKeys(key1: KeyElement[], key2: KeyElement[]): number {
	// Only compare the keys' shared elements
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
		if (diff) return diff // if diff is 0, continue onto next element
	}
	return 0
}
// Finds the index of the key that matches a search key.
// This guides the B+ tree traversal in inner nodes and leaf nodes.
// If the search key exceeds all keys, returns the index after the last key.
function lookupKey(key: KeyElement[], keys: Key[]): number {
	let i: number
	for (i = 0; i < keys.length; i++) {
		if (compareKeys(keys[i].elements, key) >= 0) break
	}
	return i
}
interface PathItem {
	page: number
	node: Node
	index: number
}
// Performs the tree lookup, traversing the path to the leaf
async function lookup(
	name: string, {root: page}: Header, key: KeyElement[]
): Promise<PathItem[]> {
	const path: PathItem[] = []
	while (true) {
		const node = await getNode(name, page)
		let keys: Key[]
		let children: number[] | undefined
		if ('leaf' in node) ({keys} = node.leaf)
		else ({keys, children} = node.inner)
		const index = lookupKey(key, keys)
		path.push({page, node, index})
		if (!children) return path // at a leaf node, so we are done

		page = children[index] // at an inner node, so traverse the matched child
	}
}

// Saves a node and any of its ancestors that need saving,
// handling any overflows that occur
async function saveWithOverflow(
	name: string, key: KeyElement[], path: PathItem[], header: Header
): Promise<void> {
	// saving stores whether a change in the child requires the parent to be saved
	// newMaxKey stores whether the key being inserted was the child's highest key
	let saving = true, newMaxKey = true
	do {
		const {page, node, index} = path.pop()!
		// Destructure parent
		const [parent] = path.slice(-1) as [PathItem] | []
		const {node: parentNode, index: parentIndex} =
			parent || {node: undefined, index: undefined}
		let parentKeys: Key[] | undefined, parentChildren: number[] | undefined
		if (parentNode) {
			// istanbul ignore if
			if ('leaf' in parentNode) throw new Error('Parent is not an inner node?')
			;({keys: parentKeys, children: parentChildren} = parentNode.inner)
		}

		// Update key if the child has a new maximum key
		if (newMaxKey) {
			newMaxKey = false
			// If the child is the rightmost child of its parent inner node,
			// or the child is the root node, there is no maximum key to update
			if (parent && parentIndex! < parentKeys!.length) {
				const children =
					'leaf' in node ? node.leaf.values : node.inner.children
				// Check whether the maximum element in the child was changed
				if (index === children.length - 1) {
					parentKeys![parentIndex!] = {elements: key}
					newMaxKey = true
				}
			}
		}

		try {
			await setNode(name, page, node)
			// Saved node without overflowing
			saving = newMaxKey // save can stop unless this is a new maximum key
		}
		catch (e) {
			// Node overflowed
			ensureOverflowError(e)

			// Get a new page to store the right half of this node
			const newPage = await getFreePage(name, header)
			let newNode: Node // the new node
			let promotedKey: Key // the key to split the nodes
			// TODO: this doesn't split leaves evenly
			if ('leaf' in node) { // splitting a leaf
				const {leaf} = node
				const {keys, values, next} = leaf
				const splitIndex = keys.length >> 1
				// istanbul ignore if
				if (!splitIndex) throw new Error('Item is too large to store')
				newNode = {leaf: {
					keys: keys.splice(splitIndex),
					// Make copies of values since they are slices of the old page,
					// which will be overwritten
					values: values.splice(splitIndex).map(value => value.slice()),
					next
				}}
				;[promotedKey] = keys.slice(-1) // promote the maximum key from the left
				leaf.next = newPage
			}
			else { // splitting an inner node
				const {keys, children} = node.inner
				// istanbul ignore if
				if (keys.length < 2) throw new Error('Item is too large to store')
				const splitIndex = (keys.length >> 1) + 1
				newNode = {inner: {
					keys: keys.splice(splitIndex),
					children: children.splice(splitIndex)
				}}
				// Promote the dangling maximum key from the left
				promotedKey = keys.pop()!
			}
			const promises = [
				setNode(name, page, node),
				setNode(name, newPage, newNode)
			]
			// Promote the new key and page to the parent node
			if (parent) {
				parentKeys!.splice(parentIndex!, 0, promotedKey)
				parentChildren!.splice(parentIndex! + 1, 0, newPage)
			}
			else { // splitting the root node
				promises.push((async () => {
					const rootPage = await getFreePage(name, header)
					header.root = rootPage
					await setNode(name, rootPage, {
						inner: {keys: [promotedKey], children: [page, newPage]}
					})
				})())
				saving = false // there is no parent to save
			}
			await Promise.all(promises)
		}
	} while (saving) // continue until the save stops propagating
}

// Tries to coalesce a node with its siblings; returns whether successful
async function tryCoalesce(
	name: string, node: Node, path: PathItem[], header: Header
): Promise<boolean> {
	if (!path.length) return false // root node can't be coalesced
	const {len} = nodeType.encodeDelimited(node)
	if (len >= MIN_NODE_LENGTH) return false // ensure node is sufficiently empty

	// Find possible siblings to coalesce with
	const [{node: parentNode, index}] = path.slice(-1)
	// istanbul ignore if
	if ('leaf' in parentNode) throw new Error('Parent is not a leaf?')
	const {keys, children} = parentNode.inner
	let thisPage = children[index] // the page storing the current node
	const file = filename(name)
	const siblingLengths = await Promise.all(
		[true, false]
			.map(left => {
				const siblingIndex = left ? index - 1 : index + 1
				return {left, siblingIndex, siblingPage: children[siblingIndex]}
			})
			.filter(({siblingPage}) => siblingPage) // skip siblings that don't exist
			.map(async sibling => {
				const length = await getNodeLength(file, sibling.siblingPage)
				return {sibling, length}
			})
	)

	const originalNode = node
	const newFreePages: number[] = []
	// Coalescing is only possible with less than half-full siblings
	const coalesceCandidates =
		siblingLengths.filter(({length}) => length < MIN_NODE_LENGTH)
	while (coalesceCandidates.length) {
		// Choose the smaller of the candidate siblings to coalesce
		const coalesceSibling = argmin(coalesceCandidates, ({length}) => length)

		// Coalesce with selected sibling
		const [{sibling}] = coalesceCandidates.splice(coalesceSibling, 1)
		const {left, siblingIndex, siblingPage} = sibling
		const siblingNode = await getNode(name, siblingPage)
		// We always coalesce into the left sibling so that if it is a leaf,
		// we don't have to change the "next" value of the previous leaf
		let leftNode: Node, rightNode: Node
		let leftIndex = siblingIndex
		if (left) {
			leftNode = siblingNode
			rightNode = node
			newFreePages.push(thisPage)
			thisPage = siblingPage // the left sibling is the one that survives
		}
		else {
			leftNode = node
			rightNode = siblingNode
			leftIndex-- // siblingIndex is to the right, one index too high
			newFreePages.push(siblingPage)
		}

		// Remove key between the siblings and the right sibling's page from parent
		const [splitKey] = keys.splice(leftIndex, 1)
		children.splice(leftIndex + 1, 1)

		let newSize: number | undefined // don't calculate size unless it's needed
		if ('leaf' in leftNode) { // coalescing leaf nodes
			// istanbul ignore if
			if ('inner' in rightNode) throw new Error('Invalid sibling?')
			const leftLeaf = leftNode.leaf, rightLeaf = rightNode.leaf
			const values = leftLeaf.values.slice()
			// Copy right node's values because they are slices of its page,
			// which will be overwritten when it gets added to the free list
			for (const value of rightLeaf.values) values.push(value.slice())
			// Combine the keys and values
			node = {leaf: {
				keys: [...leftLeaf.keys, ...rightLeaf.keys],
				values,
				next: rightLeaf.next
			}}
		}
		else { // coalescing inner nodes
			// istanbul ignore if
			if ('leaf' in rightNode) throw new Error('Invalid sibling?')
			const leftInner = leftNode.inner, rightInner = rightNode.inner
			// Combine the nodes, demote the key between them
			const newNode = {inner: {
				keys: [...leftInner.keys, splitKey, ...rightInner.keys],
				children: [...leftInner.children, ...rightInner.children]
			}}
			// Ensure that this node is not too big (since it includes the split key)
			newSize = nodeType.encodeDelimited(newNode).len
			if (newSize > PAGE_SIZE) break
			node = newNode
		}

		// See if it is possible to coalesce with the other sibling
		if (coalesceCandidates.length) {
			newSize = newSize || nodeType.encodeDelimited(node).len
			if (newSize < MIN_NODE_LENGTH) {
				// The index of the right sibling goes down if the left sibling is removed
				if (left) coalesceCandidates[0].sibling.siblingIndex--
			}
			else break
		}
	}
	if (node === originalNode) return false // no coalescing occurred

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

// Generates the key-value pairs in a given key range
async function* pairsFrom(
	name: string, inclusive: boolean, start: KeyElement[], end?: KeyElement[]
): AsyncIterableIterator<SortedKeyValuePair> {
	const path = await lookup(name, await getHeader(name), start)
	let [{node, index}] = path.slice(-1)
	while (true) {
		// istanbul ignore if
		if ('inner' in node) throw new Error('Not a leaf?')
		const {keys, values, next} = node.leaf
		while (index < keys.length) {
			const key = keys[index].elements
			if (end) {
				const comparison = compareKeys(key, end)
				// If key is past end, or key is equal to end and the end is exclusive
				if (comparison > 0 || !(inclusive || comparison)) return
			}
			yield {key, value: values[index]}
			index++
		}
		if (next === LIST_END) break

		// Follow linked list of leaves to avoid retraversing the tree
		node = await getNode(name, next)
		index = 0
	}
}
const iterators = new Iterators<AsyncIterator<SortedKeyValuePair>>()

export async function create(name: string): Promise<void> {
	await addCollection(name, CollectionType.SORTED)
	const file = filename(name)
	await createFile(file)
	await setPageCount(file, 2) // allocate the header page and root node
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
	const header = await getHeader(name)
	const path = await lookup(name, header, searchKey)
	const [{node, index}] = path.slice(-1)
	// istanbul ignore if
	if ('inner' in node) throw new Error('Path does not end in a leaf?')
	const {keys, values} = node.leaf
	const oldKey = keys[index] as Key | undefined
	// If key doesn't match, deletion can't be performed
	if (!oldKey || compareKeys(oldKey.elements, searchKey)) {
		throw new Error('No matching key')
	}

	// Remove key-value pair from leaf and save all changed nodes along path
	keys.splice(index, 1)
	values.splice(index, 1)
	let coalesced: boolean
	do {
		const {page, node} = path.pop()!
		// Only coalesce if child was coalesced
		coalesced = await tryCoalesce(name, node, path, header)
		// If node couldn't be coalesced, save it as-is
		if (!coalesced) await setNode(name, page, node)
	} while (path.length && coalesced)
	header.size--
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
	if (key.some(element => 'uniquifier' in element)) {
		throw new Error('Key cannot include uniquifier')
	}
	await checkIsSorted(name)
	iterators.checkNoIterators(name)
	const header = await getHeader(name)
	const path = await lookup(name, header, key)
	const [{node, index}] = path.slice(-1)
	// istanbul ignore if
	if ('inner' in node) throw new Error('Path does not end in a leaf?')
	const {keys, values} = node.leaf

	// If the key is already there, add uniquifiers
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

	// Insert key-value pair and save nodes along path
	keys.splice(index, 0, {elements: key})
	values.splice(index, 0, value)
	await saveWithOverflow(name, key, path, header)
	header.size++
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
	// ">= []" will match all keys
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
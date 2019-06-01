import path from 'path'
import {addCollection, dropCollection, getCollections} from '.'
import {dataDir} from '../args'
import {createFile, FilePage, removeFile, setPageCount} from '../cache'
import {CollectionType, Key, KeyElement} from '../pb/interface'
import {
	Header,
	headerType,
	LIST_END,
	Node,
	nodeType
} from '../pb/sorted'

const HEADER_PAGE = 0
const INITIAL_ROOT_PAGE = 1

interface PathItem {
	page: number
	node: Node
	index: number
}

interface KeyValuePair {
	key: Key
	value: Uint8Array
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

const isUniquifier = (element: KeyElement): element is {uniquifier: number} =>
	'uniquifier' in element
function getUniquifier(key: Key): number | undefined {
	const uniquifier = key.elements.find(isUniquifier)
	return uniquifier && uniquifier.uniquifier
}
function compareKeys(key1: Key, key2: Key): number {
	const elements1 = key1.elements, elements2 = key2.elements
	const minLength = Math.min(elements1.length, elements2.length)
	for (let i = 0; i < minLength; i++) {
		const element1 = elements1[i], element2 = elements2[i]
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
			diff = element1.uniquifier - element2.uniquifier
		}
		if (diff) return diff
	}
	return 0
}
function lookupKey(key: Key, keys: Key[], leaf: boolean): number {
	const {length} = keys
	let index = length
	for (let i = 0; i < length; i++) {
		const comparison = compareKeys(keys[i], key)
		if (comparison > 0 || (leaf && !comparison)) {
			index = i
			break
		}
	}
	return index
}
async function lookup(name: string, key: Key): Promise<PathItem[]> {
	let {root: page} = await getHeader(name)
	const path: PathItem[] = []
	let leaf: boolean
	do {
		const node = await getNode(name, page)
		let keys: Key[]
		let children: number[] | undefined
		if ('leaf' in node) {
			({keys} = node.leaf)
			leaf = true
		}
		else {
			({keys, children} = node.inner)
			leaf = false
		}
		const index = lookupKey(key, keys, leaf)
		path.push({page, node, index})
		if (children) page = children[index]
	} while (!leaf)
	return path
}

async function* itemsFrom(
	name: string, node: Node, index: number
): AsyncIterableIterator<KeyValuePair> {
	while (true) {
		if ('inner' in node) throw new Error('Not a leaf?')
		const {keys, values, next} = node.leaf
		while (index < keys.length) {
			yield {key: keys[index], value: values[index]}
			index++
		}
		if (next === LIST_END) break

		node = await getNode(name, next)
		index = 0
	}
}

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
	await Promise.all([dropCollection(name), removeFile(filename(name))])
}

export async function get(name: string, searchKey: Key): Promise<Uint8Array[]> {
	await checkIsSorted(name)
	const path = await lookup(name, searchKey)
	const [{node, index}] = path.slice(-1)
	const values: Uint8Array[] = []
	for await (const {key, value} of itemsFrom(name, node, index)) {
		if (compareKeys(key, searchKey)) break

		values.push(value)
	}
	return values
}

export async function insert(
	name: string, key: Key, value: Uint8Array
): Promise<void> {
	if (getUniquifier(key) !== undefined) {
		throw new Error('Key cannot include uniquifier')
	}
	await checkIsSorted(name)
	const path = await lookup(name, key)
	const [{page, node, index}] = path.slice(-1)
	if ('inner' in node) throw new Error('Path does not end in a leaf?')
	const {keys, values} = node.leaf
	const oldKey: Key | undefined = keys[index]
	if (oldKey && !compareKeys(key, oldKey)) {
		const oldUniquifier = getUniquifier(oldKey)
		if (oldUniquifier === undefined) oldKey.elements.push({uniquifier: 0})
		key.elements.push({uniquifier: (oldUniquifier || 0) + 1})
	}
	keys.splice(index, 0, key)
	values.splice(index, 0, value)
	const header = await getHeader(name)
	header.size++
	// TODO: handle overflow
	await setNode(name, page, node)
	while (path.length > 1) {
		const {index} = path.pop()!
		if (index) break // not changing the minimum element

		const [{page, node, index: parentIndex}] = path.slice(-1)
		if ('leaf' in node) throw new Error('Parent is not an inner node?')
		node.inner.keys[parentIndex] = key
		await setNode(name, page, node)
	}
	await setHeader(name, header)
}
import * as path from 'path'
import {addCollection, dropCollection, getCollections} from '.'
import {createFile, FilePage, getPageCount, removeFile, setPageCount} from '../cache'
import {DATA_DIR} from '../constants'
import {
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

const filename = (name: string) =>
	path.join(DATA_DIR, `${name}.${COLLECTION_TYPE}`)

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
		new Uint8Array(page).set(
			headerType.encodeDelimited(headerType.fromObject(header)).finish()
		)
	)
const setNode = (name: string, page: number, node: Node): Promise<void> =>
	new FilePage(filename(name), page).use(async page =>
		new Uint8Array(page).set(
			nodeType.encodeDelimited(nodeType.fromObject(node)).finish()
		)
	)

async function lookup(name: string, index?: number, insert = false): Promise<PathItem[]> {
	const {child: {size, page}} = await getHeader(name)
	if (index === undefined) {
		if (!insert) throw new Error('Undefined index only allowed for insertion')
		index = size
	}
	else {
		if (index < 0) index += size
		if (index < 0 || !(index < size || insert && index === size)) {
			throw new Error(`Index ${index} is out of bounds in list of size ${size}`)
		}
	}

	const file = filename(name)
	const path: PathItem[] = []
	let lookupPage = page
	while (true) {
		const node = await new FilePage(file, lookupPage).use(async page =>
			nodeType.toObject(
				nodeType.decodeDelimited(new Uint8Array(page)),
				{defaults: true, longs: Number}
			)
		)
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

const split = <T>(arr: T[]): T[] => arr.splice(arr.length >> 1)

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
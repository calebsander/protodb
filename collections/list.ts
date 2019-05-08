import * as path from 'path'
import {addCollection, dropCollection, getCollections} from '.'
import {createFile, FilePage, getPageCount, removeFile, setPageCount} from '../cache'
import {DATA_DIR} from '../constants'
import {
	FREE_LIST_END,
	freePageType,
	Header,
	headerType,
	InnerNode,
	LeafNode,
	nodeType
} from '../sb-types/list'

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
	node: InnerNode | LeafNode
}

const getHeader = (name: string): Promise<Header> =>
	new FilePage(filename(name), HEADER_PAGE).use(async page =>
		headerType.consumeValue(page, 0).value
	)
const setHeader = (name: string, header: Header): Promise<void> =>
	new FilePage(filename(name), HEADER_PAGE).use(async page =>
		new Uint8Array(page).set(new Uint8Array(headerType.valueBuffer(header)))
	)
const setNode =
	(name: string, page: number, node: InnerNode | LeafNode): Promise<void> =>
		new FilePage(filename(name), page).use(async page =>
			new Uint8Array(page).set(new Uint8Array(nodeType.valueBuffer(node)))
		)

async function lookup(name: string, index: number | null, insert = false): Promise<PathItem[]> {
	const {child: {size, page}} = await getHeader(name)
	if (index === null) {
		if (!insert) throw new Error('Index null only allowed for insertion')
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
			nodeType.consumeValue(page, 0).value
		)
		path.push({page: lookupPage, index, node})
		if (node.type === 'leaf') return path

		for (const {size, page} of node.children) {
			if (index < size || insert && index === size) {
				lookupPage = page
				break
			}

			index -= size
		}
	}
}

async function getFreePage(name: string, header: Header): Promise<number> {
	const file = filename(name)
	const {freePage} = header
	if (freePage === FREE_LIST_END) {
		const pages = await getPageCount(file)
		await setPageCount(file, pages + 1)
		return pages
	}
	else {
		header.freePage = await new FilePage(file, freePage).use(async page =>
			freePageType.consumeValue(page, 0).value
		)
		return freePage
	}
}

const split = <T>(arr: T[]): T[] => arr.splice(arr.length >> 1)

const nodeSize = (node: InnerNode | LeafNode) =>
	node.type === 'inner'
		? node.children.reduce((totalSize, {size}) => totalSize + size, 0)
		: node.values.length

function getParent(path: PathItem[]) {
	const [parent]: (PathItem | undefined)[] = path.slice(-1)
	if (!parent) return undefined

	const {node: parentNode, index} = parent
	if (parentNode.type !== 'inner') throw new Error('Parent is not an inner node?')
	const {children} = parentNode
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
			freePage: FREE_LIST_END
		}),
		setNode(name, INITIAL_ROOT_PAGE, {type: 'leaf', values: []})
	])
}

export async function drop(name: string): Promise<void> {
	await checkIsList(name)
	await Promise.all([dropCollection(name), removeFile(filename(name))])
}

export async function get(name: string, listIndex: number): Promise<ArrayBuffer> {
	await checkIsList(name)
	const path = await lookup(name, listIndex)
	const [{index, node}] = path.slice(-1)
	if (node.type !== 'leaf') throw new Error('Path does not end in a leaf?')
	return node.values[index]
}

export async function set(
	name: string, listIndex: number, value: ArrayBuffer
): Promise<void> {
	await checkIsList(name)
	const path = await lookup(name, listIndex)
	const [{page, index, node}] = path.slice(-1)
	if (node.type !== 'leaf') throw new Error('Path does not end in a leaf?')
	node.values[index] = value
	await setNode(name, page, node)
}

export async function insert(
	name: string, listIndex: number | null, value: ArrayBuffer
): Promise<void> {
	await checkIsList(name)
	const path = await lookup(name, listIndex, true)
	const [{index, node}] = path.slice(-1)
	if (node.type !== 'leaf') throw new Error('Path does not end in a leaf?')
	node.values.splice(index, 0, value)
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

			const newNode: InnerNode | LeafNode = node.type === 'inner'
				? {type: 'inner', children: split(node.children)}
				: {type: 'leaf', values: split(node.values)}
			const makeNewNode = async () => {
				const newPage = await getFreePage(name, header)
				const oldChild = {size: nodeSize(node), page},
				      newChild = {size: nodeSize(newNode), page: newPage}
				const promises = [setNode(name, newPage, newNode)]
				if (parent) {
					const {children, index} = parent
					children.splice(index, 1, oldChild, newChild)
				}
				else { // splitting the root node
					const makeNewRoot = async () => {
						const rootPage = await getFreePage(name, header)
						header.child.page = rootPage
						await setNode(
							name, rootPage, {type: 'inner', children: [oldChild, newChild]}
						)
					}
					promises.push(makeNewRoot())
				}
				await Promise.all(promises)
			}
			await Promise.all([setNode(name, page, node), makeNewNode()])
		}
	}
	header.child.size++
	await setHeader(name, header)
}
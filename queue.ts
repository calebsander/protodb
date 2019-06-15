const DEFAULT_INITIAL_SIZE = 16

export class Queue<T> {
	private buffer: T[]
	private head: number
	private tail: number

	constructor(initialSize = DEFAULT_INITIAL_SIZE) {
		this.buffer = new Array(initialSize)
		this.head = this.tail = 0
	}

	enqueue(elem: T): void {
		this.buffer[this.tail] = elem
		const {length} = this.buffer
		this.tail = (this.tail + 1) % length
		if (this.tail === this.head) {
			const newBuffer = new Array<T>(length << 1)
			let j = 0
			for (let i = this.head; i < length; i++, j++) {
				newBuffer[j] = this.buffer[i]
			}
			for (let i = 0; i < this.head; i++, j++) {
				newBuffer[j] = this.buffer[i]
			}
			this.buffer = newBuffer
			this.head = 0
			this.tail = length
		}
	}
	dequeue(): T {
		if (this.head === this.tail) throw new Error('Empty list')
		const head = this.buffer[this.head]
		this.head = (this.head + 1) % this.buffer.length
		return head
	}
}
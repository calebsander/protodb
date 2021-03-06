const DEFAULT_INITIAL_SIZE = 16

// A standard ring-buffer queue. Doubles in size when the buffer fills.
export class Queue<T> {
	private buffer: T[]
	private head = 0
	private tail = 0

	constructor(initialSize = DEFAULT_INITIAL_SIZE) {
		// istanbul ignore if
		if (initialSize <= 0) throw new Error('Initial size must be positive')

		this.buffer = new Array(initialSize)
	}

	enqueue(elem: T): void {
		this.buffer[this.tail] = elem
		const {length} = this.buffer
		this.tail = (this.tail + 1) % length
		if (this.tail === this.head) {
			const newBuffer = new Array<T>(length << 1)
			let i = this.head, j = 0
			while (i < length) newBuffer[j++] = this.buffer[i++]
			i = 0
			while (i < this.head) newBuffer[j++] = this.buffer[i++]
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
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const DEFAULT_INITIAL_SIZE = 16;
class Queue {
    constructor(initialSize = DEFAULT_INITIAL_SIZE) {
        this.head = 0;
        this.tail = 0;
        // istanbul ignore if
        if (initialSize <= 0)
            throw new Error('Initial size must be positive');
        this.buffer = new Array(initialSize);
    }
    enqueue(elem) {
        this.buffer[this.tail] = elem;
        const { length } = this.buffer;
        this.tail = (this.tail + 1) % length;
        if (this.tail === this.head) {
            const newBuffer = new Array(length << 1);
            let j = 0;
            for (let i = this.head; i < length; i++, j++) {
                newBuffer[j] = this.buffer[i];
            }
            for (let i = 0; i < this.head; i++, j++) {
                newBuffer[j] = this.buffer[i];
            }
            this.buffer = newBuffer;
            this.head = 0;
            this.tail = length;
        }
    }
    dequeue() {
        if (this.head === this.tail)
            throw new Error('Empty list');
        const head = this.buffer[this.head];
        this.head = (this.head + 1) % this.buffer.length;
        return head;
    }
}
exports.Queue = Queue;

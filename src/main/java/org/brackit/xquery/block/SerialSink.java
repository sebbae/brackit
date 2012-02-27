/*
 * [New BSD License]
 * Copyright (c) 2011-2012, Brackit Project Team <info@brackit.org>  
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the Brackit Project Team nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.brackit.xquery.block;

import java.util.ArrayList;
import java.util.concurrent.Semaphore;

import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.util.forkjoin.Deque;
import org.brackit.xquery.util.forkjoin.Task;
import org.brackit.xquery.util.forkjoin.Worker;

/**
 * 
 * @author Sebastian Baechle
 *
 */
public abstract class SerialSink implements Sink {

	private static final int NO_TOKEN = 0;
	private static final int WAIT_TOKEN = 1;
	private static final int HAS_TOKEN = 2;
	private static final int FAILED = 3;
	private static final boolean BOUNDED_FORWARD = true;
	private static final boolean ALWAYS_BACKWARD = false;
	private static final int START_STATE = NO_TOKEN;

	final Semaphore sem;
	SerialSink next;
	Tuple[] head;
	int size;
	volatile int state;
	volatile Deque<Task> deposit;

	public SerialSink(int permits) {
		this.state = HAS_TOKEN;
		this.sem = new Semaphore(permits);
	}

	protected SerialSink(Semaphore sem, SerialSink next) {
		this.state = START_STATE;
		this.sem = sem;
		this.next = next;
	}

	public final SerialSink fork() {
		return (next = doFork(sem, next));
	}

	protected abstract SerialSink doFork(Semaphore sem, SerialSink next);

	protected abstract void doOutput(Tuple[] buf, int len)
			throws QueryException;

	protected void doBegin() throws QueryException {
	}

	protected void doEnd() throws QueryException {
	}

	protected void doFail() throws QueryException {
	}

	@Override
	public final void output(Tuple[] buf, int len) throws QueryException {
		// stats.get().out++;
		if (state == HAS_TOKEN) {
			outputQueued();
			doOutput(buf, len);
		} else {
			// locally queue output
			// System.out.println(Thread.currentThread() + " enqueue " + this);
			enqueue(buf, len);
		}
	}

	public final void end() throws QueryException {
		// System.out.println("END " + this);
		// Stats s = stats.get();
		// s.end++;

		if (state == HAS_TOKEN) {
			outputQueued();
			promoteToken();
			return;
		}

		// hold reference to work queue
		Worker worker = (Worker) Thread.currentThread();
		Deque<Task> queue = worker.getQueue();
		deposit = queue;

		// attempt to put predecessor in charge of doing the output
		if (compareAndSet(NO_TOKEN, WAIT_TOKEN)) {
			// System.out.println(Thread.currentThread() +
			// " passed end() from " + this + " to predecessor");
			// System.out.println(Thread.currentThread() + " passed " +
			// head);
			// s.passed++;
			if (head == null) {
				if (!compareAndSet(queue, null)) {
					worker.dropQueue();
				}
				return;
			}
			if ((ALWAYS_BACKWARD)
					|| ((BOUNDED_FORWARD) && ((!sem.tryAcquire(size)) || (!compareAndSet(
							queue, null))))) {
				// drop local queue
				worker.dropQueue();
			}
			return;
		}

		if (state == HAS_TOKEN) {
			outputQueued();
			promoteToken();
			doEnd();
		} else {
			dropForked();
			clearQueued();
			promoteFailure();
		}
	}

	public final void begin() throws QueryException {
		doBegin();
	}

	public final void fail() throws QueryException {
		state = FAILED;
		sem.drainPermits();
		promoteFailure();
		doFail();
	}

	private void promoteToken() throws QueryException {
		// process local queues of finished successors
		SerialSink n = next;
		next = null;
		int run = 0;
		while ((n != null) && (!n.compareAndSet(NO_TOKEN, HAS_TOKEN))) {
			if (n.state == WAIT_TOKEN) {
				run++;
				takeover(n);
				n = n.next;
			} else {
				dropForked();
				n.promoteFailure();
				break;
			}
		}
		if (run > 0) {
			// Stats s = stats.get();
			// s.minRun = Math.min(s.minRun, run);
			// s.maxRun = Math.max(s.maxRun, run);
		}
		if (n == null) {
			doEnd();
		}
	}

	private void dropForked() {
		Deque<Task> deposit = ((Worker) Thread.currentThread()).getQueue();
		// TODO cleanup tasks
	}

	private void promoteFailure() throws QueryException {
		SerialSink n = next;
		// forward promote failure to forked sinks
		while ((n != null) && (!n.compareAndSet(NO_TOKEN, FAILED))) {
			if (n.state == FAILED) {
				break;
			}
			n.clearQueued(); // allow gc
			// synchronized (n)
			{
				if (n.deposit != null) {
					// TODO cleanup tasks
				}
			}
			doFail();
			n = n.next;
		}
		if (n == null) {
			// notify external caller?
		}
	}

	private void takeover(SerialSink n) throws QueryException {
		// System.out.println(Thread.currentThread() +
		// " process end() from " + this + " for " + n);
		n.outputQueued();
		Deque<Task> queue = n.deposit;
		if ((queue != null) && (n.compareAndSet(queue, null))) {
			sem.release(n.size);
			((Worker) Thread.currentThread()).adopt(queue);
		}
		// stats.get().takeover++;
	}

	private void enqueue(Tuple[] buf, int len) {
		this.head = buf;
		this.size = len;
	}

	private void clearQueued() {
		head = null;
	}

	private void outputQueued() throws QueryException {
		if (head != null) {
			doOutput(head, size);
		}
		// allow garbage collection
		head = null;
	}

	public String toString() {
		return "[" + String.valueOf(me()) + "]";
	}

	private String chain() {
		String s = String.valueOf(me());
		if (state == HAS_TOKEN) {
			s += "*";
		}
		SerialSink n = next;
		while (n != null) {
			s += "->" + n.me();
			if (n.state == HAS_TOKEN) {
				s += "*";
			}
			n = n.next;
		}
		return s;
	}

	private int me() {
		return System.identityHashCode(this);
	}

	private boolean compareAndSet(int expected, int set) {
		synchronized (this) {
			if (state == expected) {
				state = set;
				return true;
			}
			return false;
		}
	}

	private boolean compareAndSet(Deque<Task> expected, Deque<Task> set) {
		synchronized (this) {
			if (deposit == expected) {
				deposit = set;
				return true;
			}
			return false;
		}
	}

	// static AtomicInteger meCnt = new AtomicInteger();
	// final int me = meCnt.incrementAndGet();

	public static ArrayList<Stats> statlist = new ArrayList<Stats>();
	static final ThreadLocal<Stats> stats = new ThreadLocal<Stats>() {
		@Override
		protected Stats initialValue() {
			Stats s = new Stats();
			s.t = Thread.currentThread();
			synchronized (this) {
				statlist.add(s);
			}
			return s;
		}
	};

	public static class Stats {
		public Thread t;
		long tupleout;
		int out;
		int end;
		int passed;
		int takeover;
		int minRun = Integer.MAX_VALUE;
		int maxRun = Integer.MIN_VALUE;

		public String toString() {
			return "out=" + out + " tuples=" + tupleout + " end=" + end
					+ " passed=" + passed + " takeover=" + takeover
					+ " minRun=" + minRun + " maxRun=" + maxRun;
		}
	}
}
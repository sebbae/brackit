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
package org.brackit.xquery.util.forkjoin;

import java.util.concurrent.locks.LockSupport;

/**
 * Thread pool for fork/join tasks.
 * 
 * As soon as we are sure that all our parallel processing fully fits to Java
 * 7's fork/join framework, we should abandon this package anyway and stick with
 * the optimized fork/join implementation of respective Java 7 JVMs.
 * 
 * @author Sebastian Baechle
 * 
 */
public class Pool {
	private static final boolean LOG = false;
	private final int size;
	private final Worker[] workers;
	private final CASDeque<Worker> inactive;

	public Pool(int size, WorkerFactory factory) {
		this.size = size;
		inactive = new CASDeque<Worker>();
		workers = new Worker[size];
		for (int i = 0; i < size; i++) {
			workers[i] = factory.newThread(this);
			workers[i].setDaemon(true);
		}
		for (int i = 0; i < size; i++) {
			workers[i].start();
		}
	}

	public int getSize() {
		return size;
	}

	public void signalWork() {
		Worker w = inactive.poll();
		if (w != null) {
			LockSupport.unpark(w);
		}
	}

	Task stealTask(Worker stealer) {
		Task t;
		if ((stealer.victim != null)
				&& ((t = stealer.victim.pollLast()) != null)) {
			if (LOG) {
				System.out.println(stealer + " stole from last victim "
						+ stealer.victim);
			}
			return t;
		}
		Worker[] ws = workers;
		for (int i = 0; i < ws.length; i++) {
			if ((t = ws[i].pollLast()) != null) {
				if (LOG) {
					System.out.println(stealer + " stole from " + ws[i]);
				}
				stealer.victim = ws[i];
				return t;
			}
		}
		stealer.victim = null;
		return null;
	}

	public Task submit(Task task) {
		Thread me;
		if ((me = Thread.currentThread()) instanceof Worker) {
			((Worker) me).fork(task);
			return task;
		}
		Worker w = inactive.poll();
		if (w != null) {
			w.push(task);
			LockSupport.unpark(w);
		} else {
			// TODO choose random active worker
			workers[0].push(task);
		}
		return task;
	}

	void join(Worker w, Task join) {
		Task t;
		int s;
		while ((s = join.status) <= 0) {
			if ((t = w.poll()) != null) {
				// process least recently forked local task
				t.exec();
			} else if ((t = stealTask(w)) != null) {
				// process stolen task from other thread
				t.exec();
			} else {
				LockSupport.parkNanos(1000);
			}
		}
	}

	void joinLast(Worker w, Task join) {
		Task t;
		int s;
		while ((s = join.status) <= 0) {
			if ((t = w.pollLast()) != null) {
				// process most recently forked local task
				t.exec();
			} else if ((t = stealTask(w)) != null) {
				// process stolen task from other thread
				t.exec();
			} else {
				LockSupport.parkNanos(1000);
			}
		}
	}

	void run(Worker w) {
		int retry = 0;
		while (!w.isTerminate()) {
			Task t;
			if ((t = w.poll()) != null) {
				t.exec();
			} else if ((t = stealTask(w)) != null) {
				t.exec();
			} else if (++retry == 64) {
				if (LOG) {
					System.out.println(w + " goes parking");
				}
				inactive.push(w);
				LockSupport.park();
				retry = 0;
				if (LOG) {
					System.out.println(w + " unparking");
				}
			} else if (retry % 16 == 0) {
				LockSupport.parkNanos(1000);
			}
		}
	}
}
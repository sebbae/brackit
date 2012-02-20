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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * 
 * @author Sebastian Baechle
 *
 */
public abstract class Task {

	private static final AtomicReferenceFieldUpdater<Task, Worker> OWNER_CAS = AtomicReferenceFieldUpdater
			.newUpdater(Task.class, Worker.class, "owner");

	volatile int status = 0;
	volatile Worker owner;
	Throwable throwable;

	public abstract void compute() throws Throwable;

	boolean assign(Worker w) {
		return (OWNER_CAS.compareAndSet(this, null, w));
	}
	
	void exec() {
		try {
			int s = status;
			if (s != 0) {
				throw new RuntimeException("Illegal state: " + s);
			}
			compute();
			status = 1; // done
		} catch (Throwable e) {
			e.printStackTrace();
			status = -1; // error
			throwable = e;
		}
		signalFinish();
	}

	protected void fork() {
		((Worker) Thread.currentThread()).fork(this);
	}

	protected void join() {
		Thread me;
		if ((me = Thread.currentThread()) instanceof Worker) {
			Worker w = (Worker) me;
			w.join(this);
		} else {
			externalWaitForFinish();
		}
	}

	protected void joinLast() {
		Thread me;
		if ((me = Thread.currentThread()) instanceof Worker) {
			Worker w = (Worker) me;
			w.joinLast(this);
		} else {
			externalWaitForFinish();
		}
	}

	public void waitForFinish() {
		externalWaitForFinish();
	}

	synchronized void signalFinish() {
		notifyAll();
	}

	synchronized void externalWaitForFinish() {
		while (status == 0) {
			try {
				wait();
			} catch (InterruptedException e) {
			}
		}
	}
}
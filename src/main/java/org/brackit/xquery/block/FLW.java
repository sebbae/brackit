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

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.operator.Cursor;
import org.brackit.xquery.operator.LetBind;
import org.brackit.xquery.operator.Operator;
import org.brackit.xquery.operator.Select;
import org.brackit.xquery.util.forkjoin.Task;

/**
 * 
 * @author Sebastian Baechle
 * 
 */
public class FLW implements Block {

	final Operator[] op;

	public FLW(Operator[] op) {
		this.op = op;
	}

	@Override
	public int outputWidth(int initSize) {
		int s = initSize;
		for (Operator o : op) {
			s = o.tupleWidth(s);
		}
		return s;
	}

	@Override
	public Sink create(QueryContext ctx, Sink sink) throws QueryException {
		return new FLWSink(ctx, sink);
	}

	private static class Buf {
		Tuple[] b;
		int len;

		Buf(int size) {
			this.b = new Tuple[size];
		}

		boolean add(Tuple t) {
			b[len++] = t;
			return len < b.length;
		}
	}

	private class Slice extends Task {
		final QueryContext ctx;
		final Sink s;
		final Cursor off;
		final int d;
		volatile Slice fork;

		private Slice(QueryContext ctx, Sink s, Cursor off, int d) {
			this.ctx = ctx;
			this.s = s;
			this.off = off;
			this.d = d;
		}

		public void compute() throws QueryException {
			fork = bind(off, d);
			while ((fork != null) && (fork.finished())) {
				fork = fork.fork;
			}
		}

		private Slice bind(Cursor c, int d) throws QueryException {
			Buf buf = new Buf(bufSize(d));
			Slice fork = fillBuffer(c, d, buf);
			if (buf.len == 0) {
				s.begin();
				s.end();
				return null;
			}
			// process local share
			if (d + 1 < op.length) {
				descend(d, buf);
			} else {
				output(buf);
			}
			return fork;
		}

		private void output(Buf buf) throws QueryException {
			s.begin();
			try {
				s.output(buf.b, buf.len);
			} catch (QueryException e) {
				s.fail();
				throw e;
			}
			s.end();
		}

		private void descend(int d, Buf buf) throws QueryException {
			Cursor c2 = op[d + 1].create(ctx, buf.b, buf.len);
			c2.open(ctx);
			buf = null; // allow gc
			Slice fork = bind(c2, d + 1);
			while (fork != null) {
				fork.join();
				fork = fork.fork;
			}
		}

		private Slice fillBuffer(Cursor c, int d, Buf buf)
				throws QueryException {
			while (true) {
				Tuple t = c.next(ctx);
				if (t != null) {
					if (!buf.add(t)) {
						Slice fork = new Slice(ctx, s.fork(), c, d);
						fork.fork();
						return fork;
					}
				} else {
					c.close(ctx);
					c = null; // allow garbage collection
					return null;
				}
			}
		}

		private int bufSize(int d) {
			if (d < 0) {
				return 1;
			}
			if (op[d] instanceof LetBind) {
				return (d == 0) ? 1 : bufSize(d - 1);
			}
			if (op[d] instanceof Select) {
				return bufSize(d - 1);
			}
			// TODO
			return (d < FJControl.FORK_BUFFER.length) ? FJControl.FORK_BUFFER[d]
					: FJControl.FORK_BUFFER_DEFAULT;
		}

		public String toString() {
			return "Slice L=" + d + " " + System.identityHashCode(this);
		}
	}

	private class FLWSink extends FJControl implements Sink {
		Sink s;
		final QueryContext ctx;

		private FLWSink(QueryContext ctx, Sink s) {
			this.ctx = ctx;
			this.s = s;
		}

		@Override
		public void output(Tuple[] t, int len) throws QueryException {
			Cursor c = op[0].create(ctx, t, len);
			c.open(ctx);
			// fork sink for future output calls
			Sink ss = s;
			s = s.fork();
			Slice task = new Slice(ctx, ss, c, 0);
			task.compute();
			Slice fork = task.fork;
			while (fork != null) {
				fork.join();
				fork = fork.fork;
			}
		}

		@Override
		public Sink fork() {
			return new FLWSink(ctx, s.fork());
		}

		@Override
		public void fail() throws QueryException {
			s.begin();
			s.fail();
		}

		@Override
		public void begin() throws QueryException {
			// do nothing
		}

		@Override
		public void end() throws QueryException {
			s.begin();
			s.end();
		}
	}
}
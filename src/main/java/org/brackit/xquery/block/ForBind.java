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

import java.util.ArrayDeque;
import java.util.Queue;

import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.atomic.IntNumeric;
import org.brackit.xquery.util.forkjoin.Pool;
import org.brackit.xquery.util.forkjoin.Task;
import org.brackit.xquery.xdm.Expr;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Iter;
import org.brackit.xquery.xdm.Sequence;

/**
 * 
 * @author Sebastian Baechle
 * 
 */
public class ForBind implements Block {

	final Expr expr;
	final boolean allowingEmpty;
	boolean bindVar = true;
	boolean bindPos = false;

	public ForBind(Expr bind, boolean allowingEmpty) {
		this.expr = bind;
		this.allowingEmpty = allowingEmpty;
	}

	@Override
	public int outputWidth(int initSize) {
		return initSize + ((bindVar) ? 1 : 0) + ((bindPos) ? 1 : 0);
	}

	@Override
	public Sink create(QueryContext ctx, Sink sink) throws QueryException {
		return new ForBindSink(ctx, sink);
	}

	public void bindVariable(boolean bindVariable) {
		this.bindVar = bindVariable;
	}

	public void bindPosition(boolean bindPos) {
		this.bindPos = bindPos;
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
		final Sink s;
		final Tuple t;
		final Iter it;
		IntNumeric pos;
		volatile Slice fork;

		private Slice(Sink s, Tuple t, Iter it, IntNumeric pos) {
			this.s = s;
			this.t = t;
			this.it = it;
			this.pos = pos;
		}

		public void compute() throws QueryException {
			fork = bind(it);
			while ((fork != null) && (fork.finished())) {
				fork = fork.fork;
			}
		}

		private Slice bind(Iter c) throws QueryException {
			Buf buf = new Buf(bufSize());
			Slice fork = fillBuffer(c, buf);
			if (buf.len == 0) {
				s.begin();
				s.end();
				return null;
			}
			// process local share
			output(buf);
			return fork;
		}

		private void output(Buf buf) throws QueryException {
			s.begin();
			try {
				for (int i = 0; i < buf.len; i++) {
					buf.b[i] = emit(t, (Sequence) buf.b[i]);
				}
				s.output(buf.b, buf.len);
			} catch (QueryException e) {
				s.fail();
				throw e;
			}
			s.end();
		}

		private Tuple emit(Tuple t, Sequence item) throws QueryException {
			if (bindVar) {
				if (bindPos) {
					return t.concat(new Sequence[] { item,
							(item != null) ? (pos = pos.inc()) : pos });
				} else {
					return t.concat(item);
				}
			} else if (bindPos) {
				return t.concat((item != null) ? (pos = pos.inc()) : pos);
			} else {
				return t;
			}
		}

		private Slice fillBuffer(Iter it, Buf buf) throws QueryException {
			while (true) {
				Item i = it.next();
				if (i != null) {
					if (!buf.add(i)) {
						IntNumeric npos = (IntNumeric) ((pos != null) ? pos
								.add(new Int32(buf.len)) : null);
						Slice fork = new Slice(s.fork(), t, it, npos);
						fork.fork();
						it = null;
						return fork;
					}
				} else {
					it.close();
					it = null; // allow garbage collection
					return null;
				}
			}
		}

		private int bufSize() {
			// TODO
			return 20;
		}
	}

	private class ProduceTask extends Task {
		final Tuple t;
		final Sequence s;
		Sink sink;
		IntNumeric pos = (bindPos) ? Int32.ONE : null;

		public ProduceTask(Sink sink, Tuple t, Sequence s) {
			this.sink = sink;
			this.t = t;
			this.s = s;
		}

		@Override
		public void compute() throws QueryException {
			Pool pool = FJControl.POOL;
			int qsize = pool.getSize();
			Queue<EmitTask> queue = new ArrayDeque<EmitTask>(qsize * 3);
			Iter it = s.iterate();
			try {
				Item i;
				int size = bufSize();
				Item[] buf = new Item[size];
				int len = 0;
				while ((i = it.next()) != null) {
					buf[len++] = i;
					if (len == size) {
						emit(pool, qsize, queue, buf, len);
						buf = new Item[size];
						len = 0;
					}
				}
				if (len > 0) {
					emit(pool, qsize, queue, buf, len);
				}
				drainQueue(queue);
			} finally {
				it.close();
			}
			sink.begin();
			sink.end();
		}

		private void emit(Pool pool, int qsize, Queue<EmitTask> queue,
				Item[] buf, int len) throws QueryException {
			Sink ss = sink;
			sink = sink.fork();
			pos = (IntNumeric) ((pos != null) ? pos.add(new Int32(len)) : null);
			EmitTask et = new EmitTask(ss, t, buf, len, pos);
			if (pool.dispatch(et)) {
				queue.add(et);
				if (queue.size() == qsize) {
					maintainQueue(queue);
				}
			} else {
				et.join();
			}
		}

		private void drainQueue(Queue<EmitTask> queue) {
			EmitTask et;
			while ((et = queue.poll()) != null) {
				if (!et.finished()) {
					et.join();
				}
			}
		}

		private void maintainQueue(Queue<EmitTask> queue) {
			int size = queue.size();
			int cleared = 0;
			EmitTask et;
			for (int i = 0; i < size; i++) {
				et = queue.poll();
				if (et.finished()) {
					cleared++;
				} else {
					queue.add(et);
				}
			}
			if (cleared == 0) {
				queue.poll().join();
			}
//			EmitTask et;
//			while (((et = queue.poll()) != null) && (!et.finished()));
		}

		private int bufSize() {
			// TODO
			return 50;
		}
	}

	private class EmitTask extends Task {
		final Sink s;
		final Tuple t;
		final Item[] items;
		final int len;
		IntNumeric pos;

		public EmitTask(Sink s, Tuple t, Item[] items, int len, IntNumeric pos) {
			this.s = s;
			this.t = t;
			this.items = items;
			this.len = len;
			this.pos = pos;
		}

		@Override
		public void compute() throws QueryException {
			s.begin();
			try {
				Tuple[] buf = new Tuple[len];
				for (int i = 0; i < len; i++) {
					buf[i] = emit(t, items[i]);
				}
				s.output(buf, len);
				s.end();
			} catch (QueryException e) {
				s.fail();
				throw e;
			} catch (Throwable e) {
				s.fail();
				throw new QueryException(e, ErrorCode.BIT_DYN_INT_ERROR);
			}
		}

		private Tuple emit(Tuple t, Sequence item) throws QueryException {
			if (bindVar) {
				if (bindPos) {
					return t.concat(new Sequence[] { item,
							(item != null) ? (pos = pos.inc()) : pos });
				} else {
					return t.concat(item);
				}
			} else if (bindPos) {
				return t.concat((item != null) ? (pos = pos.inc()) : pos);
			} else {
				return t;
			}
		}
	}

	private class ForBindTask extends Task {
		private final QueryContext ctx;
		private final Tuple[] buf;
		private final int start;
		private final int end;
		private Sink sink;

		public ForBindTask(QueryContext ctx, Sink sink, Tuple[] buf, int start,
				int end) {
			this.ctx = ctx;
			this.sink = sink;
			this.buf = buf;
			this.start = start;
			this.end = end;
		}

		@Override
		public void compute() throws QueryException {
			if (end - start > 50) {
				int mid = start + ((end - start) / 2);
				ForBindTask a = new ForBindTask(ctx, sink.fork(), buf, mid, end);
				ForBindTask b = new ForBindTask(ctx, sink, buf, start, mid);
				a.fork();
				b.compute();
				a.join();
			} else {
				IntNumeric pos = (bindPos) ? Int32.ZERO : null;
				for (int i = start; i < end; i++) {
					Sequence s = expr.evaluate(ctx, buf[i]);
					Sink ss = sink;
					sink = sink.fork();
					if (s != null) {
						ProduceTask t = new ProduceTask(ss, buf[i], s);
						t.compute();
					}
				}
				sink.begin();
				sink.end();
			}
		}
	}

	private class ForBindSink extends FJControl implements Sink {
		Sink s;
		final QueryContext ctx;

		private ForBindSink(QueryContext ctx, Sink s) {
			this.ctx = ctx;
			this.s = s;
		}

		@Override
		public void output(Tuple[] t, int len) throws QueryException {
			// fork sink for future output calls
			Sink ss = s;
			s = s.fork();
			ForBindTask task = new ForBindTask(ctx, ss, t, 0, len);
			task.compute();
		}

		@Override
		public Sink fork() {
			return new ForBindSink(ctx, s.fork());
		}

		@Override
		public Sink partition(Sink stopAt) {
			return new ForBindSink(ctx, s.partition(stopAt));
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
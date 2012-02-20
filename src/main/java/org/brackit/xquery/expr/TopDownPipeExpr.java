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
package org.brackit.xquery.expr;

import java.util.concurrent.Semaphore;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.atomic.Int64;
import org.brackit.xquery.atomic.IntNumeric;
import org.brackit.xquery.block.AsyncSink;
import org.brackit.xquery.block.Block;
import org.brackit.xquery.block.FJControl;
import org.brackit.xquery.block.SerialSink;
import org.brackit.xquery.block.SerialSink.Stats;
import org.brackit.xquery.block.Sink;
import org.brackit.xquery.block.MutexSink;
import org.brackit.xquery.sequence.FlatteningSequence;
import org.brackit.xquery.util.ExprUtil;
import org.brackit.xquery.util.forkjoin.Task;
import org.brackit.xquery.util.join.FastList;
import org.brackit.xquery.xdm.Expr;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Sequence;

/**
 * 
 * @author Sebastian Baechle
 * 
 */
public class TopDownPipeExpr implements Expr {

	interface ReturnSink extends AsyncSink {
		Sequence asSequence();
	}

	static class CheckOrder extends SerialSink implements ReturnSink {
		static class State {
			Tuple pre;
			long count = 0;
		}

		final State s;

		public CheckOrder(int permits) {
			super(permits);
			s = new State();
		}

		private CheckOrder(Semaphore sem, CB cb, SerialSink next, State s) {
			super(sem, cb, next);
			this.s = s;
		}

		@Override
		protected SerialSink doFork(Semaphore sem, CB cb, SerialSink next) {
			return new CheckOrder(sem, cb, next, s);
		}

		@Override
		protected void doOutput(Tuple[] buf, int len) throws QueryException {
			for (int i = 0; i < len; i++) {
				Tuple v = buf[i];
				// long s = System.currentTimeMillis();
				// System.out.println(Thread.currentThread() + "\t" + v);
				s.count++;
				// if (s.count % 10000 == 0)
				// System.out.println(Thread.currentThread() + " output: " +
				// s.count);
				if (s.pre != null) {
					for (int j = 0; j < v.getSize(); j++) {
						IntNumeric pi = (IntNumeric) s.pre.get(j);
						IntNumeric vi = (IntNumeric) v.get(j);
						if (pi.atomicCmp(vi) == 0) {
							continue;
						} else if (pi.inc().atomicCmp(vi) == 0) {
							break;
						} else {
							System.err.println(Thread.currentThread() + ": "
									+ s.pre + " >= " + v);
							throw new RuntimeException(Thread.currentThread()
									+ ": " + s.pre + " >= " + v);
						}
					}
				}
				s.pre = v;
				// long e = System.currentTimeMillis();
				// total += (e - s);
			}
		}

		@Override
		public void doEnd() {
		}

		@Override
		public Sequence asSequence() {
			return new Int64(s.count);
		}
	}

	static class OrderedReturn extends SerialSink implements ReturnSink {
		final QueryContext ctx;
		final Expr expr;
		final FastList<Sequence> buf;

		public OrderedReturn(int permits, QueryContext ctx, Expr expr) {
			super(permits);
			this.ctx = ctx;
			this.expr = expr;
			this.buf = new FastList<Sequence>();
		}

		protected OrderedReturn(Semaphore sem, CB cb, SerialSink next,
				QueryContext ctx, Expr expr, FastList<Sequence> buf) {
			super(sem, cb, next);
			this.ctx = ctx;
			this.expr = expr;
			this.buf = buf;
		}

		@Override
		protected SerialSink doFork(Semaphore sem, CB cb, SerialSink next) {
			return new OrderedReturn(sem, cb, next, ctx, expr, buf);
		}

		@Override
		protected void doOutput(Tuple[] buf, int len) throws QueryException {
			for (int i = 0; i < len; i++) {
				Sequence s = expr.evaluate(ctx, buf[i]);
				if (s != null) {
					this.buf.add(s);
				}
			}
		}

		@Override
		protected void doBegin() {
		}

		@Override
		protected void doEnd() throws QueryException {
		}

		@Override
		public Sequence asSequence() {
			return new FlatteningSequence() {
				final int len = buf.getSize();

				@Override
				protected Sequence sequence(int pos) throws QueryException {
					return (pos < len) ? buf.get(pos) : null;
				}
			};
		}
	}

	static class UnorderedReturn extends MutexSink implements ReturnSink {
		final QueryContext ctx;
		final Expr expr;
		final FastList<Sequence> buf;

		public UnorderedReturn(QueryContext ctx, Expr expr) {
			this.ctx = ctx;
			this.expr = expr;
			this.buf = new FastList<Sequence>();
		}

		@Override
		protected int doPreOutput(Tuple[] buf, int len) throws QueryException {
			int nlen = 0;
			for (int i = 0; i < len; i++) {
				Sequence s = expr.evaluate(ctx, buf[i]);
				if (s != null) {
					buf[nlen++] = s;
				}
			}
			return nlen;
		}

		@Override
		protected void doOutput(Tuple[] buf, int len) throws QueryException {
			this.buf.addAllSafe(buf, 0, len);
		}

		@Override
		protected void doBegin() {
		}

		@Override
		protected void doEnd() throws QueryException {
		}

		@Override
		public Sequence asSequence() {
			return new FlatteningSequence() {
				final int len = buf.getSize();

				@Override
				protected Sequence sequence(int pos) throws QueryException {
					return (pos < len) ? buf.get(pos) : null;
				}
			};
		}
	}

	private final Block[] blocks;
	private final Expr expr;
	private final boolean ordered;

	public TopDownPipeExpr(Block[] blocks, Expr expr, boolean ordered) {
		this.blocks = blocks;
		this.expr = expr;
		this.ordered = ordered;
	}

	@Override
	public Sequence evaluate(final QueryContext ctx, final Tuple t)
			throws QueryException {

		ReturnSink s = (ordered) ? new OrderedReturn(FJControl.PERMITS, ctx,
				expr) : new UnorderedReturn(ctx, expr);
		// ReturnSink s = new CheckOrder(ForkJoinControl.PERMITS);

		Sink sink = s;
		for (int i = blocks.length - 1; i >= 0; i--) {
			sink = blocks[i].create(ctx, sink);
		}
		final Sink start = sink;

		FJControl.POOL.submit(new Task() {
			@Override
			public void compute() throws Throwable {
				start.begin();
				try {
					start.output(new Tuple[] { t }, 1);
					start.end();
				} catch (QueryException e) {
					start.fail();
					throw e;
				}
			}
		});

		// s.end();
		s.waitForCompletion();

		for (Stats stat : SerialSink.statlist) {
			System.out.println(stat.t.getName() + " " + stat);
		}

		Sequence sequence = s.asSequence();
		System.out.println("Count: " + sequence.size());
		return sequence;
		// System.out.println("Count: " + s.s.count);
		// return null;
	}

	@Override
	public Item evaluateToItem(QueryContext ctx, Tuple tuple)
			throws QueryException {
		return ExprUtil.asItem(evaluate(ctx, tuple));
	}

	@Override
	public boolean isUpdating() {
		// TODO
		return false;
	}

	@Override
	public boolean isVacuous() {
		return false;
	}

	public String toString() {
		return TopDownPipeExpr.class.getSimpleName();
	}
}
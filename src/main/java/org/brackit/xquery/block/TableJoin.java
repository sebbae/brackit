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
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.brackit.xquery.block;

import java.util.Arrays;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.atomic.Atomic;
import org.brackit.xquery.compiler.translator.Reference;
import org.brackit.xquery.util.Cmp;
import org.brackit.xquery.util.forkjoin.Task;
import org.brackit.xquery.util.join.FastList;
import org.brackit.xquery.util.join.MultiTypeJoinTable;
import org.brackit.xquery.xdm.Expr;
import org.brackit.xquery.xdm.Sequence;

/**
 * @author Sebastian Baechle
 * 
 */
public class TableJoin implements Block {
	final Block l;
	final Block r;
	final Block o;
	final Expr rExpr;
	final Expr lExpr;
	final boolean leftJoin;
	final Cmp cmp;
	final boolean isGCmp;
	final boolean skipSort;
	final int pad;
	int groupVar = -1;

	boolean ordRight = true;
	boolean ordLeft = true;
	int rPermits = FJControl.PERMITS;
	int lPermits = FJControl.PERMITS;

	public TableJoin(Cmp cmp, boolean isGCmsp, boolean leftJoin,
			boolean skipSort, Block l, Expr lExpr, Block r, Expr rExpr, Block o) {
		this.cmp = cmp;
		this.isGCmp = isGCmsp;
		this.leftJoin = leftJoin;
		this.skipSort = skipSort;
		this.l = l;
		this.r = r;
		this.o = o;
		this.rExpr = rExpr;
		this.lExpr = lExpr;
		this.pad = r.outputWidth(0) + ((o != null) ? o.outputWidth(0) : 0);
	}

	@Override
	public int outputWidth(int initSize) {
		return l.outputWidth(initSize) + pad;
	}

	public Reference group() {
		return new Reference() {
			public void setPos(int pos) {
				groupVar = pos;
			}
		};
	}

	@Override
	public Sink create(QueryContext ctx, Sink sink) throws QueryException {
		return new TableJoinSink(ctx, sink);
	}

	private final class TableJoinSink implements Sink {
		final QueryContext ctx;
		final Sink sink;
		MultiTypeJoinTable table;
		Atomic gk;

		public TableJoinSink(QueryContext ctx, Sink sink) {
			this.ctx = ctx;
			this.sink = sink;
		}

		@Override
		public Sink fork() {
			TableJoinSink f = new TableJoinSink(ctx, sink);
			f.table = table;
			f.gk = gk;
			return f;
		}

		@Override
		public void output(Tuple[] buf, int len) throws QueryException {			
			int end = 0;
			while (end < len) {
				int start = end;
				end = probeSize(buf, len, end);
				if (table == null) {
					// load table with first tuple in probe window			
					load(buf[start]);
				}
				probe(Arrays.copyOfRange(buf, start, end));
			}
		}

		private void probe(Tuple[] buf) throws QueryException {
			Sink probe = new Probe(sink, ctx, table);
			probe = (ordLeft) ? new SerialValve(lPermits, probe) : probe;
			Sink leftIn = l.create(ctx, probe);
			leftIn.begin();				
			leftIn.output(buf, buf.length);
			leftIn.end();
		}

		private void load(Tuple t) throws QueryException {
			int offset = t.getSize();					
			table = new MultiTypeJoinTable(cmp, isGCmp, skipSort);
			Sink load = new Load(ctx, table, offset);
			load = (ordRight) ? new SerialValve(rPermits, load) : load;
			Sink rightIn = r.create(ctx, load);
			Task task = new LoadTask(t, rightIn);
			task.fork();
			task.join();
		}

		private int probeSize(Tuple[] buf, int len, int end)
				throws QueryException {
			if (groupVar >= 0) {
				Atomic pgk = gk;
				gk = (Atomic) buf[end++].get(groupVar);
				if ((pgk != null) && (pgk.atomicCmp(gk) != 0)) {
					table = null; // we need to rebuild the new table
				}
				while (end < len) {
					Atomic ngk = (Atomic) buf[end].get(groupVar);
					if (ngk.atomicCmp(gk) != 0) {
						break;
					}
					end++;
				}
			} else {
				end = len + 1;
			}
			return end;
		}

		@Override
		public void begin() throws QueryException {
			sink.begin();
		}

		@Override
		public void end() throws QueryException {
			sink.end();
		}

		@Override
		public void fail() throws QueryException {
			sink.fail();
		}
	}

	private final class Probe extends ConcurrentSink {
		final QueryContext ctx;
		final Sink sink;
		final MultiTypeJoinTable table;
		final Sequence[] padding;

		Probe(Sink sink, QueryContext ctx, MultiTypeJoinTable table) {
			this.ctx = ctx;
			this.sink = sink;
			this.table = table;
			this.padding = new Sequence[pad];
		}

		@Override
		public void output(Tuple[] buf, int len) throws QueryException {
			for (int i = 0; i < len; i++) {
				Tuple tuple = buf[i];
				probe(tuple);
			}
		}

		private void probe(Tuple t) throws QueryException {
			Sequence keys = (isGCmp) ? lExpr.evaluate(ctx, t) : lExpr
					.evaluateToItem(ctx, t);
			FastList<Sequence[]> matches = table.probe(keys);
			int itSize = matches.getSize();
			if (itSize > 0) {
				Tuple[] buf = new Tuple[itSize];
				for (int i = 0; i < itSize; i++) {
					buf[i] = t.concat(matches.get(i));
				}
				if (o != null) {
					Sink opt = o.create(ctx, sink);
					opt.begin();
					opt.output(buf, itSize);
					opt.end();
				} else {
					sink.output(buf, itSize);
				}
			} else if (leftJoin) {
				Tuple[] buf = new Tuple[] { t.concat(padding) };
				sink.output(buf, 1);
			}
		}
	}

	private static final class LoadTask extends Task {
		private final Tuple t;
		private final Sink rightIn;

		private LoadTask(Tuple t, Sink rightIn) {
			this.t = t;
			this.rightIn = rightIn;
		}

		@Override
		public void compute() throws QueryException {
			rightIn.begin();
			try {
				rightIn.output(new Tuple[] { t }, 1);
				rightIn.end();
			} catch (QueryException e) {
				rightIn.fail();
				throw e;
			}
		}
	}

	private final class Load extends ConcurrentSink {
		final QueryContext ctx;
		final MultiTypeJoinTable table;
		final int offset;
		int pos = 1;

		Load(QueryContext ctx, MultiTypeJoinTable table, int offset) {
			this.ctx = ctx;
			this.table = table;
			this.offset = offset;
		}

		@Override
		public void output(Tuple[] buf, int len) throws QueryException {
			for (int i = 0; i < len; i++) {
				Tuple t = buf[i];
				Sequence keys = (isGCmp) ? rExpr.evaluate(ctx, t) : rExpr
						.evaluateToItem(ctx, t);
				if (keys != null) {
					Sequence[] tmp = t.array();
					Sequence[] bindings = Arrays.copyOfRange(tmp, offset,
							tmp.length);
					table.add(keys, bindings, pos++);
				}
			}
		}
	}
}

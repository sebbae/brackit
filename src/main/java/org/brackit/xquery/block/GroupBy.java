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

import java.util.concurrent.Semaphore;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.compiler.translator.Reference;
import org.brackit.xquery.util.aggregator.Grouping;

/**
 * @author Sebastian Baechle
 * 
 */
public class GroupBy implements Block {

	private class GroupBySink extends SerialSink {
		final Sink sink;
		final Grouping grp;

		public GroupBySink(int permits, Sink sink) {
			super(permits);
			this.sink = sink;
			this.grp = new Grouping(groupSpecs, onlyLast);
		}

		private GroupBySink(Semaphore sem, Sink sink, Grouping grp) {
			super(sem);
			this.sink = sink;
			this.grp = grp;
		}

		@Override
		protected ChainedSink doPartition(Sink stopAt) {
			Grouping grp = new Grouping(groupSpecs, onlyLast);
			return new GroupBySink(sem, sink.partition(stopAt), grp);
		}

		@Override
		protected SerialSink doFork() {
			return new GroupBySink(sem, sink, grp);
		}

		@Override
		protected void doOutput(Tuple[] buf, int len) throws QueryException {
			for (int i = 0; i < len; i++) {
				Tuple t = buf[i];
				if (!grp.add(t)) {
					outputGroup();
					grp.add(t);
				}
			}
		}

		private void outputGroup() throws QueryException {
			Tuple out = grp.emit();
			sink.output(new Tuple[] { out }, 1);
			grp.clear();
		}

		@Override
		protected void doFirstBegin() throws QueryException {
			sink.begin();
		}

		@Override
		protected void doFinalEnd() throws QueryException {
			if (grp.getSize() > 0) {
				outputGroup();
			}
			sink.end();
		}
	}

	final int[] groupSpecs; // positions of grouping variables
	final boolean onlyLast;

	public GroupBy(int groupSpecCount, boolean onlyLast) {
		this.groupSpecs = new int[groupSpecCount];
		this.onlyLast = onlyLast;
	}

	@Override
	public int outputWidth(int initSize) {
		return initSize;
	}

	public Sink create(QueryContext ctx, Sink sink) throws QueryException {
		return new GroupBySink(FJControl.PERMITS, sink);
	}

	public Reference group(final int groupSpecNo) {
		return new Reference() {
			public void setPos(int pos) {
				groupSpecs[groupSpecNo] = pos;
			}
		};
	}
}

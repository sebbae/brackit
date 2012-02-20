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
import org.brackit.xquery.atomic.Int32;
import org.brackit.xquery.atomic.IntNumeric;
import org.brackit.xquery.compiler.translator.Reference;
import org.brackit.xquery.xdm.Sequence;

/**
 * @author Sebastian Baechle
 * 
 */
public class Count implements Block {

	int check = -1;

	@Override
	public Sink create(QueryContext ctx, Sink sink)
			throws QueryException {

		return new CountSink(sink, check);
	}

	public Reference check() {
		return new Reference() {
			public void setPos(int pos) {
				check = pos;
			}
		};
	}

	static class CountSink extends SerialSink {
		final Sink sink;
		final int check;
		IntNumeric pos = Int32.ZERO;

		CountSink(Sink sink, int check) {
			super(FJControl.PERMITS);
			this.sink = sink;
			this.check = check;
		}

		CountSink(Semaphore sem, CB cb, SerialSink next, Sink sink,
				int check) {
			super(sem, cb, next);
			this.sink = sink;
			this.check = check;
		}

		@Override
		protected SerialSink doFork(Semaphore sem, CB cb, SerialSink next) {
			return new CountSink(sem, cb, next, sink, check);
		}

		@Override
		protected void doOutput(Tuple[] buf, int len) throws QueryException {
			Tuple[] out = new Tuple[len];
			for (int i = 0; i < len; i++) {
				Tuple t = buf[i];
				if ((check >= 0) && (t.get(check) == null)) {
					out[i] = t.concat((Sequence) null);
				} else {
					out[i] = t.concat(pos = pos.inc());
				}
			}
			sink.output(out, len);
		}

		@Override
		protected void doBegin() throws QueryException {
			sink.begin();
		}

		@Override
		protected void doEnd() throws QueryException {
			sink.end();
		}
	}
}

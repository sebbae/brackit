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

import org.brackit.xquery.ErrorCode;
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

	private static final boolean LOG = false;

	final Operator[] op;

	public FLW(Operator[] op) {
		this.op = op;
	}

	@Override
	public Sink create(QueryContext ctx, Sink sink) throws QueryException {
		return new FLWSink(ctx, sink);
	}

	static void log(String msg) {
		System.out.println(Thread.currentThread() + ": " + msg);
	}

	class FLWSink extends FJControl implements Sink {
		final Sink s;
		final QueryContext ctx;

		public FLWSink(QueryContext ctx, Sink s) {
			this.ctx = ctx;
			this.s = s;
		}

		@Override
		public void output(Tuple[] t, int len) throws QueryException {
			System.out.println("parallelim: " + POOL.getSize());
			Cursor c = op[0].create(ctx, t, len);
			c.open(ctx);
			Slice pslice = new Slice(s, c, 0);
			pslice.compute();
		}

		@Override
		public Sink fork() {
			return new FLWSink(ctx, s.fork());
		}

		@Override
		public void fail() {
			// TODO
		}

		@Override
		public void begin() {
		}

		@Override
		public void end() throws QueryException {
		}

		class Slice extends Task {
			final Sink s;
			final Cursor off;
			final int d;

			public Slice(Sink s, Cursor off, int d) {
				this.s = s;
				this.off = off;
				this.d = d;
			}

			@Override
			public void compute() throws QueryException {
				try {
					s.begin();
					bind(off, d);
					s.end();
				} catch (QueryException e) {
					s.fail();
					throw e;
				} catch (Throwable e) {
					s.fail();
					e.printStackTrace();
					throw new QueryException(e, ErrorCode.BIT_DYN_INT_ERROR);
				}
			}

			private void bind(Cursor c, int d) throws QueryException {
				Tuple[] buf = new Tuple[bufSize(d)];
				int len = fillBuffer(c, d, buf);
				if (len == 0) {
					return;
				}
				// process local share
				if (d + 1 < op.length) {
					descend(d, buf, len);
				} else {
					output(buf, len);
				}
			}

			private void output(Tuple[] buf, int len) throws QueryException {
				if (LOG)
					log("BEGIN OUTPUT [" + buf[0] + ", " + buf[len - 1]
							+ "] to " + s);
				s.output(buf, len);
				if (LOG)
					log("END OUTPUT [" + buf[0] + ", " + buf[len - 1] + "] to "
							+ s);
			}

			private void descend(int d, Tuple[] buf, int len)
					throws QueryException {
				if (LOG)
					log("BEGIN RECURSIVE [" + buf[0] + ", " + buf[len - 1]
							+ "]");
				Cursor c2 = op[d + 1].create(ctx, buf, len);
				c2.open(ctx);
				buf = null; // allow gc
				bind(c2, d + 1);
				if (LOG)
					log("END RECURSIVE [" + buf[0] + ", " + buf[len - 1] + "]");
			}

			private int fillBuffer(Cursor c, int d, Tuple[] buf)
					throws QueryException {
				int len = 0;
				// load local buffer and spawn if required
				// if (d == 0) {
				// System.out.println(Thread.currentThread()
				// + " START READING at level 0");
				// }
				while (true) {
					Tuple t = c.next(ctx);
					if (t != null) {
						// if (d == 0) {
						// System.out.println(Thread.currentThread()
						// + " READ NEXT at level 0");
						// }
						buf[len++] = t;
						if (len == buf.length) {
							Slice fork = new Slice(s.fork(), c, d);
							if (LOG)
								log("Forking " + fork.hashCode() + " at level "
										+ d + " after [" + buf[0] + ", "
										+ buf[len - 1] + "]");
							fork.fork();
							break;
						}
					} else {
						c.close(ctx);
						c = null; // allow garbage collection
						break;
					}
				}
				return len;
			}

			int bufSize(int d) {
				if (op[d] instanceof LetBind) {
					return (d == 0) ? 1 : bufSize(d - 1);
				}
				if (op[d] instanceof Select) {
					return bufSize(d - 1);
				}
				// TODO
				return (d < FORK_BUFFER.length) ? FORK_BUFFER[d]
						: FORK_BUFFER_DEFAULT;
			}

			public String toString() {
				return "Slice at level " + d + " -> " + s;
			}
		}
	}
}
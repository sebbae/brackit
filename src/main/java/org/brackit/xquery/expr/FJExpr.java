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
package org.brackit.xquery.expr;

import org.brackit.xquery.QueryContext;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.Tuple;
import org.brackit.xquery.block.FJControl;
import org.brackit.xquery.util.forkjoin.Task;
import org.brackit.xquery.xdm.Expr;
import org.brackit.xquery.xdm.Item;
import org.brackit.xquery.xdm.Sequence;

/**
 * @author Sebastian Baechle
 * 
 */
public class FJExpr implements Expr {

	final Expr expr;

	public FJExpr(Expr expr) {
		this.expr = expr;
	}

	@Override
	public Sequence evaluate(QueryContext ctx, Tuple t) throws QueryException {
		Eval task = new Eval(ctx, t);
		FJControl.POOL.submit(task);
		task.join();
		return task.result;
	}

	@Override
	public Item evaluateToItem(QueryContext ctx, Tuple t) throws QueryException {
		ItemEval task = new ItemEval(ctx, t);
		FJControl.POOL.submit(task);
		task.join();
		return task.result;
	}

	@Override
	public boolean isUpdating() {
		return expr.isUpdating();
	}

	@Override
	public boolean isVacuous() {
		return expr.isVacuous();
	}

	private final class Eval extends Task {
		private final QueryContext ctx;
		private final Tuple tuple;
		Sequence result;

		private Eval(QueryContext ctx, Tuple tuple) {
			this.ctx = ctx;
			this.tuple = tuple;
		}

		@Override
		public void compute() throws Throwable {
			result = expr.evaluate(ctx, tuple);
		}
	}

	private final class ItemEval extends Task {
		private final QueryContext ctx;
		private final Tuple tuple;
		Item result;

		private ItemEval(QueryContext ctx, Tuple tuple) {
			this.ctx = ctx;
			this.tuple = tuple;
		}

		@Override
		public void compute() throws Throwable {
			result = expr.evaluateToItem(ctx, tuple);
		}
	}
}

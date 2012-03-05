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
package org.brackit.xquery.compiler.translator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.brackit.xquery.ErrorCode;
import org.brackit.xquery.QueryException;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.block.Block;
import org.brackit.xquery.block.BlockChain;
import org.brackit.xquery.block.Count;
import org.brackit.xquery.block.FLW;
import org.brackit.xquery.block.GroupBy;
import org.brackit.xquery.block.OrderBy;
import org.brackit.xquery.block.TableJoin;
import org.brackit.xquery.compiler.AST;
import org.brackit.xquery.compiler.XQ;
import org.brackit.xquery.expr.BlockExpr;
import org.brackit.xquery.expr.FJExpr;
import org.brackit.xquery.module.Module;
import org.brackit.xquery.module.StaticContext;
import org.brackit.xquery.operator.ForBind;
import org.brackit.xquery.operator.LetBind;
import org.brackit.xquery.operator.Operator;
import org.brackit.xquery.operator.Select;
import org.brackit.xquery.operator.Start;
import org.brackit.xquery.util.Cmp;
import org.brackit.xquery.util.sort.Ordering.OrderModifier;
import org.brackit.xquery.xdm.Expr;
import org.brackit.xquery.xdm.type.SequenceType;

/**
 * 
 * @author Sebastian Baechle
 * 
 */
public class BlockTranslator extends Compiler {

	public BlockTranslator() {
		super();
	}

	@Override
	public Expr expression(Module module, StaticContext ctx, AST expr,
			boolean allowUpdate, boolean isBody) throws QueryException {
		Expr e = super.expression(module, ctx, expr, allowUpdate, isBody);
		return (isBody) ? new FJExpr(e) : e;
	}

	@Override
	protected Expr anyExpr(AST node) throws QueryException {
		if (node.getType() == XQ.PipeExpr) {
			// switch to bottom up compilation
			return pipeExpr(node);
		}
		return super.anyExpr(node);
	}

	protected Expr pipeExpr(AST node) throws QueryException {
		int initialBindSize = table.bound().length;
		Block block = block(node.getChild(0));

		// for simpler scoping, the return expression is
		// at the right-most leaf
		AST returnExpr = node.getChild(0);
		while (returnExpr.getType() != XQ.End) {
			returnExpr = returnExpr.getLastChild();
		}
		Expr expr = anyExpr(returnExpr.getChild(0));

		// clear operator bindings
		int unbind = table.bound().length - initialBindSize;
		for (int i = 0; i < unbind; i++) {
			table.unbind();
		}

		boolean ordered = ctx.isOrderingModeOrdered();

		return new BlockExpr(block, expr, ordered);
	}

	protected Block block(AST op) throws QueryException {
		List<Block> blocks = new ArrayList<Block>();
		anyBlock(op, blocks);
		return new BlockChain(blocks);
	}

	protected void anyBlock(AST node, List<Block> blocks) throws QueryException {
		switch (node.getType()) {
		case XQ.Start:
			// here we can add projection...
			anyBlock(node.getChild(0), blocks);
			break;
		case XQ.End:
			return; // stop
		case XQ.ForBind:
		case XQ.LetBind:
		case XQ.Selection:
			flwBlock(node, blocks);
			break;
		case XQ.OrderBy:
			orderByBlock(node, blocks);
			break;
		case XQ.Join:
			join(node, blocks);
			break;
		case XQ.GroupBy:
			groupByBlock(node, blocks);
			break;
		case XQ.Count:
			countBlock(node, blocks);
			break;
		default:
			throw new QueryException(ErrorCode.BIT_DYN_RT_ILLEGAL_STATE_ERROR,
					"Unexpected AST operator node '%s' of type: %s", node,
					node.getType());
		}
	}

	private void orderByBlock(AST node, List<Block> blocks)
			throws QueryException {
		int orderBySpecCount = node.getChildCount() - 1;
		Expr[] orderByExprs = new Expr[orderBySpecCount];
		OrderModifier[] orderBySpec = new OrderModifier[orderBySpecCount];
		for (int i = 0; i < orderBySpecCount; i++) {
			AST orderBy = node.getChild(i);
			orderByExprs[i] = expr(orderBy.getChild(0), true);
			orderBySpec[i] = orderModifier2(orderBy);
		}
		OrderBy orderByBlock = new OrderBy(orderByExprs, orderBySpec);
		blocks.add(orderByBlock);
		anyBlock(node.getLastChild(), blocks);
	}

	protected OrderModifier orderModifier2(AST orderBy) {
		boolean asc = true;
		boolean emptyLeast = true;
		String collation = null;
		for (int i = 1; i < orderBy.getChildCount(); i++) {
			AST modifier = orderBy.getChild(i);
			if (modifier.getType() == XQ.OrderByKind) {
				AST direction = modifier.getChild(0);
				asc = (direction.getType() == XQ.ASCENDING);
			} else if (modifier.getType() == XQ.OrderByEmptyMode) {
				AST empty = modifier.getChild(0);
				emptyLeast = (empty.getType() == XQ.LEAST);
			} else if (modifier.getType() == XQ.Collation) {
				collation = modifier.getChild(0).getStringValue();
			}
		}
		return new OrderModifier(asc, emptyLeast, collation);
	}

	protected void flwBlock(AST node, List<Block> blocks) throws QueryException {
		List<Operator> ops = new LinkedList<Operator>();
		while (true) {
			int type = node.getType();
			if (type == XQ.ForBind) {
				ops.add(forBind(node));
			} else if (type == XQ.LetBind) {
				ops.add(letBind(node));
			} else if (type == XQ.Selection) {
				ops.add(select(node));
			} else {
				break;
			}
			node = node.getLastChild();
		}
		FLW flwBlock = new FLW(ops.toArray(new Operator[ops.size()]));
		blocks.add(flwBlock);
		anyBlock(node, blocks);
	}

	protected Operator forBind(AST node) throws QueryException {
		int pos = 0;
		AST runVarDecl = node.getChild(pos++);
		QNm runVarName = (QNm) runVarDecl.getChild(0).getValue();
		SequenceType runVarType = SequenceType.ITEM_SEQUENCE;
		if (runVarDecl.getChildCount() == 2) {
			runVarType = sequenceType(runVarDecl.getChild(1));
		}
		AST posBindingOrSourceExpr = node.getChild(pos++);
		QNm posVarName = null;
		if (posBindingOrSourceExpr.getType() == XQ.TypedVariableBinding) {
			posVarName = (QNm) posBindingOrSourceExpr.getChild(0).getValue();
			posBindingOrSourceExpr = node.getChild(pos++);
		}
		Expr sourceExpr = expr(posBindingOrSourceExpr, true);

		Binding posBinding = null;
		Binding runVarBinding = table.bind(runVarName, runVarType);
		// Fake binding of run variable because set-oriented processing requires
		// the variable anyway
		table.resolve(runVarName);

		if (posVarName != null) {
			posBinding = table.bind(posVarName, SequenceType.INTEGER);
			// Fake binding of pos variable to simplify compilation.
			table.resolve(posVarName);
			// TODO Optimize and do not bind variable if not necessary
		}
		ForBind forBind = new ForBind(new Start(), sourceExpr, false);
		if (posBinding != null) {
			forBind.bindPosition(posBinding.isReferenced());
		}
		return forBind;
	}

	protected Operator letBind(AST node) throws QueryException {
		int pos = 0;
		AST letVarDecl = node.getChild(pos++);
		QNm letVarName = (QNm) letVarDecl.getChild(0).getValue();
		SequenceType letVarType = SequenceType.ITEM_SEQUENCE;
		if (letVarDecl.getChildCount() == 2) {
			letVarType = sequenceType(letVarDecl.getChild(1));
		}
		Expr sourceExpr = expr(node.getChild(pos++), true);
		Binding binding = table.bind(letVarName, letVarType);

		// Fake binding of let variable because set-oriented processing requires
		// the variable anyway
		table.resolve(letVarName);
		LetBind letBind = new LetBind(new Start(), sourceExpr);
		return letBind;
	}

	protected Operator select(AST node) throws QueryException {
		Expr expr = anyExpr(node.getChild(0));
		Select select = new Select(new Start(), expr);
		return select;
	}

	protected void groupByBlock(AST node, List<Block> blocks)
			throws QueryException {
		int groupSpecCount = Math.max(node.getChildCount() - 1, 0);
		boolean onlyLast = node.checkProperty("onlyLast");
		QNm[] grpVars = new QNm[groupSpecCount];
		GroupBy groupBy = new GroupBy(groupSpecCount, onlyLast);
		for (int i = 0; i < groupSpecCount; i++) {
			grpVars[i] = (QNm) node.getChild(i).getChild(0).getValue();
			table.resolve(grpVars[i], groupBy.group(i));
		}
		blocks.add(groupBy);
		anyBlock(node.getLastChild(), blocks);
	}

	protected void countBlock(AST node, List<Block> blocks)
			throws QueryException {
		AST posVarDecl = node.getChild(0);
		QNm posVarName = (QNm) posVarDecl.getChild(0).getValue();
		SequenceType posVarType = SequenceType.ITEM_SEQUENCE;
		if (posVarDecl.getChildCount() == 2) {
			posVarType = sequenceType(posVarDecl.getChild(1));
		}
		Binding binding = table.bind(posVarName, posVarType);

		// Fake binding of let variable because set-oriented processing requires
		// the variable anyway
		table.resolve(posVarName);
		Count count = new Count();
		blocks.add(count);
		anyBlock(node.getLastChild(), blocks);
	}

	protected void join(AST node, List<Block> blocks) throws QueryException {
		// get join type
		Cmp cmp = (Cmp) node.getProperty("cmp");
		boolean isGcmp = node.checkProperty("GCmp");

		int inBoundSize = table.bound().length;

		// compile right (inner) join branch
		List<Block> rb = new ArrayList<Block>();
		anyBlock(node.getChild(1), rb);
		Block r = new BlockChain(rb);
		AST tmp = node.getChild(1);
		while (tmp.getType() != XQ.End) {
			tmp = tmp.getLastChild();
		}
		Expr rightExpr = anyExpr(tmp.getChild(0));

		// unbind right
		Binding[] bound = table.bound();
		int rBoundSize = bound.length - inBoundSize;
		for (int i = 0; i < rBoundSize; i++) {
			table.unbind();
		}

		// compile left (outer) join branch
		List<Block> lb = new ArrayList<Block>();
		anyBlock(node.getChild(0), lb);
		Block l = new BlockChain(lb);
		tmp = node.getChild(0);
		while (tmp.getType() != XQ.End) {
			tmp = tmp.getLastChild();
		}
		Expr leftExpr = anyExpr(tmp.getChild(0));

		// re-bind right input and "resolve" if referenced
		for (int i = 0; i < rBoundSize; i++) {
			Binding binding = bound[inBoundSize + i];
			table.bind(binding.getName(), binding.getType());
			if (binding.isReferenced()) {
				table.resolve(binding.getName());
			}
		}

		Block o = null;
		AST post = node.getChild(2).getChild(0);
		if ((post.getType() != XQ.End)) {
			List<Block> ob = new ArrayList<Block>();
			anyBlock(node.getChild(2), ob);
			o = new BlockChain(ob);
		}

		boolean leftJoin = node.checkProperty("leftJoin");
		boolean skipSort = node.checkProperty("skipSort");
		TableJoin join = new TableJoin(cmp, isGcmp, leftJoin, skipSort, l,
				leftExpr, r, rightExpr, o);

		QNm prop = (QNm) node.getProperty("group");
		if (prop != null) {
			table.resolve(prop, join.group());
		}

		blocks.add(join);
		anyBlock(node.getLastChild(), blocks);
	}
}

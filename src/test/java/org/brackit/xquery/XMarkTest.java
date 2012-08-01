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
package org.brackit.xquery;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;

import org.brackit.xquery.node.d2linked.D2Node;
import org.brackit.xquery.node.d2linked.D2NodeBuilder;
import org.brackit.xquery.node.parser.DocumentParser;
import org.brackit.xquery.xdm.Collection;
import org.brackit.xquery.xdm.DocumentException;
import org.junit.Before;
import org.junit.Test;

/**
 * Test XMark queries against default context item
 * 
 * @author Sebastian Baechle
 * 
 */
public abstract class XMarkTest extends XQueryBaseTest {
	protected static final String QUERY_DIR = "/xmark/queries/orig/";

	protected static final String RESULT_DIR = "/xmark/results/";

	protected Collection<?> coll;

	@Test
	public void xmark01() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q01.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q01.out"), buffer.toString());
	}

	@Test
	public void xmark02() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q02.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q02.out"), buffer.toString());
	}

	@Test
	public void xmark03() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q03.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q03.out"), buffer.toString());
	}

	@Test
	public void xmark04() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q04.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q04.out"), buffer.toString());
	}

	@Test
	public void xmark05() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q05.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q05.out"), buffer.toString());
	}

	@Test
	public void xmark06() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q06.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q06.out"), buffer.toString());
	}

	@Test
	public void xmark07() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q07.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q07.out"), buffer.toString());
	}

	@Test
	public void xmark08() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q08.xq"));
		query.serialize(ctx, buffer);
		System.out.println(buffer);
		System.out.println(readFile(RESULT_DIR, "q08.out"));
		assertEquals(readFile(RESULT_DIR, "q08.out"), buffer.toString());
	}

	@Test
	public void xmark09() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q09.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q09.out"), buffer.toString());
	}

	@Test
	public void xmark10() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q10.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q10.out"), buffer.toString());
	}

	@Test
	public void xmark11() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q11.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q11.out"), buffer.toString());
	}

	@Test
	public void xmark12() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q12.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q12.out"), buffer.toString());
	}

	@Test
	public void xmark13() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q13.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q13.out"), buffer.toString());
	}

	@Test
	public void xmark14() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q14.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q14.out"), buffer.toString());
	}

	@Test
	public void xmark15() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q15.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q15.out"), buffer.toString());
	}

	@Test
	public void xmark16() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q16.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q16.out"), buffer.toString());
	}

	@Test
	public void xmark17() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q17.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q17.out"), buffer.toString());
	}

	@Test
	public void xmark18() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q18.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q18.out"), buffer.toString());
	}

	@Test
	public void xmark19() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q19.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q19.out"), buffer.toString());
	}

	@Test
	public void xmark20() throws Exception, IOException {
		ctx.setContextItem(coll.getDocument());
		PrintStream buffer = createBuffer();
		XQuery query = xquery(readQuery(QUERY_DIR, "q20.xq"));
		query.serialize(ctx, buffer);
		assertEquals(readFile(RESULT_DIR, "q20.out"), buffer.toString());
	}

	@Before
	public void setUp() throws Exception, FileNotFoundException {
		super.setUp();
		URL url = getClass().getResource("/xmark/auction.xml");
		DocumentParser parser = new DocumentParser(new File(url.getFile()));
		parser.setRetainWhitespace(true);
		coll = createDoc(parser);
	}

	protected Collection<?> createDoc(DocumentParser parser) throws DocumentException {
		D2NodeBuilder builder = new D2NodeBuilder();
		parser.parse(builder);
		D2Node subtreeRoot = builder.root();
		return subtreeRoot.getCollection();
	}
}

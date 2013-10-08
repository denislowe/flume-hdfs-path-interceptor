package com.cognitivematch.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.Interceptor.Builder;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;

public class TestHdfsPathInterceptor {

	private Builder fixtureBuilder;

	@Before
	public void init() throws Exception {
		fixtureBuilder = InterceptorBuilderFactory
				.newInstance("com.cognitivematch.flume.interceptor.HdfsPathInterceptor$Builder");
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldNotAllowConfigurationWithIllegalRegex() throws Exception {

		Context context = new Context();
		context.put(HdfsPathInterceptor.DELIMITER_NAME, "?&?&&&?&?&?&&&??");
		fixtureBuilder.configure(context);
		fixtureBuilder.build();
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldNotAllowConfigurationWithoutHeadersConfig() throws Exception {

		Context context = new Context();
		context.put(HdfsPathInterceptor.DELIMITER_NAME, ".*");
		context.put(HdfsPathInterceptor.HEADER_NAME, "");
		fixtureBuilder.configure(context);
		fixtureBuilder.build();
	}

	@Test
	public void shouldExtractAddHeadersForAllPositions() throws Exception {
		Context context = new Context();
		context.put(HdfsPathInterceptor.DELIMITER_NAME, "\t");
		context.put(HdfsPathInterceptor.HEADER_NAME, "0:headerA, 1:headerB, 2:headerC");
		fixtureBuilder.configure(context);
		Interceptor fixture = fixtureBuilder.build();

		Event event = EventBuilder.withBody("1	2	3.4foobar5", Charsets.UTF_8);
		Event expected = EventBuilder.withBody("1	2	3.4foobar5", Charsets.UTF_8);

		expected.getHeaders().put("headerA", "1");
		expected.getHeaders().put("headerB", "2");
		expected.getHeaders().put("headerC", "3.4foobar5");

		Event actual = fixture.intercept(event);

		Assert.assertArrayEquals(expected.getBody(), actual.getBody());
		Assert.assertEquals(expected.getHeaders(), actual.getHeaders());
	}

	@Test
	public void shouldExtractAddHeadersForAllPositionsNullDelimiter() throws Exception {

		Context context = new Context();
		context.put(HdfsPathInterceptor.DELIMITER_NAME, "\0");
		context.put(HdfsPathInterceptor.HEADER_NAME, "0:headerA, 1:headerB, 2:headerC");
		fixtureBuilder.configure(context);
		Interceptor fixture = fixtureBuilder.build();

		String delimString = "1" + '\000' + "2" + '\0' + "3.4foobar5";

		Event event = EventBuilder.withBody(delimString, Charsets.UTF_8);
		Event expected = EventBuilder.withBody(delimString, Charsets.UTF_8);

		expected.getHeaders().put("headerA", "1");
		expected.getHeaders().put("headerB", "2");
		expected.getHeaders().put("headerC", "3.4foobar5");

		Event actual = fixture.intercept(event);

		Assert.assertArrayEquals(expected.getBody(), actual.getBody());
		Assert.assertEquals(expected.getHeaders(), actual.getHeaders());
	}

	@Test
	public void shouldExtractAddHeadersForPartialPositions() throws Exception {
		Context context = new Context();
		context.put(HdfsPathInterceptor.DELIMITER_NAME, "\t");
		context.put(HdfsPathInterceptor.HEADER_NAME, "0:headerA, 10:headerB, 2:headerC");
		fixtureBuilder.configure(context);
		Interceptor fixture = fixtureBuilder.build();

		Event event = EventBuilder.withBody("1	2	3.4foobar5", Charsets.UTF_8);
		Event expected = EventBuilder.withBody("1	2	3.4foobar5", Charsets.UTF_8);

		expected.getHeaders().put("headerA", "1");
		expected.getHeaders().put("headerC", "3.4foobar5");
		Event actual = fixture.intercept(event);

		Assert.assertArrayEquals(expected.getBody(), actual.getBody());
		Assert.assertEquals(expected.getHeaders(), actual.getHeaders());
	}

	@Test
	public void shouldNotFailWithNoValidPositions() throws Exception {
		Context context = new Context();
		context.put(HdfsPathInterceptor.DELIMITER_NAME, "\t");
		context.put(HdfsPathInterceptor.HEADER_NAME, "20:headerA, 10:headerB, 30:headerC");
		fixtureBuilder.configure(context);
		Interceptor fixture = fixtureBuilder.build();

		Event event = EventBuilder.withBody("1	2	3.4foobar5", Charsets.UTF_8);
		Event expected = EventBuilder.withBody("1	2	3.4foobar5", Charsets.UTF_8);
		Event actual = fixture.intercept(event);

		Assert.assertArrayEquals(expected.getBody(), actual.getBody());
		Assert.assertEquals(expected.getHeaders(), actual.getHeaders());
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldFailWithInvalidHeaders() throws Exception {

		Context context = new Context();
		context.put(HdfsPathInterceptor.DELIMITER_NAME, "\t");
		context.put(HdfsPathInterceptor.HEADER_NAME, "20:headerA, 10headerB, 30:headerC");
		fixtureBuilder.configure(context);
		fixtureBuilder.build();
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldFailWithInvalidPositionNameNoComma() throws Exception {
		Context context = new Context();
		context.put(HdfsPathInterceptor.DELIMITER_NAME, "\t");
		context.put(HdfsPathInterceptor.HEADER_NAME, "20:headerA:10headerB, 30:headerC");
		fixtureBuilder.configure(context);
		fixtureBuilder.build();
	}
}

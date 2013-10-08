package com.cognitivematch.flume.interceptor;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Intercepter that performs a split operation on the event based on the
 * provided delimiter. The headers list determines what elements at specified
 * position to extract and add pass to the downstream agents.
 * <p>
 * Sample config:
 * <p>
 * <code>
 *   agent.sources.r1.interceptors = i1<p>
 *   agent.sources.r1.interceptors.i1.type = com.cognitivematch.flume.interceptor.HdfsPathInterceptor$Builder<p>
 *   agent.sources.r1.interceptors.i1.delimiter = :<p>
 *   agent.sources.r1.interceptors.i1.headers = 0:headerA, 2:headerB<p>
 * </code>
 * </p>
 * 
 * <pre>
 * Example:
 * </p>
 * EventBody: 1:2:3.4foobar5</p> Configuration:
 * <p>
 * <code>
 *   agent.sources.r1.interceptors = i1<p>
 *   agent.sources.r1.interceptors.i1.type = com.cognitivematch.flume.interceptor.HdfsPathInterceptor$Builder<p>
 *   agent.sources.r1.interceptors.i1.delimiter = :<p>
 *   agent.sources.r1.interceptors.i1.headers = 0:headerA, 2:headerB<p>
 * </code>
 * 
 * </p>
 * results in an event with the the following
 * 
 * body: 1:2:3.4foobar5 headers: headerA=>1, headerB=>3
 * 
 * The headerA and headerB headers from above can then be used by the sink for writing to HDFS
 * 
 */
public class HdfsPathInterceptor implements Interceptor {

	// Parameter names referenced from the flume configuration file
	protected static final String DELIMITER_NAME = "delimiter";
	protected static final String HEADER_NAME = "headers";
	protected static final String HEADER_NAME_SEPERATOR = ":";

	private static final Logger logger = LoggerFactory.getLogger(HdfsPathInterceptor.class);

	private final Pattern regex;
	private final List<HeaderIndex> headerIndex;

	private HdfsPathInterceptor(Pattern regex, List<HeaderIndex> headerIndex) {
		this.regex = regex;
		this.headerIndex = headerIndex;
	}

	/** {@inheritDoc} */
	@Override
	public void initialize() {
		// NO-OP...
	}

	/** {@inheritDoc} */
	@Override
	public void close() {
		// NO-OP...
	}

	/** {@inheritDoc} */
	@Override
	public Event intercept(Event event) {

		// TODO: Save space by either splitting up to max header position or iterating the event body.
		String[] data = regex.split(new String(event.getBody()));
		
		for (HeaderIndex hp : headerIndex) {
			// Add new headers to the event
			if (hp.index <= data.length) {
				event.getHeaders().put(hp.header, data[hp.index]);
			} else {
				// either miss configuration or dirty/invalid data, either way
				// log the error and continue
				logger.error(hp.header + " not set. No attribute found at position: " + hp.index + " with delimiter: '"
						+ regex.pattern() + "'");
			}
		}

		return event;
	}

	/** {@inheritDoc} */
	@Override
	public List<Event> intercept(List<Event> events) {
		List<Event> eventList = Lists.newArrayListWithCapacity(events.size());
		for (Event event : events) {
			eventList.add(intercept(event));
		}
		return eventList;
	}

	public static class Builder implements Interceptor.Builder {

		private Pattern regex;
		private List<HeaderIndex> headerIndexList;

		/**
		 * Configure and validate the interceptor using the configuration properties.
		 */
		@Override
		public void configure(Context context) {
			// Delimiter is mandatory
			String delim = context.getString(DELIMITER_NAME);
			Preconditions.checkArgument(!StringUtils.isEmpty(delim), "Must supply a valid delimiter");
			regex = Pattern.compile(delim);

			// index and header is mandatory
			String headerConfig = context.getString(HEADER_NAME);
			Preconditions.checkArgument(!StringUtils.isEmpty(headerConfig),
					"Must supply at least one 'headers' parameter in the form: pos " + HEADER_NAME_SEPERATOR
							+ " headername");

			String[] headers = headerConfig.split(",");
			headerIndexList = Lists.newArrayListWithCapacity(headers.length);
			for (String header : headers) {
				String[] indexAndHeader = header.split(HEADER_NAME_SEPERATOR);

				Preconditions.checkArgument(indexAndHeader.length == 2, "invalid position and name provided: "
						+ header.trim());
				Preconditions
						.checkArgument(!(StringUtils.isEmpty(indexAndHeader[0].trim()) && StringUtils
								.isNumeric(indexAndHeader[0].trim())), "invalid position provided: " + header
								+ " -> " + indexAndHeader[0].trim());
				Preconditions.checkArgument(!StringUtils.isEmpty(indexAndHeader[1].trim()), "invalid name provided: "
						+ header + " -> " + indexAndHeader[1].trim());
				headerIndexList.add(new HeaderIndex(header));
			}

			logger.info(String.format("Configuring RegexExtractorInterceptor. Delimiter: '" + regex + "'"
					+ ". Postions and Names: " + headerConfig));
		}

		@Override
		public Interceptor build() {
			return new HdfsPathInterceptor(regex, headerIndexList);
		}
	}
	
	/**
	 * DTO for storing the index and header
	 *
	 */
	protected static class HeaderIndex {
		private final String header;
		private final int index;

		public HeaderIndex(String headerIndex) {
			String[] headerAndPos = headerIndex.split(HEADER_NAME_SEPERATOR, 2);
			this.index = Integer.parseInt(headerAndPos[0].trim());
			this.header = headerAndPos[1].trim();
		}
	}
}

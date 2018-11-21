package org.springframework.data.r2dbc.function;

/**
 * Interface declaring a query operation that can be represented with a query string. This interface is typically
 * implemented by classes representing a SQL operation such as {@code SELECT}, {@code INSERT}, and such.
 *
 * @author Mark Paluch
 */
public interface QueryOperation {

	/**
	 * Returns the string-representation of this operation to be used with {@link io.r2dbc.spi.Statement} creation.
	 *
	 * @return the operation as SQL string.
	 * @see io.r2dbc.spi.Connection#createStatement(String)
	 */
	String toQuery();
}

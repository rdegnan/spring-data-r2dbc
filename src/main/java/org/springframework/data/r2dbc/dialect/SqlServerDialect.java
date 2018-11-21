package org.springframework.data.r2dbc.dialect;

/**
 * An SQL dialect for Microsoft SQL Server.
 *
 * @author Mark Paluch
 */
public class SqlServerDialect implements Dialect {

	/**
	 * Singleton instance.
	 */
	public static final SqlServerDialect INSTANCE = new SqlServerDialect();

	private static final BindMarkersFactory NAMED = BindMarkersFactory.named("@", "P", 32,
			SqlServerDialect::filterBindMarker);

	@Override
	public BindMarkersFactory getBindMarkersFactory() {
		return NAMED;
	}

	@Override
	public String returnGeneratedKeys() {
		return "select SCOPE_IDENTITY() AS GENERATED_KEYS";
	}

	private static String filterBindMarker(CharSequence input) {

		StringBuilder builder = new StringBuilder();

		for (int i = 0; i < input.length(); i++) {

			char ch = input.charAt(i);

			// ascii letter or digit
			if (Character.isLetterOrDigit(ch) && ch < 127) {
				builder.append(ch);
			}
		}

		if (builder.length() == 0) {
			return "";
		}

		return "_" + builder.toString();
	}
}

package org.springframework.data.r2dbc.function;

import org.springframework.data.r2dbc.testing.PostgresTestSupport;

/**
 * Integration tests for {@link TransactionalDatabaseClient} against PostgreSQL.
 *
 * @author Mark Paluch
 */
public class PostgresTransactionalDatabaseClientIntegrationTests
		extends AbstractTransactionalDatabaseClientIntegrationTests {

	@Override
	protected String getCreateTableStatement() {
		return PostgresTestSupport.CREATE_TABLE_LEGOSET;
	}

	@Override
	protected String getInsertIntoLegosetStatement() {
		return PostgresTestSupport.INSERT_INTO_LEGOSET;
	}

	@Override
	protected String getCurrentTransactionIdStatement() {
		return "SELECT txid_current();";
	}
}

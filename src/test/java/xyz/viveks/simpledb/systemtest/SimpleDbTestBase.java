package xyz.viveks.simpledb.systemtest;

import org.junit.Before;

import org.junit.Ignore;
import xyz.viveks.simpledb.Database;

/**
 * Base class for all SimpleDb test classes. 
 * @author nizam
 *
 */
@Ignore
public class SimpleDbTestBase {
	/**
	 * Reset the database before each test is run.
	 */
	@Before	public void setUp() throws Exception {					
		Database.reset();
	}
	
}

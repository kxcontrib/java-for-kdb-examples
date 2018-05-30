package kx.examples;

import java.io.IOException;

import kx.c;
import kx.c.KException;

/**
 * Example methods for creating objects representative of q types, and
 * subsequently sending them to a remote kdb+ instance.
 * @author plyness
 *
 */
public class CreateAndSendExamples {

	private c qConnection;

	public CreateAndSendExamples() throws KException, IOException {
		// Localhost, port 10000
		qConnection = QConnectionFactory.getDefault().getQConnection();
	}

	public static void main(String[] args) {
		try {
			new CreateAndSendExamples().start();
		} catch (KException | IOException e) {
			System.err.println("Error occurred in execution - Details:");
			e.printStackTrace();
		}

	}

	private void start() throws KException, IOException {

		createSimpleList();
		createMixedList();
		createDictionary();
	    createTable();
		createGUID();
	}

	private void createTable() throws IOException {
		// Create rows and columns
		int[] values = { 1, 2, 3 };
		Object[] data = new Object[] { values };
		String[] columnNames = new String[] { "column" };
		// Wrap values in dictionary
		c.Dict dict = new c.Dict(columnNames, data);
		// Create table using dict
		c.Flip table = new c.Flip(dict);
		// Send to q using 'insert' method
		qConnection.ks("insert", "t1", table);

	}

	private void createDictionary() throws KException, IOException {
		// Create keys and values
		Object[] keys = { "a", "b", "c" };
		int[] values = { 100, 200, 300 };
		// Set in dict constructor
		c.Dict dict = new c.Dict(keys, values);
		// Set in q session
		qConnection.k("set", "dict", dict);

	}

	private void createMixedList() throws KException, IOException {
		// Create generic Object array.
		Object[] mixedList = { new String[] { "first", "second" },
				new double[] { 1.0, 2.0 } };
		// Pass to q in the same way as a simple list.
		qConnection.k("set", "mixedList", mixedList);

	}

	private void createSimpleList() throws KException, IOException {
		// Create typed array
		int[] simpleList = { 10, 20, 30 };
		// Pass array to q using set function.
		qConnection.k("set", "simpleList", simpleList);

	}

	private void createGUID() throws KException, IOException {
		// Generate random UUID object
		java.util.UUID uuid = java.util.UUID.randomUUID();
		//System.out.println(uuid.toString());
		// Pass object to q using set function
		qConnection.k("set", "randomGUID", uuid);
		//System.out.println(qConnection.k("randomGUID").toString());
	}

}

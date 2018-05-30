package kx.examples;

import java.io.IOException;

import kx.c;
import kx.c.KException;

/**
 * Example methods demonstrating the steps taken to subscribe to and receieve tick data from a kdb+ tickerplant instance.
 * @author plyness
 *
 */
public class TickSubscriberExample {

	private c qConnection;

	public TickSubscriberExample() throws KException, IOException {
		// Localhost, port 10000
		qConnection = QConnectionFactory.getDefault().getQConnection();
	}

	public static void main(String[] args) {
		try {
			new TickSubscriberExample().start();
		} catch (KException | IOException e) {
			System.err.println("Error occured in execution - Details:");
			e.printStackTrace();
		}

	}

	private void start() throws KException, IOException {
		getSchemaForOneTable();
		getSchemaForMultipleTables();
		subscribeAndGetData();
		qConnection.close();
	}

	private void subscribeAndGetData() throws KException, IOException {
		//Subscribe to the trade table
		qConnection.k(".u.sub[`trade;`]");
		
		//Start listening loop
		while (true) {
			//wait on k()
			Object response = qConnection.k();
			
			if(response != null) {
				Object[] data = (Object[]) response;
				//Slightly different.. table is in data[2]!
				c.Flip table = (c.Flip) data[2];
				//Get column names and data
                String[] columnNames = table.x;
                Object[] columnData = table.y;
                //Get row count for looping
                int rowCount = c.n(columnData[0]);
                //Print out the table!
                System.out.printf("%s\t\t\t%s\t%s\t%s\n", columnNames[0], columnNames[1], columnNames[2], columnNames[3]);
                System.out.println("--------------------------------------------");
                for (int i = 0; i < rowCount; i++)
                {
                	//Printing logic
                    float value = Float.parseFloat(c.at(columnData[2],i).toString());
                    System.out.printf("%s\t%s\t%3.2f\t%s\n", c.at(columnData[0], i).toString(),
                                                             c.at(columnData[1], i).toString(),
                                                             value,
                                                             c.at(columnData[3], i).toString());
                }

			}
		}
		
	}

	private void getSchemaForOneTable() throws KException, IOException {
		// Run sub function and store result
		Object[] response = (Object[]) qConnection.k(".u.sub[`trade;`]");
		// first index is table name
		System.out.println("table name: " + response[0]);
		// second index is flip object
		c.Flip table = (c.Flip) response[1];
		// Retreive column names
		String[] columnNames = table.x;
		for (int i = 0; i < columnNames.length; i++) {
			System.out.printf("Column %d is named %s\n", i, columnNames[i]);
		}

	}

	private void getSchemaForMultipleTables() throws KException, IOException {
		// Run sub function and store result
		Object[] response = (Object[]) qConnection.k(".u.sub[`;`]");
		// iterate through Object array
		for (Object tableObjectElement : response) {
			// From here, it is similar to the one-table schema extraction
			Object[] tableData = (Object[]) tableObjectElement;
			System.out.println("table name: " + tableData[0]);
			c.Flip table = (c.Flip) tableData[1];
			String[] columnNames = table.x;
			for (int i = 0; i < columnNames.length; i++) {
				System.out.printf("Column %d is named %s\n", i, columnNames[i]);
			}

		}

	}

}

package kx.examples;

import java.io.IOException;

import kx.c;
import kx.c.KException;

/**
 * Example methods demonstrating the steps taken to publish to a kdb+ tickerplant instance.
 * @author plyness
 *
 */
public class TickPublisherExample {

	private c qConnection;
	
	public TickPublisherExample() throws KException, IOException {
		// Localhost, port 10000
		qConnection = QConnectionFactory.getDefault().getQConnection();
	}

	public static void main(String[] args) {
		try {
			new TickPublisherExample().start();
		} catch (KException | IOException e) {
			System.err.println("Error occured in execution - Details:");
			e.printStackTrace();
		}

	}

	private void start() throws IOException {
		publishOneRow();
		publishMultipleRows();
		publishOneRowWithTimespanColumn();
		qConnection.close();
	}

	private void publishOneRow() throws IOException {        
		//Create typed arrays for holding data
        String[] sym = new String[] {"IBM"};
        double[] bid = new double[] {100.25};
        double[] ask = new double[] {100.26};
        int[] bSize = new int[]{1000};
        int[] aSize = new int[]{1000};
        //Create Object[] for holding typed arrays
        Object[] data = new Object[] {sym, bid, ask, bSize, aSize};
        //Call .u.upd asynchronously
        qConnection.ks(".u.upd", "quote", data);
	}

	private void publishMultipleRows() throws IOException {
		//Create typed arrays for holding data
		String[] sym = new String[] {"IBM", "GE"};
        double[] bid = new double[] {100.25, 120.25};
        double[] ask = new double[] {100.26, 120.26};
        int[] bSize = new int[]{1000, 2000};
        int[] aSize = new int[]{1000, 2000};
        //Create Object[] for holding typed arrays
        Object[] data = new Object[] {sym, bid, ask, bSize, aSize};
        //Call .u.upd asynchronously
        qConnection.ks(".u.upd", "quote", data);
		
	}

	private void publishOneRowWithTimespanColumn() throws IOException {
		//Timespan can be added here
		c.Timespan[] time = new c.Timespan[] {new c.Timespan()};
        String[] sym = new String[] {"GS"};
        double[] bid = new double[] {100.25};
        double[] ask = new double[] {100.26};
        int[] bSize = new int[]{1000};
        int[] aSize = new int[]{1000};
        //Timespan array is then added at beginning of Object array
        Object[] data = new Object[] {time, sym, bid, ask, bSize, aSize};
        qConnection.ks(".u.upd", "quote", data);

	}

}

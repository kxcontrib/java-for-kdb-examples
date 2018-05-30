package kx.examples;

import java.io.IOException;
import java.util.Arrays;

import kx.c;
import kx.c.KException;

/**
 * Example methods for manipulating data retrieved from a remote kdb+ instance
 * @author plyness
 *
 */
public class ExtractionExamples {

	private c qConnection;
	
	public ExtractionExamples() throws KException, IOException {
		//Localhost, port 10000
		qConnection = QConnectionFactory.getDefault().getQConnection();
	}
	
	public static void main(String[] args) {
		
		
		try {
			new ExtractionExamples().start();
		} catch (KException | IOException e) {
			System.err.println("Error occured in execution - Details:");
			e.printStackTrace();
		}
		
		
	}

	private void start() throws KException, IOException {
		extractAtoms();
		extractLists();
		workingWithDictionaries();
		workingWithTables();
		workingWithGUIDs();
		qConnection.close();
	}

	private void workingWithTables() throws KException, IOException {
		// (try to load trade.q first for this to work..)
		qConnection.ks("system \"l trade.q\"");
		
		//Retrieve table
		c.Flip flip = (c.Flip) qConnection.k("select from trade where sym = `a");
		
		//Retrieve columns and data
		String[] columnNames = flip.x;
		Object[] columnData = flip.y;
		//Extract row data into typed arrays
		java.sql.Time[] time = (java.sql.Time[]) columnData[0];
		String[] sym = (String[]) columnData[1];
		double[] price = (double[]) columnData[2];
		int[] size = (int[]) columnData[3];
		int rows = time.length;
		
		//Actually print the table now - columns first:
        for (String columnName : columnNames)
        {
            System.out.print(columnName + "\t\t\t");
        }
        
        System.out.println("\n----------------------------------------------------------------------------");
      
        //Then rows:
        for (int i = 0; i < rows; i++)
        {
            System.out.print(time[i] + "\t" + sym[i] + "\t\t\t" + price[i] + "\t\t\t" + size[i] + "\n");
        }
    }


        
	

	private void workingWithDictionaries() throws KException, IOException {
		//Retrieve Dictionary
		c.Dict dict = (c.Dict) qConnection.k("`a`b`c!((1 2 3);\"Second\"; (`x`y`z))");
		
		//Retrieve keys from dictionary
		String[] keys = (String[]) dict.x;
		System.out.println(Arrays.toString(keys));
		
		//Retrieve values
		Object[] values = (Object[]) dict.y;
		
		//These can then be worked with similarly to nested lists
		long[] valuesLong = (long[]) values[0];
		System.out.println(Arrays.toString(valuesLong));
	}
	
	private void workingWithGUIDs() throws KException, IOException {
		//Single GUID extraction
		Object result = qConnection.k("rand 0Ng");
		java.util.UUID castResult = (java.util.UUID) result;
		System.out.println("Single GUID: "+castResult.toString());
		
		//Multiple GUID extraction
		Object resultArray = qConnection.k("2?0Ng") ;
		java.util.UUID[] castResultArray = (java.util.UUID[]) resultArray;
		System.out.print("Multiple GUIDs: ");
		
		for(java.util.UUID guid: castResultArray) {
			System.out.print(guid.toString()+" ");
		}
		System.out.println();
	}

	private void extractLists() throws KException, IOException {
		// Start by casting the returned Object into Object[]
		Object[] resultArray = (Object[]) qConnection.k("((1 2 3 4); (1 2))");
		//Iterate through the Object array
		for (Object resultElement : resultArray) {
			//Retrieve each list and cast to appropriate type
			long[] elementArray = (long[]) resultElement;
			//Iterate through these arrays to access values.
			for(long elementAtom : elementArray) {
				System.out.println(elementAtom);
			}
			
		}
		
	}

	private void extractAtoms() throws KException, IOException {
		//Get a list from the q session
		Object result = qConnection.k("(1 2 3 4)");
		//Cast the returned Object into long[], and retrieve the desired result.
		long[] castList = ((long[]) result);
		long extractedAtom = castList[0];
		System.out.println(extractedAtom);
	}

}

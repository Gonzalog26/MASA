package csvUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;


public class CsvUtils {
	
	static String cvsSplitBy = ";";

	public static String[] getMainRowFromCsv(String path){
		
		String line = "";
        String[] mainRow = null;

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {

        	if((line = br.readLine())!=null)
        		mainRow = line.split(cvsSplitBy);

        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return mainRow;
		
	}
	
	public static ArrayList<String[]> getDataRowsFromCsv(String path) {

        String line = "";
        String[] columns = null;
        ArrayList<String[]> rows = new ArrayList<String[]>();

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {

        	int n=0;
            while ((line = br.readLine()) != null) {
                if(n!=0){
                	columns = line.split(cvsSplitBy);
                    rows.add(columns);
                    columns = null;
                    n++;
                }else
                	n++;
            }
            
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return rows;

    }
	
	public static void writeRowToCsv(String path, String[] columns){
		
		String row = "";
		
		for(int columnIndex=0;columnIndex<columns.length;columnIndex++)
			row+=columns[columnIndex]+";";
		
		row+="\n";

		FileWriter fw;
		try {
			fw = new FileWriter(new File(path), true);
			fw.write(row);
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}	

	}
	
}

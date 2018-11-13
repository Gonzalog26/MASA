package csvAgregator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import csvUtils.CsvUtils;

public class CsvAgregator {
	
	static String folderPath = "/home/gonzalo/Documents/MASA/Converter/CsvFiles";
	static String agregatedFolderPath = "/home/gonzalo/Documents/MASA/Converter/CsvAggregatedFiles";
	static int numberOfRowsPerAggregation = 1000;
	
	public CsvAgregator(){
		
	}
	
	public static void main(String[] args)  {

		long initialTime = System.currentTimeMillis();
		
		try {
			
			ProcessBuilder p = new ProcessBuilder();
	        System.out.println("Started EXE");
	        p.command("//home/gonzalo/Documents/MASA/Converter/DpfToCsvGeneric", "//home/gonzalo/Documents/MASA/Converter/BinaryFiles/",
	        		"//home/gonzalo/Documents/MASA/Converter/CsvFiles/");   

	        p.start();
			
			System.out.println("Command executed");
			
			File[] files = new File(folderPath).listFiles();
			
			for(int n=0;n<files.length;n++){
				if(files[n].getName().substring(files[n].getName().length()-4, files[n].getName().length()).equals(".csv")){
					
					String[] mainRow = CsvUtils.getMainRowFromCsv(folderPath+"/"+files[n].getName());		
			        ArrayList<String[]> InitialDatarows = CsvUtils.getDataRowsFromCsv(folderPath+"/"+files[n].getName());
			        
			        CsvUtils.writeRowToCsv(agregatedFolderPath+"/(Agregated)"+files[n].getName(), mainRow);
			        
			        for(int aggregatedRowIndex=0;aggregatedRowIndex<(InitialDatarows.size()/numberOfRowsPerAggregation);aggregatedRowIndex++){
			
			            String[] AggregatedColumns = new String[37];
			            
			            for(int columnIndex=0;columnIndex<8;columnIndex++)
			            	AggregatedColumns[columnIndex] = InitialDatarows.get(0)[columnIndex];
			            
			            AggregatedColumns[8] = InitialDatarows.get(aggregatedRowIndex*numberOfRowsPerAggregation)[8];
			            
			            for(int columnIndex=9;columnIndex<33;columnIndex++){
			            	
			            	int aggregate = 0;
			            	
			            	for(int rowIndex=(aggregatedRowIndex*numberOfRowsPerAggregation);rowIndex<(aggregatedRowIndex*numberOfRowsPerAggregation)+numberOfRowsPerAggregation;rowIndex++)
			            		aggregate+=Integer.parseInt(InitialDatarows.get(rowIndex)[columnIndex]);
			
			            	AggregatedColumns[columnIndex] = Integer.toString(aggregate);        	
			            }
			            
			            for(int columnIndex=33;columnIndex<37;columnIndex++)
			            	AggregatedColumns[columnIndex] = "#nv";
			            	        
			            CsvUtils.writeRowToCsv(agregatedFolderPath+"/(Agregated)"+files[n].getName(), AggregatedColumns);			            
			        }		
			        
				}
			}			
			
			System.out.println(System.currentTimeMillis()-initialTime);
    	
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        //System.out.println(Agregatedrows.get(1)[9]);

    }

}

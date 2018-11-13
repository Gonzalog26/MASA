/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.tekniker.processors.MASA;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import csvUtils.CsvUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

@Tags({"csv","aggregator"})
@CapabilityDescription("Aggregate csvs")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class CsvAggregator extends AbstractProcessor {
	
	private static final Log log = LogFactory.getLog(CsvAggregator.class);
	
	//static String folderPath = "/home/gonzalo/Documents/MASA/Converter/CsvFiles";
	//static String agregatedFolderPath = "/home/gonzalo/Documents/MASA/Converter/CsvAggregatedFiles";
	static String folderPath;
	static String agregatedFolderPath;
	static int numberOfRowsPerAggregation = 1000;

    public static final PropertyDescriptor CsvsFolderPath = new PropertyDescriptor
            .Builder().name("Csvs Folder Path")
            .description("Path where Csvs are saved after the conversion")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor AggregatedCsvsFolderPath = new PropertyDescriptor
            .Builder().name("Aggregated Csvs Folder Path")
            .description("Path where Aggregated Csvs will be saved")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(CsvsFolderPath);
        descriptors.add(AggregatedCsvsFolderPath);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        this.relationships = Collections.unmodifiableSet(relationships);

    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
       
    	log.info("Starting...");
    	folderPath = context.getProperty(CsvsFolderPath).evaluateAttributeExpressions().getValue();
    	agregatedFolderPath = context.getProperty(AggregatedCsvsFolderPath).evaluateAttributeExpressions().getValue();
    	
    	log.info("Csvs Folder Path: "+folderPath);
    	log.info("Aggregated Csvs folder path: "+agregatedFolderPath);
  
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
		            
		            if(files[n].delete()){
		    			System.out.println(files[n].getName() + " is deleted!");
		    		}else{
		    			System.out.println("Delete operation is failed.");
		    		}
		        }		
		        
			}
		}			
		
		log.info("Finished...");      
    	
    }
}

/**
 * Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * This file is licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License. A copy of
 * the License is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
*/


package com.amazonaws.ddb.DDB_Milestone2;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.firehose.FirehoseClient;

import java.nio.charset.Charset;

public class StreamsRecordProcessor implements IRecordProcessor {
    private Integer checkpointCounter;

    private final AmazonDynamoDB dynamoDBClient;
    private final String tableName;
    private final Region region = Region.AP_SOUTHEAST_2;
    private final String FireshoseName="Gdelt_event_data";
    
    FirehoseClient kinesisFireshoseClient = FirehoseClient.builder()
            .region(region)
            .build();

    public StreamsRecordProcessor(AmazonDynamoDB dynamoDBClient2, String tableName) {
        this.dynamoDBClient = dynamoDBClient2;
        this.tableName = tableName;
    }

    @Override
    public void initialize(InitializationInput initializationInput) {
        checkpointCounter = 0;
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        for (Record record : processRecordsInput.getRecords()) {
            String data = new String(record.getData().array(), Charset.forName("UTF-8"));
            System.out.println(data);
           
            if (record instanceof RecordAdapter) {
                com.amazonaws.services.dynamodbv2.model.Record streamRecord = ((RecordAdapter) record)
                        .getInternalObject();
                System.out.println("Before  getEventName...");

				switch (streamRecord.getEventName()) {
				case "INSERT":
	                System.out.println("Inside  getEventName Insert...");
					/*
					 * StreamsAdapterDemoHelper.putItem(dynamoDBClient, tableName,
					 * streamRecord.getDynamodb().getNewImage());
					 */
	                String textValue = "Poonam";
	                PutRecord.putSingleRecord(kinesisFireshoseClient,textValue, FireshoseName);
	                
					break;
				case "MODIFY":
	                System.out.println("Inside  getEventName Modify...");
					/*
					 * StreamsAdapterDemoHelper.putItem(dynamoDBClient, tableName,
					 * streamRecord.getDynamodb().getNewImage());
					 */
	                String textValue1 = "sandeep";
	                PutRecord.putSingleRecord(kinesisFireshoseClient,textValue1, FireshoseName);
	                
					break;
				case "REMOVE":
	                System.out.println("Inside  getEventName Remove...");
					StreamsAdapterDemoHelper.deleteItem(dynamoDBClient, tableName,
							streamRecord.getDynamodb().getKeys().get("EventId").getN());
				}
            }
            checkpointCounter += 1;
            if (checkpointCounter % 10 == 0) {
                try {
                    processRecordsInput.getCheckpointer().checkpoint();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
            try {
                shutdownInput.getCheckpointer().checkpoint();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        
    }    
}


package com.datastax.widerow;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.mortbay.log.Log;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

public class Main {

	private Session session;
	private static String keyspaceName = "datastax_widerow_demo";
	private static String tableName = keyspaceName + ".followers";

	private static final String INSERT_INTO_FOLLOWERS = "Insert into " + tableName
			+ " (id, follower_id, follower_name) values (?,?,?);";
	private static final String SELECT_FROM_FOLLOWERS = "select * from " + tableName + " where id = ?";

	private PreparedStatement insertStmt;
	private PreparedStatement queryStmt;

	public Main() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		
		Cluster cluster = Cluster.builder().addContactPoints(contactPointsStr.split(",")).build();
		this.session = cluster.connect();

		insertStmt = session.prepare(INSERT_INTO_FOLLOWERS);
		queryStmt = session.prepare(SELECT_FROM_FOLLOWERS);

		String noOfRowsStr = PropertyHelper.getProperty("noOfRows", "500");
		String noOfColsStr = PropertyHelper.getProperty("noOfCols", "100");

		System.out.println("Cluster and Session created.");

		int rowSize = Integer.parseInt(noOfRowsStr);
		int colSize = Integer.parseInt(noOfColsStr);
				
		this.insertWideRowsAsync(rowSize, colSize);
		this.insertWideRowsBatch(rowSize, colSize);		

		System.out.println("Wide row test finished.");

		cluster.shutdown();
	}
	
	private void insertWideRowsAsync(int rowSize, int colSize) {
		BoundStatement boundStmt = new BoundStatement(insertStmt);
		List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();

		int count=0;
		long start = System.currentTimeMillis();
		
		for (int i = 0; i < rowSize; i++) {
			for (int j = 1; j < (colSize+1); j++) {

				boundStmt.bind("id-" + (i+1), UUID.randomUUID(), "Name " + (j+1));
				results.add(session.executeAsync(boundStmt));
				
				if (j % 10000 == 0 && j > 0){
					Log.info("Inserted " + j + " cols");
				}
			}
			
			count++;
			
			if (count % 100 == 0){
				Log.info("Inserted " + count + " rows");
			}
		}
		
		boolean wait = true;
		while(wait){			
			//start with getting out, if any results are not done, wait is true.
			wait = false;			
			for (ResultSetFuture result : results){				
				if (!result.isDone()){
					wait = true;
					break;
				}			
			}
		}
		
		Log.info("Inserted (Async) " + colSize + " columns for " + rowSize + " rows in " + (System.currentTimeMillis() - start) + "ms");
	}

	private void insertWideRowsBatch(int rowSize, int colSize) {
				
		int count=0;
		long start = System.currentTimeMillis();
		
		if (colSize > 1500){
			Log.info("ColSize too big for Batch statement");
			return;
		}
		
		for (int i = 0; i < rowSize; i++) {
			
			List<Object> bindingsList = new ArrayList<Object>();
			StringBuffer batchStmt = new StringBuffer();
			batchStmt.append("BEGIN BATCH\n");
			
			for (int j = 0; j < colSize; j++) {

				batchStmt.append(this.INSERT_INTO_FOLLOWERS);
				batchStmt.append("\n");
			
				bindingsList.add("id-" + (i+1));
				bindingsList.add(UUID.randomUUID());
				bindingsList.add("Name " + (j+1));
								
			}
			batchStmt.append("APPLY BATCH\n");
			
			PreparedStatement preparedStmt = session.prepare(batchStmt.toString());
			BoundStatement boundStmt = new BoundStatement(preparedStmt);
			boundStmt.bind(bindingsList.toArray());
			
			session.executeAsync(boundStmt);
			
			count++;
			
			if (count % 100 == 0){
				Log.info("Inserted " + count + " rows");
			}
		}

		Log.info("Inserted (Batch) " + colSize + " columns for " + rowSize + " rows in " + (System.currentTimeMillis() - start) + "ms");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Main();
	}
}

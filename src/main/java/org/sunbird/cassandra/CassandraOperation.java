package org.sunbird.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import java.sql.Timestamp;

/**
 * this is an interface class for connection with db..
 *
 */
public interface CassandraOperation {

  /**
   * this method will return the resultSet of the queried data
   *
   * @param query
   * @return ResultSet
   */
  public ResultSet getRecords(String query);

  public ResultSet getRecord(Statement query);


  /** this method will be responsible to close the db connection */
  public void closeConnection();

  /**
   * this method is used to insert the user record into db
   *
   * @return boolean
   */
  public boolean updateRecord(Statement query, String id);

  public Session getSession();
}

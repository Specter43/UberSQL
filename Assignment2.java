import java.sql.*;
// You should use this class so that you can represent SQL points as
// Java PGpoint objects.
import org.postgresql.geometric.PGpoint;

public class Assignment2 {

   // A connection to the database
   Connection connection;

   Assignment2() throws SQLException {
      try {
         Class.forName("org.postgresql.Driver");
      } catch (ClassNotFoundException e) {
         e.printStackTrace();
      }
   }

  /**
   * Connects and sets the search path.
   *
   * Establishes a connection to be used for this session, assigning it to
   * the instance variable 'connection'.  In addition, sets the search
   * path to uber, public.
   *
   * @param  url       the url for the database
   * @param  username  the username to connect to the database
   * @param  password  the password to connect to the database
   * @return           true if connecting is successful, false otherwise
   */
   public boolean connectDB(String URL, String username, String password) {
      // Implement this method!
      try {
         connection = DriverManager.getConnection(URL, username, password);
         PreparedStatement stat = 
            connection.prepareStatement("SET SEARCH_PATH TO uber, public");
	      stat.execute();
      } catch (SQLException ex){
	      System.err.println("Error in connectDB" + ex.getMessage());
         return false;
      }
      System.out.println("Connected to database");
      return true;
   }

  /**
   * Closes the database connection.
   *
   * @return true if the closing was successful, false otherwise
   */
   public boolean disconnectDB() {
      // Implement this method!
      try {
         connection.close();
      } catch(SQLException ex){
         return false;
      }
      System.out.println("Disconnected to database");
      return true;
   }
   
   /* ======================= Driver-related methods ======================= */

   /**
    * Records the fact that a driver has declared that he or she is available 
    * to pick up a client.  
    *
    * Does so by inserting a row into the Available table.
    * 
    * @param  driverID  id of the driver
    * @param  when      the date and time when the driver became available
    * @param  location  the coordinates of the driver at the time when 
    *                   the driver became available
    * @return           true if the insertion was successful, false otherwise. 
    */
   public boolean available(int driverID, Timestamp when, PGpoint location) {
	   // Implement this method!
	   try {
    String query = "INSERT INTO Available " +
                        "VALUES (?,?,?)";
         PreparedStatement stat = connection.prepareStatement(query);
	      stat.setInt(1, driverID);
	      stat.setTimestamp(2, when);
	      stat.setObject(3, location);
	      int num = stat.executeUpdate();
	      System.out.println("number: " + num);
         if (num == 0) {
            return false;
         }
      } catch (SQLException se) {
	      System.err.println("Error in available" + se.getMessage());
         return false;
      }
      return true;
   }

   /**
    * Records the fact that a driver has picked up a client.
    *
    * If the driver was dispatched to pick up the client and the corresponding
    * pick-up has not been recorded, records it by adding a row to the
    * Pickup table, and returns true.  Otherwise, returns false.
    * 
    * @param  driverID  id of the driver
    * @param  clientID  id of the client
    * @param  when      the date and time when the pick-up occurred
    * @return           true if the operation was successful, false otherwise
    */
   public boolean picked_up(int driverID, int clientID, Timestamp when) {
      // Implement this method!
      try {
         String query = "SELECT Dispatch.driver_id, Dispatch.request_id, " + 
                        "Request.client_id " + 
                        "FROM Dispatch, Request " +
                        "WHERE (Dispatch.request_id NOT IN " +
                        "(SELECT request_id FROM Pickup)) and " +
                        "(Dispatch.request_id = Request.request_id)";
         PreparedStatement stat = connection.prepareStatement(query);
         ResultSet rs = stat.executeQuery();
         while (rs.next()) {
            int driver = rs.getInt("driver_id");
            int client = rs.getInt("client_id");
            int request = rs.getInt("request_id");
            if (driver == driverID && client == clientID) {
               String update_query = "INSERT INTO Pickup " + 
                                       "VALUES (?,?)";
               PreparedStatement stat2 = 
                          connection.prepareStatement(update_query);
	       stat2.setInt(1, request);
	       stat2.setTimestamp(2, when);
               if (stat2.executeUpdate() == 0){return false;}
            }
         }
      } catch (SQLException se) {
	 System.err.println("Error in picked up" + se.getMessage());
         return false;
      }
      return true;
   }
   
   /* ===================== Dispatcher-related methods ===================== */
   public ResultSet getWaitingClients(PGpoint NW, PGpoint SE) {	
	ResultSet rs = null;
   	try{
	PreparedStatement dropView1 = connection.prepareStatement(
	"DROP VIEW IF EXISTS WaitingClient CASCADE");
	PreparedStatement dropView2 = connection.prepareStatement(
	"DROP VIEW IF EXISTS ClientHistoryBilling CASCADE");
	PreparedStatement dropView3 = connection.prepareStatement(
	"DROP VIEW IF EXISTS AvailableDriver CASCADE");
        dropView1.execute();
	dropView2.execute();
	dropView3.execute();
      } catch (SQLException se) {
	System.err.println("Error in dropViews." + "<Message>: " +
                            se.getMessage());
      }

      try{
	PreparedStatement waitingClient = connection.prepareStatement(
	"CREATE VIEW WaitingClient as " +
	"SELECT R.client_id, R.request_id, R.source, R.destination "+
	"FROM Request R " +
	"WHERE R.request_id NOT IN (SELECT request_id FROM Dispatch)");
         waitingClient.execute();
      } catch (SQLException se) {
	System.err.println("Error in waitingClient." + "<Message>: " +
                            se.getMessage());
      }

      try{
	PreparedStatement clientHistoryBilling = connection.prepareStatement(
	"CREATE VIEW ClientHistoryBilling as " +
	"SELECT R.client_id, " +
	"       sum(B.amount) as total " +
	"FROM Dropoff D, Billed B, Request R " +
	"WHERE D.request_id = B.request_id AND " +
	"      B.request_id = R.request_id " +
	"GROUP BY client_id " +
	"ORDER BY sum(B.amount) DESC");
         clientHistoryBilling.execute();
      } catch (SQLException se) {
	System.err.println("Error in clientHistoryBilling." + "<Message>: " +
                            se.getMessage());
      }

      try{
	String sqlText1 = 
	"CREATE VIEW WaitingRequest as " +
	"SELECT W.client_id as client_id, " +
	"       W.request_id as request_id, " +
	"       W.source as source_name, " +
	"       P1.location as source_location, " +
	"       W.destination as destination_name, " +
	"       P2.location as destination_location, " +
	"       C.total as history_total " + 
	"FROM (WaitingClient W JOIN Place P1 on W.source = P1.name) JOIN " +
	"      Place P2 on W.destination = P2.name, ClientHistoryBilling C " +
	"WHERE W.client_id = C.client_id " +
	"      AND P1.location[0] <= %f " +
        "      AND P1.location[0] >= %f " +
	"      AND P1.location[1] <= %f " +
        "      AND P1.location[1] >= %f " +
	"ORDER BY history_total DESC";
        PreparedStatement waitingRequest =
		connection.prepareStatement(
		String.format(sqlText1, NW.x, SE.x, SE.y, NW.y));
        waitingRequest.execute();
      } catch (SQLException se) {
	System.err.println("Error in waitingRequest." + "<Message>: " +
                            se.getMessage());
      }

      try{
	String sqlText2 = 
	"CREATE VIEW AvailableDriver as " +
	"SELECT * " +
	"FROM Available A " +
	"GROUP BY A.datetime, A.driver_id " +
	"HAVING (max(A.datetime) > all(SELECT datetime " +
	"	              FROM Dispatch " +
	"	              WHERE Dispatch.driver_id = A.driver_id)) " +
	"	AND " +
	"       (max(A.datetime) >= all(SELECT Dropoff.datetime " +
	"	              FROM Dispatch, Dropoff " +
	"	              WHERE Dispatch.request_id = Dropoff.request_id "+
	"	                    AND Dispatch.driver_id = A.driver_id)) " +
	"       AND " +
        "       (max(A.datetime) <= any(SELECT datetime " +
        "                     FROM Request)) " +
	"       AND A.location[0] <= %f " +
        "       AND A.location[0] >= %f " +
	"       AND A.location[1] <= %f " +
        "       AND A.location[1] >= %f";
        PreparedStatement availableDriver =
		connection.prepareStatement(
		String.format(sqlText2, NW.x, SE.x, SE.y, NW.y));
	availableDriver.execute();
      } catch (SQLException se) {
	System.err.println("Error in availableDriver." + "<Message>: " +
                            se.getMessage());
      }

      try{
	PreparedStatement distanceToClients = connection.prepareStatement(
	"CREATE VIEW DistanceToClients as " +
	"SELECT DISTINCT A.driver_id, " +
        "       A.location <@> W.source_location as distance_to_client, " +
	"	W.client_id " +
	"FROM AvailableDriver A, WaitingRequest W");
	distanceToClients.execute();
      } catch (SQLException se) {
	System.err.println("Error in distanceToClients." + "<Message>: " +
                            se.getMessage());
      }

      try{
	PreparedStatement minDis = connection.prepareStatement(
	"CREATE VIEW MinDis as " +
	"SELECT client_id, min(distance_to_client) as min_dis " +
	"FROM DistanceToClients D1 " +
	"GROUP BY client_id");
	minDis.execute();
      } catch (SQLException se) {
	System.err.println("Error in minDis." + "<Message>: " +
                            se.getMessage());
      }

      try{
	PreparedStatement minDisToClients = connection.prepareStatement(
	"CREATE VIEW MinDisToClients as " +
	"SELECT DistanceToClients.driver_id, MinDis.min_dis, MinDis.client_id "+
	"FROM MinDis LEFT JOIN DistanceToClients " +
	"	    ON MinDis.client_id = DistanceToClients.client_id AND " +
	"	       MinDis.min_dis = DistanceToClients.distance_to_client");
	minDisToClients.execute();
      } catch (SQLException se) {
	System.err.println("Error in minDisToClients." + "<Message>: " +
                            se.getMessage());
      }

      try{
        PreparedStatement driverToClients = connection.prepareStatement(
	"CREATE VIEW DriverToClients as " +
	"SELECT M.driver_id, A.location as driver_location, " +
        "       M.min_dis, M.client_id " + 
	"FROM MinDisToClients M, AvailableDriver A " +
	"WHERE M.driver_id = A.driver_id");
	driverToClients.execute();
      } catch (SQLException se) {
	System.err.println("Error in driverToClients." + 
                           "<Message>: " + se.getMessage());
      }

      try{
      PreparedStatement waitingRequestWithDriver = connection.prepareStatement(
	"SELECT W.*, D.driver_id as closest_driver, " +
	"	    D.driver_location, " +
	"	    D.min_dis as distance_to_driver " +
	"FROM WaitingRequest W, DriverToClients D " +
	"WHERE W.client_id = D.client_id " +
	"ORDER BY W.history_total DESC " +
	"LIMIT 1");
	rs = waitingRequestWithDriver.executeQuery();
	return rs;
      } catch (SQLException se) {
	System.err.println("Error in waitingRequestWithDriver." + 
                           "<Message>: " + se.getMessage());
      }
      return rs;
   }
   /**
    * Dispatches drivers to the clients who've requested rides in the area
    * bounded by NW and SE.
    * 
    * For all clients who have requested rides in this area (i.e., whose 
    * request has a source location in this area), dispatches drivers to them
    * one at a time, from the client with the highest total billings down
    * to the client with the lowest total billings, or until there are no
    * more drivers available.
    *
    * Only drivers who (a) have declared that they are available and have 
    * not since then been dispatched, and (b) whose location is in the area
    * bounded by NW and SE, are dispatched.  If there are several to choose
    * from, the one closest to the client's source location is chosen.
    * In the case of ties, any one of the tied drivers may be dispatched.
    *
    * Area boundaries are inclusive.  For example, the point (4.0, 10.0) 
    * is considered within the area defined by 
    *         NW = (1.0, 10.0) and SE = (25.0, 2.0) 
    * even though it is right at the upper boundary of the area.
    *
    * Dispatching a driver is accomplished by adding a row to the
    * Dispatch table.  All dispatching that results from a call to this
    * method is recorded to have happened at the same time, which is
    * passed through parameter 'when'.
    * 
    * @param  NW    x, y coordinates in the northwest corner of this area.
    * @param  SE    x, y coordinates in the southeast corner of this area.
    * @param  when  the date and time when the dispatching occurred
    */
   public void dispatch(PGpoint NW, PGpoint SE, Timestamp when) {
      // Implement this method!
      while (true) {
          System.err.println("good");
          ResultSet rs = null;
          try {
              rs = getWaitingClients(NW, SE);
	      if (rs.next()) {
                  int request_id = rs.getInt(2);
                  System.err.println("request_id = " + request_id);
                  int driver_id = rs.getInt(8);
                  System.err.println("driver_id = " + driver_id);
	          PGpoint car_location = (PGpoint) rs.getObject(9);
                  System.err.println("car_location = " + car_location);
                  System.err.println("datetime = " + when);
	          PreparedStatement insertInfo = connection.prepareStatement(
		          "INSERT INTO Dispatch " + 
		          "VALUES (?,?,?,?)");
	          insertInfo.setInt(1, request_id);
	          insertInfo.setInt(2, driver_id);
	          insertInfo.setObject(3, car_location);
	          insertInfo.setObject(4, when);
	          insertInfo.execute();
              } else {break;}
          } catch (SQLException se) {
	      System.err.println("Error in insertInfo." + 
                                 "<Message>: " + se.getMessage());
          }
      }
   }

//    public void PrintInfo(){
//       try{
//         String dispatch = "SELECT * FROM Dispatch";
//         PreparedStatement stat0 = connection.prepareStatement(dispatch);
//         ResultSet rs0 = stat0.executeQuery();
//         while (rs0.next()) {
// 	    int request_id = rs0.getInt("request_id");
// 	    int closest_driver = rs0.getInt("driver_id");
// 	    PGpoint car_location = (PGpoint) rs0.getObject("car_location");
//             Timestamp datetime = (Timestamp) rs0.getObject("datetime");
// 	    System.err.println("Dispatch: " + request_id + " " + 
//                                closest_driver + " " +  car_location + " " +
//                                datetime);
//         }
//       } catch (SQLException se){
// 	System.err.println("WHAT R U FUCKING DOING? 0" + se.getMessage());
// 	System.err.println("Error!");
//       }
//       try{
// 	String query1 = "SELECT * FROM Available";
//       PreparedStatement stat1 = connection.prepareStatement(query1);
//       ResultSet rs1 = stat1.executeQuery();
//       while (rs1.next()) {
//         int driver = rs1.getInt("driver_id");
//          Timestamp time = (Timestamp) rs1.getObject("datetime");
//          PGpoint location = (PGpoint) rs1.getObject("location");
//          System.out.println("available: " + driver + " " + time + location);
//       }

//       String query2 = "SELECT * FROM Pickup";
//       PreparedStatement stat2 = connection.prepareStatement(query2);
//       ResultSet rs2 = stat2.executeQuery();
//       while (rs2.next()) {
//         int request = rs2.getInt("request_id");
//          Timestamp time1 = (Timestamp) rs2.getObject("datetime");
//          System.out.println("pickup: " + request + " " + time1);
//       }
//     }catch(SQLException se){
// 	System.err.println("WHAT R U FUCKING DOING? 1" + se.getMessage());
// 	System.err.println("Error!");
// 	}  
//    }

   public static void main(String[] args) {
      // You can put testing code in here. It will not affect our autotester.
      System.out.println("Boo!");
    //   PGpoint p1 = new PGpoint(1.361, 51.3267);
    //   Timestamp t1 = Timestamp.valueOf("2019-10-22 09:00:00");
    //   Timestamp t2 = Timestamp.valueOf("2019-10-12 12:15:00");
    //   PGpoint NW = new PGpoint(100, 0);
    //   PGpoint SE = new PGpoint(0, 100);
    //   Timestamp when = Timestamp.valueOf("2016-02-02 11:05:00");
    //   try {
    //       Assignment2 test = new Assignment2();
	//   test.connectDB("jdbc:postgresql://localhost:5432/csc343h-yangzhey", "yangzhey", "");
	//    System.out.println("result: " + test.connectDB("jdbc:postgresql://localhost:5432/csc343h-yangzhey", "yangzhey", ""));
    //        test.PrintInfo();
	//    test.available(12345, t1, p1);
    //        test.picked_up(22222, 222, t2);
    //        test.dispatch(NW, SE, when);
	//    test.PrintInfo();
    //        test.disconnectDB();
    //   } catch(SQLException se) {
	//     System.err.println("Error in main." + se.getMessage());
    //   }     
     
   }

}

import java.sql.*;
import java.util.*;

public class UniDB {

    public static void print_students(Connection conn, ArrayList<String> id_result) throws SQLException{
        int numMatches = id_result.size();
        System.out.println(numMatches + " matches found.");

        for (String id : id_result) {

           // int credits = 0;

            String x = "SELECT S.first_name, S.last_name FROM Students S WHERE S.id = ?";
            PreparedStatement w = conn.prepareStatement(x);
            w.setInt(1, Integer.parseInt(id));
            ResultSet y = w.executeQuery();
            StringBuilder sb = new StringBuilder();

            while (y.next()){
                String firstName = y.getString("first_name");
                String lastName = y.getString("last_name");
                //credits = y.getInt("credits");
                sb.append(lastName + ", " + firstName + "\n");
            }

            String s =
                    "SELECT " +
                    "SUM(CASE ht.grade WHEN 'A' THEN 4.0 WHEN 'B' THEN 3.0 WHEN 'C' THEN 2.0 WHEN 'D' THEN 1.0 END * CASE WHEN ht.grade <> 'F' THEN c.credits ELSE 0.0 END) / SUM(CASE WHEN ht.grade <> 'F' THEN c.credits ELSE 0.0 END) AS gpa, COALESCE(SUM(CASE WHEN ht.grade <> 'F' THEN c.credits ELSE 0.0 END), 0.0)  as credits FROM Students S LEFT OUTER JOIN HasTaken ht ON S.id = ht.sid INNER JOIN Classes c ON ht.name = c.name WHERE S.id=?";


            PreparedStatement p = conn.prepareStatement(s);
            p.setInt(1, Integer.parseInt(id));
            ResultSet rs = p.executeQuery();


            ArrayList<String> majors = new ArrayList<>();
            ArrayList<String> minors = new ArrayList<>();

            String major_query = "SELECT dname FROM Majors WHERE sid = ?";
            PreparedStatement p1 = conn.prepareStatement(major_query);
            p1.setInt(1, Integer.parseInt(id));
            ResultSet mrs = p1.executeQuery();

            String minor_query = "SELECT dname FROM Minors WHERE sid = ?";
            PreparedStatement p2 = conn.prepareStatement(minor_query);
            p2.setInt(1, Integer.parseInt(id));
            ResultSet minrs = p2.executeQuery();

            while(rs.next()){
                double gpa_result = rs.getDouble("gpa");
                int credits = rs.getInt("credits");

                sb.append( "ID: " + id + "\n");

                StringBuilder major_section = new StringBuilder();

                while(mrs.next()){
                    String m = mrs.getString("dname");
                    major_section.append(m);
                    if (!mrs.isLast()) {
                        major_section.append(", ");
                    }
                }

                StringBuilder min_section = new StringBuilder();
                while(minrs.next()) {
                    String m = minrs.getString("dname");
                    min_section.append(m);
                    if (!minrs.isLast()) {
                        min_section.append(", ");
                    }

                }

                String omit = "SELECT TRUE WHERE NOT EXISTS (SELECT * FROM HasTaken WHERE sid = ?);";
                PreparedStatement noCreditFresh = conn.prepareStatement(omit);
                noCreditFresh.setInt(1, Integer.parseInt(id));
                ResultSet gpaBool = noCreditFresh.executeQuery();

                boolean checker = false;
                while (gpaBool.next()){
                    boolean value = gpaBool.getBoolean(1);
                    if (value){
                        checker = true;
                    }
                }

                sb.append("Major(s): " + major_section.toString() + "\n"); sb.append("Minor(s): " + min_section.toString() +"\n");
                if (!checker){
                    sb.append("GPA: " + gpa_result + "\n");
                }
                sb.append("Credits: " + credits + "\n" + "\n");

            }
            System.out.print(sb.toString());

        }
    }

    public static void query_1(Connection conn, String name_substring) throws SQLException {

        ArrayList<String> id_result = new ArrayList<>();
        name_substring = name_substring.toLowerCase();

        String a1 = "SELECT DISTINCT id FROM Students WHERE first_name LIKE ? OR last_name LIKE ?;";
        PreparedStatement p1 = conn.prepareStatement(a1);
        p1.setString(1, "%" + name_substring + "%");
        p1.setString(2, "%" + name_substring + "%");
        ResultSet rs = p1.executeQuery();


        StringBuilder sb = new StringBuilder();

        while (rs.next()){
            String id = rs.getString("id");
            id_result.add(id);
        }

        print_students(conn, id_result);
    }

    public static void query_2(Connection conn, String yr) throws SQLException {

        ArrayList<String> id_results = new ArrayList<>();
        int min = Integer.MIN_VALUE;
        int max = Integer.MAX_VALUE;

        switch (yr.toLowerCase()){
            case "fr":
                min = 0;
                max = 29;
                break;
            case "so":
                min = 30;
                max = 59;
                break;
            case "ju":
                min = 60;
                max = 89;
                break;
            case "sr":
                min = 90;
                max = Integer.MAX_VALUE;
                break;
            default:
                System.out.println("Invalid year.");
                return;
        }

        String b2 = "SELECT DISTINCT subquery.id\n" +
                "FROM (\n" +
                "  SELECT s.id, COALESCE(SUM(c.credits), 0) as credits FROM Students s LEFT OUTER JOIN HasTaken ht ON s.id = ht.sid LEFT OUTER JOIN Classes c ON ht.name = c.name GROUP BY s.id ORDER BY s.id ) as subquery " +
                "WHERE subquery.credits >= ? AND subquery.credits <= ?;\n";
                //"//>= ? AND credits <= ?";
        PreparedStatement p = conn.prepareStatement(b2);
        p.setInt(1, min);
        p.setInt(2, max);
        ResultSet rs = p.executeQuery();
        while (rs.next()){
            String id = rs.getInt("id") + "";
            id_results.add(id);
        }
        // call print student here
        print_students(conn, id_results);
    }

    public static void query_3 (Connection conn, double test) throws SQLException {

        //String query_3 =
       //         "SELECT gpa_table.id FROM ( SELECT S.id, SUM(CASE ht.grade WHEN 'A' THEN 4.0 WHEN 'B' THEN 3.0 WHEN 'C' THEN 2.0 WHEN 'D' THEN 1.0 ELSE 0.0 END * c.credits) / SUM(c.credits) AS gpa FROM HasTaken ht INNER JOIN Classes c ON ht.name = c.name INNER JOIN Students S ON S.id = ht.sid GROUP BY S.id ) AS gpa_table WHERE gpa >= ?;";
        String query_3 =
                "SELECT gpa_table.id FROM ( SELECT S.id, SUM(CASE ht.grade WHEN 'A' THEN 4.0 WHEN 'B' THEN 3.0 WHEN 'C' THEN 2.0 WHEN 'D' THEN 1.0 END * CASE WHEN ht.grade <> 'F' THEN c.credits ELSE 0.0 END) / SUM(CASE WHEN ht.grade <> 'F' THEN c.credits ELSE 0.0 END) AS gpa FROM Students s LEFT OUTER JOIN HasTaken ht ON S.id = ht.sid LEFT OUTER JOIN Classes c ON c.name = ht.name GROUP BY S.id ORDER BY S.id ) AS gpa_table WHERE gpa >= ?;";
        PreparedStatement p = conn.prepareStatement(query_3);
        p.setDouble(1, test);
        ResultSet rs = p.executeQuery();

        ArrayList<String> id_results = new ArrayList<>();

        while (rs.next()){
            String id = rs.getInt("id") + "";
            id_results.add(id);
        }
        print_students(conn, id_results);

    }

    public static void query_4 (Connection conn, double vth) throws SQLException {

        String query_4 =
                "SELECT gpa_table.id FROM ( SELECT S.id, SUM(CASE ht.grade WHEN 'A' THEN 4.0 WHEN 'B' THEN 3.0 WHEN 'C' THEN 2.0 WHEN 'D' THEN 1.0 END * CASE WHEN ht.grade <> 'F' THEN c.credits ELSE 0.0 END) / SUM(CASE WHEN ht.grade <> 'F' THEN c.credits ELSE 0.0 END) AS gpa FROM Students s LEFT OUTER JOIN HasTaken ht ON S.id = ht.sid LEFT OUTER JOIN Classes c ON c.name = ht.name GROUP BY S.id ORDER BY S.id ) AS gpa_table WHERE gpa <= ?;";
        PreparedStatement p = conn.prepareStatement(query_4);
        p.setDouble(1, vth);
        ResultSet rs = p.executeQuery();

        ArrayList<String> id_results = new ArrayList<>();

        while (rs.next()){
            String id = rs.getInt("gpa_table.id") + "";
            id_results.add(id);
        }
        print_students(conn, id_results);
    }

    public static void query_5 (Connection conn, String department) throws SQLException {

        department = department.toLowerCase();

        String query_5 = "SELECT COUNT(DISTINCT gpa_table.id) as count, AVG(gpa) as avg FROM (SELECT S.id, (SUM(CASE ht.grade WHEN 'A' THEN 4.0 WHEN 'B' THEN 3.0 WHEN 'C' THEN 2.0 WHEN 'D' THEN 1.0 END * CASE WHEN ht.grade <> 'F' THEN c.credits ELSE 0.0 END) / SUM(CASE WHEN ht.grade <> 'F' THEN c.credits ELSE 0.0 END) ) AS gpa FROM Students S LEFT OUTER JOIN HasTaken ht ON S.id = ht.sid LEFT OUTER JOIN Classes c ON ht.name = c.name INNER JOIN Majors M ON M.sid = S.id INNER JOIN Minors M2 ON M2.sid = S.id WHERE M.dname = ? OR M2.dname = ? GROUP BY S.id) AS gpa_table;";
        PreparedStatement p = conn.prepareStatement(query_5);
        p.setString(1, department);
        p.setString(2, department);
        ResultSet rs = p.executeQuery();

        while (rs.next()){
            int numStudents = rs.getInt("count");
            double avg_gpa = rs.getDouble("avg");

            System.out.println("Num students: " + numStudents + "\n" + "Average GPA: " + avg_gpa);
        }

    }

    public static void query_6 (Connection conn, String class_name) throws SQLException{

        String iths =
                "SELECT count(DISTINCT it.sid) as count, 0 as numA, 0 as numB, 0 as numC, 0 as numD, 0 as numF FROM isTaking it INNER JOIN Classes c ON c.name = it.name INNER JOIN Students s ON s.id = it.sid WHERE c.name = ? UNION ALL SELECT 0 as count, count(CASE WHEN ht.grade = 'A' THEN 1 END) as numA, count(CASE WHEN ht.grade = 'B' THEN 1 END) as numB, count(CASE WHEN ht.grade = 'C' THEN 1 END) as numC, count(CASE WHEN ht.grade = 'D' THEN 1 END) AS numD, count(CASE WHEN ht.grade = 'F' THEN 1 END) as numF FROM hasTaken ht INNER JOIN Classes c ON c.name = ht.name INNER JOIN Students s ON s.id = ht.sid WHERE c.name =  ? GROUP BY ht.grade;\n";
        //String it =
        //        "SELECT count(DISTINCT s.id) as count FROM isTaking it INNER JOIN Classes c ON c.name = it.name INNER JOIN Students S ON S.id = it.sid WHERE it.name = ?;\n";
        PreparedStatement p = conn.prepareStatement(iths);
        p.setString(1, class_name);
        p.setString(2, class_name);
        ResultSet rs = p.executeQuery();
       // System.out.println();
        int countA = 0, countB=0, countC=0, countD=0, countF = 0, count = 0;
        while (rs.next()){
            count += rs.getInt("count");
            countA += rs.getInt("numA");
            countB += rs.getInt("numB");
            countC += rs.getInt("numC");
            countD += rs.getInt("numD");
            countF += rs.getInt("numF");
        }

        System.out.println(count +" students currently enrolled");
        System.out.println("Grades of previous enrollees: \n" + "A " + countA +"\n" + "B " + countB +"\n" + "C " + countC +"\n" + "D " + countD +"\n" + "F " + countF);
    }

    public static void query_7 (Connection conn, String query) throws SQLException {

        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(query);

        ResultSetMetaData metaData = rs.getMetaData();
        int count = metaData.getColumnCount();
        System.out.println();
        for (int i = 1; i <= count; i++) {
            System.out.print(metaData.getColumnName(i) + "\t");

        }
        while (rs.next()) {
            System.out.println();
            for (int i = 1; i <= count; i++){
                System.out.print(rs.getString(i) + "\t");
            }
        }
        System.out.println();
    }
    public static void main(String[] args) throws SQLException {

        String url = "jdbc:mysql://" + args[0];
        String user = args[1];
        String pass = args[2];

        Scanner scanner = new Scanner(System.in);
        Connection conn = DriverManager.getConnection(url, user, pass);

        String a = "1. Search students by name.\n";
        String b = "2. Search students by year.\n";
        String c = "3. Search for students with a GPA >= threshold.\n";
        String d = "4. Search for students with a GPA <= threshold.\n";
        String e = "5. Get department statistics.\n";
        String f = "6. Get class statistics.\n";
        String g = "7. Execute an abitrary SQL query.\n";
        String h = "8. Exit the application.";

        boolean exit = false;

        System.out.println("Welcome to the university database. Queries available:");
        System.out.println(a+b+c+d+e+f+g+h);

        while (true){
            String input = scanner.nextLine();
            switch(input){
                case "1":
                    System.out.println("Please enter the name.");
                    String name_substring = scanner.next();
                    query_1(conn, name_substring);
                    System.out.println("Which query would you like to run (1-8)?");
                    break;
                case "2":
                    System.out.print("Please enter the year.");
                    String yr = scanner.nextLine();
                    query_2(conn, yr);
                    System.out.println("Which query would you like to run (1-8)?");
                    break;
                case "3":
                    System.out.println("Please enter the threshold.");
                    double gpa = Double.parseDouble(scanner.nextLine());
                    query_3(conn, gpa);
                    System.out.println("Which query would you like to run (1-8)?");
                    break;
                case "4":
                    System.out.println("Please enter the threshold.");
                    double gpa_th = Double.parseDouble(scanner.nextLine());
                    query_4(conn, gpa_th);
                    System.out.println("Which query would you like to run (1-8)?");
                    break;
                case "5":
                    System.out.println("Please enter the department.");
                    String dep = scanner.nextLine();
                    query_5(conn, dep);
                    System.out.println("Which query would you like to run (1-8)?");
                    break;
                case "6":
                    System.out.println("Please enter the class name.");
                    String class_name = scanner.nextLine();
                    query_6(conn, class_name);
                    System.out.println("Which query would you like to run (1-8)?");
                    break;
                case "7":
                    System.out.println("Please enter the query.");
                    String query = scanner.nextLine();
                    query_7(conn, query);
                    System.out.println("Which query would you like to run (1-8)?");
                    break;
                case "8":
                    exit = true;
                    break;
                default:
                    //System.out.println("Error in choosing a query to run.\nPlease try again.");
                    break;
            }
            if (exit){
                System.out.println("Goodbye.");
                break;
            }
        }
        conn.close();
    }

}

import java.util.Properties;
import java.sql.*;

public class druidSql {

    public static void main(String args[]) {
        // Connect to /druid/v2/sql/avatica/ on your broker.
        String url = "jdbc:avatica:remote:url=http://localhost:8082/druid/v2/sql/avatica/";
        //String query = "select * from Invsales_kafka where sales_date='2018-06-15'";
        String query = "select distinct \"sales_date\",\"sales_hr_nbr\",sum(\"ty_sales\") as ty_sales,sum(\"ty_comp\") as ty_comp,sum(\"ly_sales\") as ly_sales,sum(\"ly_comp\") as ly_comp,\n" +
                "    sum(\"ty_cust_cnt\") as ty_cust_cnt,sum(\"ty_comp_cust_cnt\") as ty_comp_cust_cnt,sum(\"ly_cust_cnt\") as ly_cust_cnt,sum(\"ly_comp_cust_cnt\")as ly_comp_cust_cnt\n" +
                "    from \"fl_hrly_s1\"\n" +
                "    where \"oper_level_code\" like '%REG%'\n" +
                "      AND \"mdse_level_code\" like '%DPT%'\n" +
                "      AND \"sales_date\" >= '2018-07-10'\n" +
                "    group by sales_date,sales_hr_nbr";

        // Set any connection context parameters you need here (see "Connection context" below).
// Or leave empty for default behavior.
        Properties connectionProperties = new Properties();

        try {
            Connection client = DriverManager.getConnection(url, connectionProperties);
            try {
                final Statement statement = client.createStatement();
                final ResultSet resultSet = statement.executeQuery(query);
                ResultSetMetaData metadata = resultSet.getMetaData();
                int colCount = metadata.getColumnCount();
                int i=0;
                int j=0;
                while (resultSet.next()) {
                    j++;
                    StringBuilder sb = new StringBuilder();
                    for(i=1;i <= colCount; i++) {
                        sb.append(String.format(String.valueOf(resultSet.getString(i))) + "|");
                    }
                    System.out.println(sb.toString());
                }
            } catch (SQLException ex) {
                System.out.println(" Sql error " + ex);
                }
        } catch (SQLException ex) {
            System.out.println(ex);
        }
    }
}
package newt.test;

//import org.apache.spark.lineage.NewtWrapper;

import newt.client.NewtClient;
import newt.common.Configuration;
import newt.server.NewtMySql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Vector;

/**
 * Created by kshitij on 1/16/15.
 */
public class KshCaptureTest {

    public static void main(String[] args)
    {
        //CaptureTest();

        //TraceTest();
        //TraceTest("HadoopActorTable1422660086","Test","HadoopActor1422660086",3);
        TraceTest("HadoopActorTable441494053","Test","HadoopActor441494053",6," where output='(1,1,1)'");

    }

    private static void CaptureTest() {
        NewtWrapper newt1 = new NewtWrapper(500);
        String[] data1 = {"k1","k2","k3"};
        newt1.add("k11", Arrays.asList(data1));
        newt1.commit();


        NewtWrapper newt2 = new NewtWrapper(501);
        String[] data2 = {"k11"};
        newt2.add("k21",Arrays.asList(data2));
        newt2.commit();

        newt2.finalCommit();
    }

    private static void TraceTest()
    {
        TraceTest("HadoopActorTable501","Test","HadoopActor501",10,"");
    }

    private static void TraceTest(String tableName,String actorType,String actorName,int actorID,String clause) {
        Vector data = new Vector();
        NewtMySql mySql = new NewtMySql(Configuration.mysqlUser, Configuration.mysqlPassword);
        if(clause == null)
            clause = " ";
        ResultSet rs = mySql.executeQuery("Select output from Newt."+tableName + " " + clause);

        try {
            while(rs.next())
            {
                data.add(rs.getString(1));
                //break;
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        int replayTraceID = NewtClient.trace(data, "backward", 1, actorID, actorType);
        NewtClient.printTraceResults( replayTraceID, actorName );
    }
}

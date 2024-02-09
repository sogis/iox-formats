package ch.interlis.ioxwkf.dbtools;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import ch.ehi.basics.settings.Settings;

@Testcontainers
public class Db2ArcGenTest {
    
    private static final String WAIT_PATTERN = ".*database system is ready to accept connections.*\\s";

    private static final String TEST_OUT = "build/test/data/Db2ArcGen/";

    @Container
    public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("sogis/postgis:16-3.4").asCompatibleSubstituteFor("postgres"))
        .withDatabaseName("edit")
        .withUsername(TestUtil.PG_CON_DDLUSER)
        .withPassword(TestUtil.PG_CON_DDLPASS)
        .waitingFor(Wait.forLogMessage(WAIT_PATTERN, 2));

    @BeforeAll
    public static void setupFolder() {
        new File(TEST_OUT).mkdirs();
    }

    @Test
    public void export_Point_with_Attributes_Ok() throws Exception {
        // Prepare
        File parentDir = new File(TEST_OUT, "export_Point_with_Attributes_Ok");
        parentDir.mkdirs();

        Settings config = new Settings();
        Connection jdbcConnection = null;
        try {
            jdbcConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
            {
                Statement preStmt = jdbcConnection.createStatement();
                preStmt.execute("DROP SCHEMA IF EXISTS dbtoarcgen CASCADE");
                preStmt.execute("CREATE SCHEMA dbtoarcgen");
                preStmt.execute("CREATE TABLE dbtoarcgen.arggenexport(attr character varying, the_geom geometry(POINT,2056));");
                preStmt.executeUpdate("INSERT INTO dbtoarcgen.arggenexport(attr, the_geom) VALUES ('coord2d', '010100002008080000295C8F720EEC43413108AC1C34BC3241')");
                preStmt.close();
            }
            {
                File file = new File(parentDir, "export_Point_with_Attributes_Ok.txt");
                
                config.setValue(IoxWkfConfig.SETTING_DBSCHEMA, "dbtoarcgen");
                config.setValue(IoxWkfConfig.SETTING_DBTABLE, "arggenexport");
                
                // Run
                Db2ArcGen db2ArcGen = new Db2ArcGen();
                db2ArcGen.exportData(file, jdbcConnection, config);

                // Validate
                List<String> allLines = Files.readAllLines(file.toPath());

                for (int i=0; i<allLines.size(); i++) {
                    String[] lineParts = allLines.get(i).split("\t");
                    System.out.println(lineParts[0]);
                    if (i==0) {
                        Assertions.assertEquals("ID",lineParts[0]);
                        Assertions.assertEquals("X",lineParts[1]);
                        Assertions.assertEquals("Y",lineParts[2]);
                        Assertions.assertEquals("ATTR",lineParts[3]);
                    } 
                    if (i==1) {
                        Assertions.assertEquals("1",lineParts[0]);
                        Assertions.assertEquals("2611228.895",lineParts[1]);
                        Assertions.assertEquals("1227828.112",lineParts[2]);
                        Assertions.assertEquals("coord2d",lineParts[3]);
                    }
                    if (i==2) {
                        Assertions.assertEquals("END",lineParts[0]);
                    }
                }
   
            }
        } finally {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }
        }
    }
}

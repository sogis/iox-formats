package ch.interlis.ioxwkf.parquet;

import java.io.File;

import org.junit.BeforeClass;
import org.junit.Test;

import ch.ehi.basics.settings.Settings;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.Iom_jObject;
import ch.interlis.iox.IoxException;
import ch.interlis.iox_j.EndBasketEvent;
import ch.interlis.iox_j.EndTransferEvent;
import ch.interlis.iox_j.ObjectEvent;
import ch.interlis.iox_j.StartBasketEvent;
import ch.interlis.iox_j.StartTransferEvent;
import ch.interlis.ioxwkf.parquet.FlatGeobufWriter;

public class ParquetWriterTest {
    
    private static final String TEST_IN="src/test/data/ParquetWriter/";
    private final static String TEST_OUT="build/test/data/ParquetWriter";

    @BeforeClass
    public static void setupFolder() {
        new File(TEST_OUT).mkdirs();
    }

    @Test
    public void foo() throws IoxException {
        Iom_jObject inputObj = new Iom_jObject("Test1.Topic1.Point", "o1");
        inputObj.setattrvalue("id1", "1");
        inputObj.setattrvalue("aText", "text1");
        inputObj.setattrvalue("aDouble", "53434.123");
        IomObject coordValue = inputObj.addattrobj("attrPoint", "COORD");
        {
            coordValue.setattrvalue("C1", "2600000.000");
            coordValue.setattrvalue("C2", "1200000.000");
        }
        
        IomObject polylineValue=inputObj.addattrobj("attrLineString", "POLYLINE");
        {
            IomObject segments=polylineValue.addattrobj("sequence", "SEGMENTS");
            IomObject coordStart=segments.addattrobj("segment", "COORD");
            IomObject coordEnd=segments.addattrobj("segment", "COORD");
            coordStart.setattrvalue("C1", "2600000.000");
            coordStart.setattrvalue("C2", "1200000.000");
            coordEnd.setattrvalue("C1", "2600010.000");
            coordEnd.setattrvalue("C2", "1200000.000");            
        }

        FlatGeobufWriter writer = null;
        File file = new File(TEST_OUT,"foo.parquet");
        try {
            writer = new FlatGeobufWriter(file);
            Settings settings = new Settings();
            //settings.setValue(FlatGeobufWriter.FEATURES_COUNT, "1");
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
            writer.write(new ObjectEvent(inputObj));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
        } catch(IoxException e) {
            throw new IoxException(e);
        } finally {
            if(writer != null) {
                try {
                    writer.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                writer=null;
            }
        }

    }
    
}

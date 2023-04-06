package ch.interlis.ioxwkf.parquet;

import java.io.File;
import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import ch.ehi.basics.settings.Settings;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.Iom_jObject;
import ch.interlis.iox.IoxException;
import ch.interlis.iox_j.EndBasketEvent;
import ch.interlis.iox_j.EndTransferEvent;
import ch.interlis.iox_j.ObjectEvent;
import ch.interlis.iox_j.StartBasketEvent;
import ch.interlis.iox_j.StartTransferEvent;

public class ParquetWriterTest {
    
    private static final String TEST_IN="src/test/data/ParquetWriter/";
    private static final String TEST_OUT="build/test/data/ParquetWriter";
    
    private static final Configuration testConf = new Configuration();


    @BeforeClass
    public static void setupFolder() {
        new File(TEST_OUT).mkdirs();
    }
    
    // Zukünftige Tests
    // - Falls nicht immer alle Felder optional/nullable sind, kann man das Verhalten auch testen. Es wird ein Fehler geworfen: "java.lang.RuntimeException: Null-value for required field: aText"
    
    @Test
    public void wkt_point_Ok() throws IoxException, IOException {
        // Prepare
        Iom_jObject inputObj = new Iom_jObject("Test1.Topic1.Obj1", "o1");
        IomObject coordValue = inputObj.addattrobj("attrPoint", "COORD");
        {
            coordValue.setattrvalue("C1", "2600000.000");
            coordValue.setattrvalue("C2", "1200000.000");
        }

        // Run
        ParquetWriter writer = null;
        File file = new File(TEST_OUT,"wkt_point_Ok.parquet");
        try {
            writer = new ParquetWriter(file);
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

        // Validate
        Path resultFile = new Path(file.getAbsolutePath());
        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(resultFile,testConf)).build();
      
        GenericRecord record = reader.read();       
        assertEquals(record.get("attrPoint").toString(),"POINT ( 2600000.0 1200000.0 )");
                
        GenericRecord nextRecord = reader.read();
        assertNull(nextRecord);
    }
    
    @Test
    public void wkt_multipoint_Ok() throws IoxException, IOException {
        // Prepare
        Iom_jObject inputObj = new Iom_jObject("Test1.Topic1.Obj1", "o1");
        IomObject multiCoordValue=inputObj.addattrobj("attrMPoint", "MULTICOORD");
        {
            IomObject coordValue1 = multiCoordValue.addattrobj("coord", "COORD");
            coordValue1.setattrvalue("C1", "2600000.000");
            coordValue1.setattrvalue("C2", "1200000.000");

            IomObject coordValue2 = multiCoordValue.addattrobj("coord", "COORD");
            coordValue2.setattrvalue("C1", "2600010.000");
            coordValue2.setattrvalue("C2", "1200000.000");

            IomObject coordValue3 = multiCoordValue.addattrobj("coord", "COORD");
            coordValue3.setattrvalue("C1", "2600010.000");
            coordValue3.setattrvalue("C2", "1200010.000");
        }
        
        // Run
        ParquetWriter writer = null;
        File file = new File(TEST_OUT,"wkt_point_Ok.parquet");
        try {
            writer = new ParquetWriter(file);
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

        // Validate
        Path resultFile = new Path(file.getAbsolutePath());
        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(resultFile,testConf)).build();
      
        GenericRecord record = reader.read();       
        assertEquals(record.get("attrMPoint").toString(),"MULTIPOINT ((2600000 1200000), (2600010 1200000), (2600010 1200010))");
                
        GenericRecord nextRecord = reader.read();
        assertNull(nextRecord);
    }

    @Test
    public void wkt_linestring_Ok() throws IoxException, IOException {
        // Prepare
        Iom_jObject inputObj = new Iom_jObject("Test1.Topic1.Obj1", "o1");
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

        // Run
        ParquetWriter writer = null;
        File file = new File(TEST_OUT,"wkt_linestring_Ok.parquet");
        try {
            writer = new ParquetWriter(file);
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

        // Validate
        Path resultFile = new Path(file.getAbsolutePath());
        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(resultFile,testConf)).build();
      
        GenericRecord record = reader.read();       
        assertEquals(record.get("attrLineString").toString(),"LINESTRING (2600000 1200000, 2600010 1200000)");
                
        GenericRecord nextRecord = reader.read();
        assertNull(nextRecord);
    }
    
    
    
    

    @Test
    public void wkt_point_and_linestring_Ok() throws IoxException, IOException {
        // Prepare
        Iom_jObject inputObj = new Iom_jObject("Test1.Topic1.Obj1", "o1");
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

        // Run
        ParquetWriter writer = null;
        File file = new File(TEST_OUT,"wkt_point_and_linestring_Ok.parquet");
        try {
            writer = new ParquetWriter(file);
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

        // Validate
        Path resultFile = new Path(file.getAbsolutePath());
        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(resultFile,testConf)).build();
      
        GenericRecord record = reader.read();       
        assertEquals(record.get("attrPoint").toString(),"POINT ( 2600000.0 1200000.0 )");
        assertEquals(record.get("attrLineString").toString(),"LINESTRING (2600000 1200000, 2600010 1200000)");
                
        GenericRecord nextRecord = reader.read();
        assertNull(nextRecord);
    }

    @Test
    public void attributes_no_description_set_Ok() throws IoxException, IOException {
        // Prepare
        Iom_jObject inputObj = new Iom_jObject("Test1.Topic1.Obj1", "o1");
        inputObj.setattrvalue("id1", "1");
        inputObj.setattrvalue("aText", "text1");
        inputObj.setattrvalue("aDouble", "53434.123");
        IomObject coordValue = inputObj.addattrobj("attrPoint", "COORD");
        {
            coordValue.setattrvalue("C1", "2600000.000");
            coordValue.setattrvalue("C2", "1200000.000");
        }
        
        // Run
        ParquetWriter writer = null;
        File file = new File(TEST_OUT,"attributes_no_description_set_Ok.parquet");
        try {
            writer = new ParquetWriter(file);
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
        
        // Validate
        Path resultFile = new Path(file.getAbsolutePath());
        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(resultFile,testConf)).build();
      
        GenericRecord record = reader.read();
        assertEquals(record.get("id1").toString(),"1");
        assertEquals(record.get("aText").toString(),"text1");
        assertEquals(record.get("aDouble").toString(),"53434.123");
                
        GenericRecord nextRecord = reader.read();
        assertNull(nextRecord);
    }
    
    @Test
    public void attributes_no_description_set_null_value_Ok() throws IoxException, IOException {
        // Prepare
        Iom_jObject inputObj1 = new Iom_jObject("Test1.Topic1.Obj1", "o1");
        inputObj1.setattrvalue("id1", "1");
        inputObj1.setattrvalue("aText", "text1");            
        
        Iom_jObject inputObj2 = new Iom_jObject("Test1.Topic1.Obj1", "o2");
        inputObj2.setattrvalue("id1", "2");
                
        // Run
        ParquetWriter writer = null;
        File file = new File(TEST_OUT,"attributes_no_description_set_Ok.parquet");
        try {
            writer = new ParquetWriter(file);
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
            writer.write(new ObjectEvent(inputObj1));
            writer.write(new ObjectEvent(inputObj2));
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
        
        // Validate
        Path resultFile = new Path(file.getAbsolutePath());
        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(resultFile,testConf)).build();
      
        GenericRecord record = reader.read();
        assertEquals(record.get("id1").toString(),"1");
        assertEquals(record.get("aText").toString(),"text1");

        GenericRecord nextRecord = reader.read();
        assertEquals(nextRecord.get("id1").toString(),"2");
        assertNull(nextRecord.get("aText"));
        
        assertNull(reader.read());
    }
    
    

}

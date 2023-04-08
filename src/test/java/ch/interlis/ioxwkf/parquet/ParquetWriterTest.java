package ch.interlis.ioxwkf.parquet;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.apache.avro.Schema;
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
    public void dummy() throws IOException {
        //Path resultFile = new Path(new File("/Users/stefan/tmp/lineitem.parquet").getAbsolutePath());
        Path resultFile = new Path(new File("/Users/stefan/Downloads/timestamp_table.parquet").getAbsolutePath());
        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(resultFile,testConf)).build();
      
        GenericRecord record = reader.read();     
        //System.out.println(record.getSchema());
        System.out.println(record.getSchema().getField("timestamp_col").schema());
        System.out.println(record.getSchema().getField("timestamp_col").schema().getDoc());
        System.out.println(record.getSchema().getField("timestamp_col").schema().getObjectProps());
        System.out.println(record.getSchema().getField("timestamp_col").schema().getType());
        System.out.println(record.getSchema().getField("timestamp_col").schema().getLogicalType());

        System.out.println(record.get("timestamp_col"));
        System.out.println(record.get("timestamp_col").getClass());
        
        
//        for (Schema foo : record.getSchema().getField("timestamp_col").schema().getTypes()) {
//            System.out.println("****");
//            System.out.println(foo);
//            System.out.println(foo.getObjectProps());
//            System.out.println(foo.getType());
//            System.out.println(foo.getLogicalType());
//
//        }

        
    }
    
    
    
    
    @Test
    public void attributes_description_set_Ok() throws IoxException, IOException {
        // Prepare
        List<ParquetAttributeDescriptor> attrDescs = new ArrayList<>();
        {
            ParquetAttributeDescriptor attrDesc = new ParquetAttributeDescriptor();
            attrDesc.setAttributeName("id1");
            attrDesc.setBinding(Integer.class);
            attrDescs.add(attrDesc);
        }
        {
            ParquetAttributeDescriptor attrDesc = new ParquetAttributeDescriptor();
            attrDesc.setAttributeName("aText");
            attrDesc.setBinding(String.class);
            attrDescs.add(attrDesc);
        }
        {
            ParquetAttributeDescriptor attrDesc = new ParquetAttributeDescriptor();
            attrDesc.setAttributeName("aDouble");
            attrDesc.setBinding(Double.class);
            attrDescs.add(attrDesc);
        }
        {
            ParquetAttributeDescriptor attrDesc = new ParquetAttributeDescriptor();
            attrDesc.setAttributeName("aDate");
            attrDesc.setBinding(LocalDate.class);
            attrDescs.add(attrDesc);
        }
        {
            ParquetAttributeDescriptor attrDesc = new ParquetAttributeDescriptor();
            attrDesc.setAttributeName("aDatetime");
            attrDesc.setBinding(LocalDateTime.class);
            attrDescs.add(attrDesc);
        }
        
        Iom_jObject inputObj = new Iom_jObject("Test1.Topic1.Obj1", "o1");
        inputObj.setattrvalue("id1", "1");
        inputObj.setattrvalue("aText", "text1");
        inputObj.setattrvalue("aDouble", "53434.123");
        inputObj.setattrvalue("aDate", "1977-09-23");
        inputObj.setattrvalue("aDatetime", "1977-09-23T19:51:35.123");
//        IomObject coordValue = inputObj.addattrobj("attrPoint", "COORD");
//        {
//            coordValue.setattrvalue("C1", "2600000.000");
//            coordValue.setattrvalue("C2", "1200000.000");
//        }
        
        // Run
        ParquetWriter writer = null;
        File file = new File(TEST_OUT,"attributes_description_set_Ok.parquet");
        try {
            writer = new ParquetWriter(file);
            writer.setAttributeDescriptors(attrDescs);
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
        assertEquals(record.get("id1"), Integer.valueOf(1));
        assertEquals(record.get("aText").toString(),"text1");
        assertEquals(Double.valueOf(record.get("aDouble").toString()).doubleValue(),53434.123, 0.0001);
        
        System.out.println("aDate: " + record.get("aDate"));
        
        LocalDate resultDate = Instant.ofEpochSecond((int)record.get("aDate") * 24 * 60 *60 ).atZone(ZoneId.systemDefault()).toLocalDate();
        LocalDate expectedDate = LocalDate.parse("1977-09-23", DateTimeFormatter.ISO_LOCAL_DATE);
        assertEquals(resultDate, expectedDate);


        GenericRecord nextRecord = reader.read();
        assertNull(nextRecord);        
    }
    
    
    
    
    
    
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
        File file = new File(TEST_OUT,"wkt_multipoint_Ok.parquet");
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
    public void wkt_multilinestring_Ok() throws IoxException, IOException {
        // Prepare
        Iom_jObject inputObj = new Iom_jObject("Test1.Topic1.Obj1", "o1");
        IomObject multiPolylineValue=inputObj.addattrobj("attrMLineString", "MULTIPOLYLINE");
        
        IomObject polylineValue=multiPolylineValue.addattrobj("polyline", "POLYLINE");
        {
            IomObject segments=polylineValue.addattrobj("sequence", "SEGMENTS");
            IomObject coordStart=segments.addattrobj("segment", "COORD");
            IomObject coordEnd=segments.addattrobj("segment", "COORD");
            coordStart.setattrvalue("C1", "2600000.000");
            coordStart.setattrvalue("C2", "1200000.000");
            coordEnd.setattrvalue("C1", "2600010.000");
            coordEnd.setattrvalue("C2", "1200000.000");            
        }

        IomObject polylineValue2=multiPolylineValue.addattrobj("polyline", "POLYLINE");
        {
            IomObject segments=polylineValue2.addattrobj("sequence", "SEGMENTS");
            IomObject coordStart=segments.addattrobj("segment", "COORD");
            IomObject coordEnd=segments.addattrobj("segment", "COORD");
            coordStart.setattrvalue("C1", "2600010.000");
            coordStart.setattrvalue("C2", "1200000.000");
            coordEnd.setattrvalue("C1", "2600010.000");
            coordEnd.setattrvalue("C2", "1200010.000");            
        }
        
        // Run
        ParquetWriter writer = null;
        File file = new File(TEST_OUT,"wkt_multilinestring_Ok.parquet");
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
        assertEquals(record.get("attrMLineString").toString(),"MULTILINESTRING ((2600000 1200000, 2600010 1200000), (2600010 1200000, 2600010 1200010))");
                
        GenericRecord nextRecord = reader.read();
        assertNull(nextRecord);
    }
    
    @Test
    public void wkt_polygon_Ok() throws IoxException, IOException {
        // Prepare
        Iom_jObject inputObj = new Iom_jObject("Test1.Topic1.Obj1", "o1");
        
        IomObject multisurfaceValue = inputObj.addattrobj("attrPolygon", "MULTISURFACE");
        IomObject surfaceValue = multisurfaceValue.addattrobj("surface", "SURFACE");
        {
            IomObject outerBoundary = surfaceValue.addattrobj("boundary", "BOUNDARY");
            // polyline
            IomObject polylineValue = outerBoundary.addattrobj("polyline", "POLYLINE");
            IomObject segments=polylineValue.addattrobj("sequence", "SEGMENTS");
            IomObject startSegment=segments.addattrobj("segment", "COORD");
            startSegment.setattrvalue("C1", "-0.22857142857142854");
            startSegment.setattrvalue("C2", "0.5688311688311687");
            IomObject endSegment=segments.addattrobj("segment", "COORD");
            endSegment.setattrvalue("C1", "-0.15857142857142854");
            endSegment.setattrvalue("C2", "0.5688311688311687");
            // polyline 2
            IomObject polylineValue2 = outerBoundary.addattrobj("polyline", "POLYLINE");
            IomObject segments2=polylineValue2.addattrobj("sequence", "SEGMENTS");
            IomObject startSegment2=segments2.addattrobj("segment", "COORD");
            startSegment2.setattrvalue("C1", "-0.15857142857142854");
            startSegment2.setattrvalue("C2", "0.5688311688311687");
            IomObject endSegment2=segments2.addattrobj("segment", "COORD");
            endSegment2.setattrvalue("C1", "-0.15857142857142854");
            endSegment2.setattrvalue("C2", "0.5888311688311687");
            // polyline 3
            IomObject polylineValue3 = outerBoundary.addattrobj("polyline", "POLYLINE");
            IomObject segments3=polylineValue3.addattrobj("sequence", "SEGMENTS");
            IomObject startSegment3=segments3.addattrobj("segment", "COORD");
            startSegment3.setattrvalue("C1", "-0.15857142857142854");
            startSegment3.setattrvalue("C2", "0.5888311688311687");
            IomObject endSegment3=segments3.addattrobj("segment", "COORD");
            endSegment3.setattrvalue("C1", "-0.22857142857142854");
            endSegment3.setattrvalue("C2", "0.5688311688311687");
        }
    
        // Run
        ParquetWriter writer = null;
        File file = new File(TEST_OUT,"wkt_polygon_Ok.parquet");
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
        assertEquals(record.get("attrPolygon").toString(),"MULTIPOLYGON (((-0.2285714285714285 0.5688311688311687, -0.1585714285714285 0.5688311688311687, -0.1585714285714285 0.5688311688311687, -0.1585714285714285 0.5888311688311687, -0.1585714285714285 0.5888311688311687, -0.2285714285714285 0.5688311688311687)))");
                
        GenericRecord nextRecord = reader.read();
        assertNull(nextRecord);
    }
    
    @Test
    public void wkt_multipolygon_Ok() throws IoxException, IOException {
        // Prepare
        Iom_jObject inputObj = new Iom_jObject("Test1.Topic1.Obj1", "o1");
        
        IomObject multisurfaceValue = inputObj.addattrobj("attrMultiPolygon", "MULTISURFACE");
        IomObject surfaceValue = multisurfaceValue.addattrobj("surface", "SURFACE");
        {
            IomObject outerBoundary = surfaceValue.addattrobj("boundary", "BOUNDARY");
            // polyline
            IomObject polylineValue = outerBoundary.addattrobj("polyline", "POLYLINE");
            IomObject segments = polylineValue.addattrobj("sequence", "SEGMENTS");
            IomObject startSegment = segments.addattrobj("segment", "COORD");
            startSegment.setattrvalue("C1", "-0.228");
            startSegment.setattrvalue("C2", "0.568");
            IomObject endSegment = segments.addattrobj("segment", "COORD");
            endSegment.setattrvalue("C1", "-0.158");
            endSegment.setattrvalue("C2", "0.568");
            // polyline 2
            IomObject polylineValue2 = outerBoundary.addattrobj("polyline", "POLYLINE");
            IomObject segments2 = polylineValue2.addattrobj("sequence", "SEGMENTS");
            IomObject startSegment2 = segments2.addattrobj("segment", "COORD");
            startSegment2.setattrvalue("C1", "-0.158");
            startSegment2.setattrvalue("C2", "0.568");
            IomObject endSegment2 = segments2.addattrobj("segment", "COORD");
            endSegment2.setattrvalue("C1", "-0.158");
            endSegment2.setattrvalue("C2", "0.588");
            // polyline 3
            IomObject polylineValue3 = outerBoundary.addattrobj("polyline", "POLYLINE");
            IomObject segments3 = polylineValue3.addattrobj("sequence", "SEGMENTS");
            IomObject startSegment3 = segments3.addattrobj("segment", "COORD");
            startSegment3.setattrvalue("C1", "-0.158");
            startSegment3.setattrvalue("C2", "0.588");
            IomObject endSegment3 = segments3.addattrobj("segment", "COORD");
            endSegment3.setattrvalue("C1", "-0.228");
            endSegment3.setattrvalue("C2", "0.568");
        }

        IomObject surfaceValue2 = multisurfaceValue.addattrobj("surface", "SURFACE");
        {
            IomObject outerBoundary = surfaceValue2.addattrobj("boundary", "BOUNDARY");
            // polyline
            IomObject polylineValue = outerBoundary.addattrobj("polyline", "POLYLINE");
            IomObject segments = polylineValue.addattrobj("sequence", "SEGMENTS");
            IomObject startSegment = segments.addattrobj("segment", "COORD");
            startSegment.setattrvalue("C1", "0.228");
            startSegment.setattrvalue("C2", "1.300");
            IomObject endSegment = segments.addattrobj("segment", "COORD");
            endSegment.setattrvalue("C1", "0.158");
            endSegment.setattrvalue("C2", "1.568");
            // polyline 2
            IomObject polylineValue2 = outerBoundary.addattrobj("polyline", "POLYLINE");
            IomObject segments2 = polylineValue2.addattrobj("sequence", "SEGMENTS");
            IomObject startSegment2 = segments2.addattrobj("segment", "COORD");
            startSegment2.setattrvalue("C1", "0.158");
            startSegment2.setattrvalue("C2", "1.568");
            IomObject endSegment2 = segments2.addattrobj("segment", "COORD");
            endSegment2.setattrvalue("C1", "0.158");
            endSegment2.setattrvalue("C2", "0.500");
            // polyline 3
            IomObject polylineValue3 = outerBoundary.addattrobj("polyline", "POLYLINE");
            IomObject segments3 = polylineValue3.addattrobj("sequence", "SEGMENTS");
            IomObject startSegment3 = segments3.addattrobj("segment", "COORD");
            startSegment3.setattrvalue("C1", "0.158");
            startSegment3.setattrvalue("C2", "0.500");
            IomObject endSegment3 = segments3.addattrobj("segment", "COORD");
            endSegment3.setattrvalue("C1", "0.228");
            endSegment3.setattrvalue("C2", "1.300");
        }
    
        // Run
        ParquetWriter writer = null;
        File file = new File(TEST_OUT,"wkt_multipolygon_Ok.parquet");
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
        assertEquals(record.get("attrMultiPolygon").toString(),"MULTIPOLYGON (((-0.228 0.568, -0.158 0.568, -0.158 0.568, -0.158 0.588, -0.158 0.588, -0.228 0.568)), ((0.228 1.3, 0.158 1.568, 0.158 1.568, 0.158 0.5, 0.158 0.5, 0.228 1.3)))");
                
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
        File file = new File(TEST_OUT,"attributes_no_description_set_null_value_Ok.parquet");
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

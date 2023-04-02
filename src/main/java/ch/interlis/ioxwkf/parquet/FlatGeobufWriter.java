package ch.interlis.ioxwkf.parquet;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.wololo.flatgeobuf.ColumnMeta;
import org.wololo.flatgeobuf.Constants;
import org.wololo.flatgeobuf.HeaderMeta;
import org.wololo.flatgeobuf.generated.ColumnType;
import org.wololo.flatgeobuf.generated.Feature;
import org.wololo.flatgeobuf.generated.GeometryType;
import org.wololo.flatgeobuf.GeometryConversions;
import org.locationtech.jts.geom.Envelope;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.google.flatbuffers.FlatBufferBuilder;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTWriter;

import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxFactoryCollection;
import ch.interlis.iox.IoxWriter;
import ch.interlis.iox.ObjectEvent;
import ch.interlis.iox.StartBasketEvent;
import ch.interlis.iox.StartTransferEvent;
import ch.interlis.iox_j.jts.Iox2jts;
import ch.interlis.iox_j.jts.Iox2jtsException;

import static java.nio.charset.CodingErrorAction.REPLACE;

public class FlatGeobufWriter implements IoxWriter {
    public static final String FEATURES_COUNT = "ch.interlis.ioxwkf.flatgeobuf.featurescount";

    private OutputStream outputStream;
    private File outputFile;
    private FlatBufferBuilder builder;
    private ParquetWriter<GenericData.Record> writer = null;
    
    private HeaderMeta headerMeta = null;
    
    private Schema schema = null;
    private List<MyAttributeDescriptor> attrDescs = null;

    private TransferDescription td = null;
    private String iliGeomAttrName = null;
    
    private String tableName = null;
    
    // ili types
    private static final String COORD="COORD";
    private static final String MULTICOORD="MULTICOORD";
    private static final String POLYLINE="POLYLINE";
    private static final String MULTIPOLYLINE="MULTIPOLYLINE";
    private static final String MULTISURFACE="MULTISURFACE";

    private Integer srsId = null;
    private Integer defaultSrsId = 2056; // TODO: null
    
    private long featuresCount = 0;

    public FlatGeobufWriter(File file) throws IoxException {
        this(file,null);
    }
    
    public FlatGeobufWriter(File file, Settings settings) throws IoxException { 
        init(file,settings);
    }
    
    private void init(File file, Settings settings) throws IoxException {
        //this.outputStream = new FileOutputStream(file);
        this.outputFile = file;
        this.tableName = file.getName().replace(".fgb", ""); // TODO: ja...        
    }

    @Override
    public void write(IoxEvent event) throws IoxException {
        if(event instanceof StartTransferEvent){
            // ignore
        }else if(event instanceof StartBasketEvent){
        }else if(event instanceof ObjectEvent){
            ObjectEvent obj = (ObjectEvent) event;
            IomObject iomObj = (IomObject)obj.getIomObject();
            String tag = iomObj.getobjecttag();
            System.out.println("tag: " + tag);

            // Wenn null, dann gibt es noch kein "Schema"
//            attrDescsMap
            if(attrDescs == null) {
                attrDescs = new ArrayList<MyAttributeDescriptor>();
               // initAttrDescs(); // TODO ? 
                if(td != null) {
                    // TODO
                } else {
                    for(int u=0;u<iomObj.getattrcount();u++) {
                        String attrName = iomObj.getattrname(u);
                        System.out.println(attrName);
                        //create the builder
//                        AttributeTypeBuilder attributeBuilder = new AttributeTypeBuilder();
                        MyAttributeDescriptor attrDesc = new MyAttributeDescriptor();

                        //if(attrName.equals(iliGeomAttrName)) {
                        if(attrName.equals("gaga")) {

//                            iliGeomAttrName=attrName;
//                            IomObject iomGeom=iomObj.getattrobj(attrName,0);
//                            if (iomGeom != null){
//                                if (iomGeom.getobjecttag().equals(COORD)){
//                                    attributeBuilder.setBinding(Point.class);
//                                }else if (iomGeom.getobjecttag().equals(MULTICOORD)){
//                                    attributeBuilder.setBinding(MultiPoint.class);
//                                }else if(iomGeom.getobjecttag().equals(POLYLINE)){
//                                    attributeBuilder.setBinding(LineString.class);
//                                }else if (iomGeom.getobjecttag().equals(MULTIPOLYLINE)){
//                                    attributeBuilder.setBinding(MultiLineString.class);
//                                }else if (iomGeom.getobjecttag().equals(MULTISURFACE)){
//                                    int surfaceCount=iomGeom.getattrvaluecount("surface");
//                                    if(surfaceCount<=1) {
//                                        /* Weil der Featuretype (das Schema) des Shapefiles anhand des ersten IomObjektes erstellt wird, 
//                                         * kann es vorkommen, dass Multisurfaces mit mehr als einer Surface nicht zu einem Multipolygon umgewandelt werden, 
//                                         * sondern zu einem Polygon. Aus diesem Grund wird immer das MultiPolygon-Binding verwendet. */
//                                        attributeBuilder.setBinding(MultiPolygon.class);
//                                    }else if(surfaceCount>1){
//                                        attributeBuilder.setBinding(MultiPolygon.class);
//                                    }
//                                }else {
//                                    attributeBuilder.setBinding(Point.class);
//                                }
//                                if(defaultSrsId!=null) {
//                                    attributeBuilder.setCRS(createCrs(defaultSrsId));
//                                }
//                            }
                        } else {                                                        
                            // Ist das nicht relativ heikel?
                            // Funktioniert mit Strukturen nicht mehr, oder? Wegen getattrvaluecount?
                            // TODO: testen
                            if(iliGeomAttrName==null && iomObj.getattrvaluecount(attrName)>0 && iomObj.getattrobj(attrName,0) != null) {
                                iliGeomAttrName = attrName;
                                System.out.println("geometry found");
                                IomObject iomGeom = iomObj.getattrobj(attrName,0);
                                if (iomGeom != null) {
                                    if (iomGeom.getobjecttag().equals(COORD)){
                                        attrDesc.setBinding(Point.class);
                                    }else if (iomGeom.getobjecttag().equals(MULTICOORD)){
                                        attrDesc.setBinding(MultiPoint.class);
                                    }else if(iomGeom.getobjecttag().equals(POLYLINE)){
                                        attrDesc.setBinding(LineString.class);
                                    }else if (iomGeom.getobjecttag().equals(MULTIPOLYLINE)){
                                        attrDesc.setBinding(MultiLineString.class);
                                    }else if (iomGeom.getobjecttag().equals(MULTISURFACE)){
                                        int surfaceCount=iomGeom.getattrvaluecount("surface");
                                        if(surfaceCount==1) {
                                            /* Weil das "Schema" anhand des ersten IomObjektes erstellt wird, 
                                             * kann es vorkommen, dass Multisurfaces mit mehr als einer Surface nicht zu einem Multipolygon umgewandelt werden, 
                                             * sondern zu einem Polygon. Aus diesem Grund wird immer das MultiPolygon-Binding verwendet. */
                                            attrDesc.setBinding(MultiPolygon.class);
                                        }else if(surfaceCount>1){
                                            attrDesc.setBinding(MultiPolygon.class);
                                        }
                                    } else {
                                        attrDesc.setBinding(Point.class);
                                    }
                                    if(defaultSrsId != null) {
                                        attrDesc.setSrId(defaultSrsId);
                                    }
                                    attrDesc.setGeometry(true);
                                }
                            } else {
                                attrDesc.setBinding(String.class);

//                                ColumnMeta column = new ColumnMeta();
//                                column.name = attrName;
//                                column.type = ColumnType.String;
//                                columns.add(column);

                                
                            }
                        }
                        attrDesc.setAttributeName(attrName);
                        attrDescs.add(attrDesc);

                        
//                        attributeBuilder.setName(attrName);
//                        attributeBuilder.setMinOccurs(0);
//                        attributeBuilder.setMaxOccurs(1);
//                        attributeBuilder.setNillable(true);
//                        //build the descriptor
//                        String trimmedAttrName = trimAttributeName(attrName,9);
//                        AttributeDescriptor descriptor = attributeBuilder.buildDescriptor(trimmedAttrName);                            
//                        addAttrDesc(attrName, descriptor);  
                    }
                }
            }
            if (schema == null) {
                schema = createSchema();
                
                Path path = new Path(outputFile.getAbsolutePath());
                try {
                    writer = AvroParquetWriter.<GenericData.Record>builder(path)
                            .withSchema(schema)
                            .withCompressionCodec(CompressionCodecName.SNAPPY)
                            .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                            .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                            .withConf(new Configuration())
                            .withValidation(false)
                            .withDictionaryEncoding(false)
                            .build();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new IoxException(e.getMessage());
                }
            }
            

//                for (GenericData.Record record : recordList) {
//                    writer.write(record);
//                }
                
            GenericData.Record record = null;
            try {
                record = generateRecord(iomObj, schema);
            } catch (Iox2jtsException e) {
                e.printStackTrace();
                throw new IoxException(e.getMessage());
            }
            System.out.println(record.toString());
            try {
                writer.write(record);
            } catch (IOException e) {
                e.printStackTrace();
                throw new IoxException(e.getMessage());
            }

        }
    }

    private GenericData.Record generateRecord(IomObject iomObj, Schema schema) throws Iox2jtsException {
        GenericData.Record record = new GenericData.Record(schema);

        Iterator<Field> fieldi = schema.getFields().iterator();
        while (fieldi.hasNext()) {
            Field field = fieldi.next();
            String attrName = field.name();
            String attrValue = null;

            String geomType = field.getProp("geomtype");
            
            if (geomType != null && geomType.equalsIgnoreCase(this.COORD)) {
                Coordinate geom = Iox2jts.coord2JTS(iomObj.getattrobj(attrName, 0));
                attrValue = WKTWriter.toPoint(geom);
            }
        
            else {
                attrValue = iomObj.getattrvalue(attrName);
            }
                       
            record.put(attrName, attrValue);
        }
        
        return record;
    }
    
    private Schema createSchema() {
        ObjectNode rootNode = JsonNodeFactory.instance.objectNode();
        
        JsonNode namespaceNode = JsonNodeFactory.instance.textNode("ch.so.agi.ioxwkf.parquet");
        rootNode.set("namespace", namespaceNode);

        JsonNode typeNode = JsonNodeFactory.instance.textNode("record");
        rootNode.set("type", typeNode);
        
        JsonNode nameNode = JsonNodeFactory.instance.textNode("myrecordname"); // TODO ist aber egal
        rootNode.set("name", nameNode);

        ArrayNode fieldsNode = JsonNodeFactory.instance.arrayNode();
        for (MyAttributeDescriptor attrDesc : attrDescs) {
            ObjectNode fieldNode = JsonNodeFactory.instance.objectNode();

            JsonNode fieldNameNode = JsonNodeFactory.instance.textNode(attrDesc.getAttributeName()); 
            fieldNode.set("name", fieldNameNode);

            if (attrDesc.isGeometry()) {
                fieldNode.set("type", getStringType(false));
                                
                if (attrDesc.getBinding() == Point.class) {
                    fieldNode.set("geomtype", JsonNodeFactory.instance.textNode(this.COORD));
                } 
                // else if ... Eigentlich müsste ich den Umweg über ein generische AttrDesc gar nicht gehen und könnten gleich das Schema zusammensuchen, oder?
                // Oder es muss eine Liste von Fields sein. Glaubs...
  
            } else {
                if (attrDesc.getBinding() == String.class) {
                    fieldNode.set("type", getStringType(false));
                }
            }

            fieldsNode.add(fieldNode);
        }
        rootNode.set("fields", fieldsNode);
        
        Schema.Parser parser = new Schema.Parser().setValidate(true);
        return parser.parse(rootNode.toString());
    }
    
    // TODO wohin? getType? getXXXType?
    
    private JsonNode getStringType(boolean isMandatory) {
        ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
        arrayNode.add("string");
        if (!isMandatory) {
            arrayNode.add("null");
        }
        return arrayNode;
    }
        
    @Override
    public void close() throws IoxException {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new IoxException(e.getMessage());
        }
    }

    @Override
    public IomObject createIomObject(String arg0, String arg1) throws IoxException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void flush() throws IoxException {
        // TODO Auto-generated method stub

    }

    @Override
    public IoxFactoryCollection getFactory() throws IoxException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setFactory(IoxFactoryCollection arg0) throws IoxException {
        // TODO Auto-generated method stub

    }


}

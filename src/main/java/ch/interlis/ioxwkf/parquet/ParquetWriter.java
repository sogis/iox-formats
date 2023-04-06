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

import org.locationtech.jts.geom.Envelope;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
//import org.apache.parquet.hadoop.ParquetWriter;
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

public class ParquetWriter implements IoxWriter {
    private File outputFile;
    private org.apache.parquet.hadoop.ParquetWriter<GenericData.Record> writer = null;
        
    private Schema schema = null;
    private List<ParquetAttributeDescriptor> attrDescs = null;

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

    public ParquetWriter(File file) throws IoxException {
        this(file,null);
    }
    
    public ParquetWriter(File file, Settings settings) throws IoxException { 
        init(file,settings);
    }
    
    private void init(File file, Settings settings) throws IoxException {
        //this.outputStream = new FileOutputStream(file);
        this.outputFile = file;
    }
    
    public void setAttributeDescriptors(List<ParquetAttributeDescriptor> attrDescs) {
        this.attrDescs = attrDescs;
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
                attrDescs = new ArrayList<ParquetAttributeDescriptor>();
               // initAttrDescs(); // TODO ? 
                if(td != null) {
                    // TODO
                } else {
                    for(int u=0;u<iomObj.getattrcount();u++) {
                        String attrName = iomObj.getattrname(u);
                        System.out.println(attrName);
                        //create the builder
//                        AttributeTypeBuilder attributeBuilder = new AttributeTypeBuilder();
                        ParquetAttributeDescriptor attrDesc = new ParquetAttributeDescriptor();

                        // Für was brauche ich dieses if/else? Verstehe es nicht.
                        // Bei GPKG scheint es das nicht zu geben.
                        
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
                            // Es wurde weder ein Modell gesetzt noch wurde das Schema
                            // mittel setAttrDescs definiert. -> Es wird aus dem ersten IomObject
                            // das Zielschema möglichst gut definiert. 
                            // Nachteile:
                            // - Geometrie aus Struktur eruieren ... siehe Kommentar wegen anderen Strukturen. Kann eventuell abgefedert werden.
                            // - Wenn das erste Element fehlende Attribute hat (also NULL-Werte) gehen diese Attribute bei der Schemadefinition
                            // verloren.
                            
                            
                            // TODO: Eigentlich könnte ich gleich das Schema machen, aber da diese nicht so gleich gut geht wie bei Geotools-Shapefile,
                            // doch eher Umweg über einen AttributeDescriptor?!
                            
                            
                            // TODO: Umgang mit mehreren Geometrien klären. Sowieso: muss ich im non-geo-parquet iliGeomAttrName mitschleppen?
                            
                            // Ist das nicht relativ heikel?
                            // Funktioniert mit Strukturen nicht mehr, oder? Wegen getattrvaluecount?
                            // TODO: testen
                            //if (iliGeomAttrName==null && iomObj.getattrvaluecount(attrName)>0 && iomObj.getattrobj(attrName,0) != null) {
                            if (iomObj.getattrvaluecount(attrName)>0 && iomObj.getattrobj(attrName,0) != null) {
                                //iliGeomAttrName = attrName;
                                System.out.println("geometry found");
                                IomObject iomGeom = iomObj.getattrobj(attrName,0);
                                if (iomGeom != null) {
                                    if (iomGeom.getobjecttag().equals(COORD)) {
                                        attrDesc.setBinding(Point.class);
                                    } else if (iomGeom.getobjecttag().equals(MULTICOORD)) {
                                        attrDesc.setBinding(MultiPoint.class);
                                    } else if (iomGeom.getobjecttag().equals(POLYLINE)) {
                                        attrDesc.setBinding(LineString.class);
                                    } else if (iomGeom.getobjecttag().equals(MULTIPOLYLINE)) {
                                        attrDesc.setBinding(MultiLineString.class);
                                    } else if (iomGeom.getobjecttag().equals(MULTISURFACE)) {
                                        int surfaceCount=iomGeom.getattrvaluecount("surface");
                                        if(surfaceCount==1) {
                                            /* Weil das "Schema" anhand des ersten IomObjektes erstellt wird, 
                                             * kann es vorkommen, dass Multisurfaces mit mehr als einer Surface nicht zu einem Multipolygon umgewandelt werden, 
                                             * sondern zu einem Polygon. Aus diesem Grund wird immer das MultiPolygon-Binding verwendet. */
                                            attrDesc.setBinding(MultiPolygon.class);
                                        } else if (surfaceCount>1) {
                                            attrDesc.setBinding(MultiPolygon.class);
                                        }
                                    } else {
                                        // Siehe Kommentar oben. Ist das sinnvoll? Resp funktioniert das wenn es andere Strukturen gibt? Diese könnten man nach JSON 
                                        // umwandeln und als String behandeln.
                                        // Was passiert in der Logik, falls keine Geometrie gesetzt ist? 
                                        
                                        attrDesc.setBinding(Point.class);
                                    }
                                    if (defaultSrsId != null) {
                                        attrDesc.setSrId(defaultSrsId);
                                    }
                                    attrDesc.setGeometry(true);
                                }
                            } else {
                                attrDesc.setBinding(String.class);
                            }
                        }
                        attrDesc.setAttributeName(attrName);
                        attrDescs.add(attrDesc);
                    }
                }
            }
            if (schema == null) {
                schema = createSchema(attrDescs);
                
                System.out.println(schema);
                
                Path path = new Path(outputFile.getAbsolutePath());
                try {
                    writer = AvroParquetWriter.<GenericData.Record>builder(path)
                            .withSchema(schema)
                            .withCompressionCodec(CompressionCodecName.SNAPPY) // TODO was ist gut? Snappy ist was "natives" (ähnlich wie sqlite).
                            .withRowGroupSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE)
                            .withPageSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE)
                            .withConf(new Configuration())
                            .withValidation(false)
                            .withDictionaryEncoding(false)
                            .build();
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new IoxException(e.getMessage());
                }
            }
     
            GenericData.Record record = null;
            try {
                record = generateRecord(iomObj, schema);
            } catch (Iox2jtsException e) {
                e.printStackTrace();
                throw new IoxException(e.getMessage());
            }
//            System.out.println(record.toString());
            try {
                writer.write(record);
            } catch (IOException e) {
                e.printStackTrace();
                throw new IoxException(e.getMessage());
            }

        }
    }

    // Beim Shapefile ist die Schlaufe über attrDescs
    // Hier aber glaubs ok über schema.
    // MMMMh, wird kompliziert bei den normalen Datentypen. Schon nullable int ist ne Liste / Union.
    // bei den logical types gute Nacht.
    // Würde wohl auch mein Polygon- vs MultiPolygon-Problem lösen.
    
    
    private GenericData.Record generateRecord(IomObject iomObj, Schema schema) throws Iox2jtsException {
        GenericData.Record record = new GenericData.Record(schema);

        Iterator<Field> fieldi = schema.getFields().iterator();
        while (fieldi.hasNext()) {
            Field field = fieldi.next();
            String attrName = field.name();
            String attrValue = null;
          
            String geomType = field.getProp("geomtype");
            System.out.println("geomType: " + geomType);
            System.out.println("schema: " + field.schema());
            
            if (geomType != null) {
                if (geomType.equalsIgnoreCase(this.COORD)) {
                    Coordinate geom = Iox2jts.coord2JTS(iomObj.getattrobj(attrName, 0));
                    attrValue = WKTWriter.toPoint(geom); 
                } else if (geomType.equalsIgnoreCase(this.MULTICOORD)) { 
                    MultiPoint geom = Iox2jts.multicoord2JTS(iomObj.getattrobj(attrName, 0));
                    attrValue = geom.toText();                    
                } else if (geomType.equalsIgnoreCase(this.POLYLINE)) {
                    LineString geom = Iox2jts.polyline2JTSlineString(iomObj.getattrobj(attrName, 0), false, 0);
                    attrValue = geom.toText();
                } else if (geomType.equalsIgnoreCase(this.MULTIPOLYLINE)) {
                    MultiLineString geom = Iox2jts.multipolyline2JTS(iomObj.getattrobj(attrName, 0), 0);
                    attrValue = geom.toText();
                } else if (geomType.equalsIgnoreCase(this.MULTISURFACE)) {
                    // TODO
                    // Mir nicht ganz klar wie man Polygon/Multipolygon handelt.
                    // Bei den Bindings kann man den Unterschied machen (im String-Modus aber auch nicht).
                    MultiPolygon geom = Iox2jts.multisurface2JTS(iomObj.getattrobj(attrName, 0), 0, 2056); 
                    attrValue = geom.toText();
                }
            }
            else {
                
                // TODO cast error.
                // einfach machbar via Schema? Einfacher wohl wieder mit den attrDesc.
                
                attrValue = iomObj.getattrvalue(attrName);
            }

            record.put(attrName, attrValue);
        }
        
        return record;
    }
    
    private Schema createSchema(List<ParquetAttributeDescriptor> attrDescs) {
        Schema schema = Schema.createRecord("myrecordname", null, "ch.so.agi.ioxwkf.parquet", false);
        List<Schema.Field> fields = new ArrayList<>();

        for (ParquetAttributeDescriptor attrDesc : attrDescs) {
            if (attrDesc.isGeometry()) {
                Field field = new Schema.Field(attrDesc.getAttributeName(), Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)), null, null);

                // Das non-geo-parquet müsste im Schema den Geometrietyp nicht kennen.
                // Die Umwandlung der Iox-Geometrie könnte anhand der Tags geschehen.
                // Eigentlich ist die ganze Unterscheidung der Geometrien momentan
                // gar nicht nötig (Gedanken nicht zu Ende geprüft).
                // Für GeoParquet sehe es wohl wieder anders aus.
                if (attrDesc.getBinding() == Point.class) {
                    field.addProp("geomtype", JsonNodeFactory.instance.textNode(this.COORD));
                } else if (attrDesc.getBinding() == MultiPoint.class)  {
                    field.addProp("geomtype", JsonNodeFactory.instance.textNode(this.MULTICOORD));
                } else if (attrDesc.getBinding() == LineString.class) {
                    field.addProp("geomtype", JsonNodeFactory.instance.textNode(this.POLYLINE));
                } else if (attrDesc.getBinding() == MultiLineString.class) {
                    field.addProp("geomtype", JsonNodeFactory.instance.textNode(this.MULTIPOLYLINE));
                } else if (attrDesc.getBinding() == Polygon.class) {
                    field.addProp("geomtype", JsonNodeFactory.instance.textNode(this.MULTISURFACE));
                } else if (attrDesc.getBinding() == MultiPolygon.class) {
                    field.addProp("geomtype", JsonNodeFactory.instance.textNode(this.MULTISURFACE)); 
                } 
                fields.add(field);
            } else {
                Field field = null;
                if (attrDesc.getBinding() == String.class) {
                    field = new Schema.Field(attrDesc.getAttributeName(), Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)), null, null);
                    //field = new Schema.Field(attrDesc.getAttributeName(), Schema.create(Schema.Type.STRING), null, null);
                } else if (attrDesc.getBinding() == Integer.class) {
                    field = new Schema.Field(attrDesc.getAttributeName(), Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL)), null, null);
                }

                
                
                fields.add(field);
            }
        }        
        schema.setFields(fields);
        return schema;
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

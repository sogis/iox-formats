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

import com.google.flatbuffers.FlatBufferBuilder;

import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

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

import static java.nio.charset.CodingErrorAction.REPLACE;

public class FlatGeobufWriter implements IoxWriter {
    public static final String FEATURES_COUNT = "ch.interlis.ioxwkf.flatgeobuf.featurescount";

    private OutputStream outputStream;
    private FlatBufferBuilder builder;
    
    private HeaderMeta headerMeta = null;
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
        try {
            this.outputStream = new FileOutputStream(file);
            this.tableName = file.getName().replace(".fgb", ""); // TODO: ja...
        } catch (FileNotFoundException e) {
            throw new IoxException(e);
        }
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


            
            

            
            
//            if(featureType==null) {
//                featureType=createFeatureType(attrDescs);
//                featureBuilder = new SimpleFeatureBuilder(featureType);
//                try {
//                    dataStore.createSchema(featureType);
//                    
//                    String typeName = dataStore.getTypeNames()[0];
//                    
//                    transaction = new DefaultTransaction(
//                            "create");
//
//                    writer = dataStore.getFeatureWriter(typeName, transaction);
//                } catch (IOException e) {
//                    throw new IoxException(e);
//                }
//            }
//            // write object attribute-values of model attribute-names
//            try {
//                SimpleFeature feature=convertObject(iomObj);
//
//                writeFeatureToShapefile(feature);
//            } catch (IOException e) {
//                throw new IoxException("failed to write object "+iomObj.getobjecttag(),e);
//            } catch (Iox2jtsException e) {
//                throw new IoxException("failed to convert "+iomObj.getobjecttag()+" in jts",e);
//            }
 
            
        }
    }

    private static void buildPropertiesVector(
            IomObject iomObj, HeaderMeta headerMeta, ByteBuffer target) {

        target.order(ByteOrder.LITTLE_ENDIAN);
        for (short i = 0; i < headerMeta.columns.size(); i++) {
            ColumnMeta column = headerMeta.columns.get(i);
            byte type = column.type;
            Object value = iomObj.getattrvalue(column.name);
            if (value == null) {
                continue;
            }
            target.putShort(i);
            if (type == ColumnType.Bool) {
                target.put((byte) ((boolean) value ? 1 : 0));
            } else if (type == ColumnType.Byte) {
                target.put((byte) value);
            } else if (type == ColumnType.Short) {
                target.putShort((short) value);
            } else if (type == ColumnType.Int) {
                target.putInt((int) value);
            } else if (type == ColumnType.Long)
                if (value instanceof Long) {
                    target.putLong((long) value);
                } else if (value instanceof BigInteger) {
                    target.putLong(((BigInteger) value).longValue());
                } else {
                    target.putLong((long) value);
                }
            else if (type == ColumnType.Double)
                if (value instanceof Double) {
                    target.putDouble((double) value);
                } else if (value instanceof BigDecimal) {
                    target.putDouble(((BigDecimal) value).doubleValue());
                } else {
                    target.putDouble((double) value);
                }
            else if (type == ColumnType.DateTime) {
                String isoDateTime = "";
                if (value instanceof LocalDateTime) {
                    isoDateTime = ((LocalDateTime) value).toString();
                } else if (value instanceof LocalDate) {
                    isoDateTime = ((LocalDate) value).toString();
                } else if (value instanceof LocalTime) {
                    isoDateTime = ((LocalTime) value).toString();
                } else if (value instanceof OffsetDateTime) {
                    isoDateTime = ((OffsetDateTime) value).toString();
                } else if (value instanceof OffsetTime) {
                    isoDateTime = ((OffsetTime) value).toString();
                } else {
                    throw new RuntimeException("Unknown date/time type " + type);
                }
                writeString(target, isoDateTime);
            } else if (type == ColumnType.String) {
                writeString(target, (String) value);
            } else {
                throw new RuntimeException("Unknown type " + type);
            }
        }
    }

    private static void writeString(ByteBuffer target, String value) {

        CharsetEncoder encoder =
                StandardCharsets.UTF_8
                        .newEncoder()
                        .onMalformedInput(REPLACE)
                        .onUnmappableCharacter(REPLACE);

        // save current position to write the string length later
        final int lengthPosition = target.position();
        // and leave room for it
        target.position(lengthPosition + Integer.BYTES);

        final int startStrPos = target.position();
        final boolean endOfInput = true;
        encoder.encode(CharBuffer.wrap(value), target, endOfInput);

        final int endStrPos = target.position();
        final int encodedLength = endStrPos - startStrPos;

        // absolute put, doesn't change the current position
        target.putInt(lengthPosition, encodedLength);
    }

    // AttributeDescriptor ist im dbtools-Package. Es dient dort unter anderem,
    // um die Metainformationen der zu exportierenden Spalten/Attribute 
    // festzustellen. Es ist aber hardcodiert für PostGIS. Wenn man z.B. 
    // von Geopackage exportieren will, funktionieren einige Methoden nicht.
    // Oder wenn man beliebig (ohne Modell) via IOX Formate umwandeln will.
    // Sollte man es nicht trennen? Das Holen der Informationen aus der Quelle
    // und das Speichern der Spalten-Infos? Oder einen AttributeDescriptor pro
    // Format? Mmmmh was ist aber mit dem Mapping? Wo findet das dann statt?
    // Oder ist AttributeDescriptor etwas agnostisches? Und das Mapping findet
    // immer innerhalb einer IoxWriter-Klasse statt (beim expliziten Erstellen eines
    // spezifischen "Schema"-Objektes.
    // TODO 
//    public void setAttributeDescriptors(AttributeDescriptor attrDescs[]) {
//        
//    }
    
    public void setDefaultSridCode(String sridCode) {
        defaultSrsId = Integer.parseInt(sridCode);
    }
    
    // Wird wahrscheinlich vom Format gebraucht, um schlau zu sein.
    public void setFeaturesCount(long featuresCount) {
        this.featuresCount = featuresCount;
    }
    

    @Override
    public void close() throws IoxException {
        // TODO Auto-generated method stub

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

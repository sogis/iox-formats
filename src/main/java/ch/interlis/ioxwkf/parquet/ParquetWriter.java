package ch.interlis.ioxwkf.parquet;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.LocalTimestampMillis;
import org.apache.avro.LogicalTypes.TimeMillis;
import org.apache.avro.LogicalTypes.TimestampMillis;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTWriter;

import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.generator.XSDGenerator;
import ch.interlis.ili2c.metamodel.LocalAttribute;
import ch.interlis.ili2c.metamodel.NumericType;
import ch.interlis.ili2c.metamodel.NumericalType;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.ili2c.metamodel.Viewable;
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

public class ParquetWriter implements IoxWriter {
    private File outputFile;
    private org.apache.parquet.hadoop.ParquetWriter<GenericData.Record> writer = null;

    private Schema schema = null;
    private List<ParquetAttributeDescriptor> attrDescs = null;

    private TransferDescription td = null;
    private String iliGeomAttrName = null;


    final long NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1);
    final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
    final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

    // ili types
    private static final String COORD="COORD";
    private static final String MULTICOORD="MULTICOORD";
    private static final String POLYLINE="POLYLINE";
    private static final String MULTIPOLYLINE="MULTIPOLYLINE";
    private static final String MULTISURFACE="MULTISURFACE";

    private Integer srsId = null;
    private Integer defaultSrsId = 2056; // TODO: null

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

    public void setModel(TransferDescription td) {
        this.td = td;
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
            IomObject iomObj = obj.getIomObject();
            String tag = iomObj.getobjecttag();

            // Wenn null, dann gibt es noch kein "Schema".
            if(attrDescs == null) {
                attrDescs = new ArrayList<>();
                if(td != null) {
                    Viewable aclass = (Viewable) XSDGenerator.getTagMap(td).get(tag);
                    if (aclass == null) {
                        throw new IoxException("class "+iomObj.getobjecttag()+" not found in model");
                    }
                    Iterator viewableIter = aclass.getAttributes();
                    while(viewableIter.hasNext()) {
                        ParquetAttributeDescriptor attrDesc = new ParquetAttributeDescriptor();

                        Object attrObj = viewableIter.next();
                        System.out.println(attrObj);

                        if(attrObj instanceof LocalAttribute) {
                            LocalAttribute localAttr = (LocalAttribute)attrObj;
                            String attrName = localAttr.getName();
                            System.out.println(attrName);
                            attrDesc.setAttributeName(attrName);

                            // TODO Geometriedinger
                            ch.interlis.ili2c.metamodel.Type iliType = localAttr.getDomainResolvingAliases();
                            if (iliType instanceof ch.interlis.ili2c.metamodel.NumericalType) {
                                NumericalType numericalType = (NumericalType)iliType;
                                NumericType numericType = (NumericType)numericalType;
                                int precision = numericType.getMinimum().getAccuracy(); 
                                if (precision > 0) {
                                    attrDesc.setBinding(Double.class);
                                } else {
                                    attrDesc.setBinding(Integer.class);
                                }
                                attrDescs.add(attrDesc);
                            } else {
                                if (localAttr.isDomainBoolean()) {
                                    attrDesc.setBinding(Boolean.class);
                                    attrDescs.add(attrDesc);
                                } else if (localAttr.isDomainIli2Date()) {
                                    attrDesc.setBinding(LocalDate.class);
                                    attrDescs.add(attrDesc);
                                } else if (localAttr.isDomainIli2DateTime()) {
                                    attrDesc.setBinding(LocalDateTime.class);
                                    attrDescs.add(attrDesc);
                                } else if (localAttr.isDomainIli2Time()) {
                                    attrDesc.setBinding(LocalTime.class);
                                    attrDescs.add(attrDesc);
                                } else {
                                    attrDesc.setBinding(String.class);
                                    attrDescs.add(attrDesc);
                                }
                            }
                        }
                    }
                } else {
                    for(int u=0;u<iomObj.getattrcount();u++) {
                        String attrName = iomObj.getattrname(u);
                        System.out.println(attrName);
                        //create the builder
                        ParquetAttributeDescriptor attrDesc = new ParquetAttributeDescriptor();

                        // Es wurde weder ein Modell gesetzt noch wurde das Schema
                        // mittel setAttrDescs definiert. -> Es wird aus dem ersten IomObject
                        // das Zielschema möglichst gut definiert.
                        // Nachteile:
                        // - Geometrie aus Struktur eruieren ... siehe Kommentar wegen anderen Strukturen. Kann eventuell abgefedert werden.
                        // - Wenn das erste Element fehlende Attribute hat (also NULL-Werte) gehen diese Attribute bei der Schemadefinition
                        // verloren.

                        // Ist das nicht relativ heikel?
                        // Funktioniert mit mehr, wenn es andere Strukturen gibt, oder? Wegen getattrvaluecount?
                        // TODO: testen
                        if (iomObj.getattrvaluecount(attrName)>0 && iomObj.getattrobj(attrName,0) != null) {
//                            System.out.println("geometry found");
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
                                    // Siehe Kommentar oben. Ist das sinnvoll? Resp. funktioniert das wenn es andere Strukturen gibt? Diese könnte man nach JSON
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
                        attrDesc.setAttributeName(attrName);
                        attrDescs.add(attrDesc);
                    }
                }
            }
            if (schema == null) {
                schema = createSchema(attrDescs);

                Path path = new Path(outputFile.getAbsolutePath());
                try {
                    Configuration conf = new Configuration();
                    writer = AvroParquetWriter.<GenericData.Record>builder(path)
                            .withSchema(schema)
                            .withCompressionCodec(CompressionCodecName.SNAPPY) // TODO was ist gut? Snappy ist was "natives" (ähnlich wie sqlite).
                            .withRowGroupSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE)
                            .withPageSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE)
                            .withConf(conf)
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

        for (ParquetAttributeDescriptor attrDesc : attrDescs) {
            String attrName = attrDesc.getAttributeName();
            Object attrValue = null;
            if (attrDesc.getBinding() == Point.class) {
                if ((iomObj.getattrobj(attrName, 0)!=null)) {
                    Coordinate geom = Iox2jts.coord2JTS(iomObj.getattrobj(attrName, 0));
                    attrValue = WKTWriter.toPoint(geom);                    
                }
            } else if (attrDesc.getBinding() == MultiPoint.class) {
                if ((iomObj.getattrobj(attrName, 0)!=null)) {
                    MultiPoint geom = Iox2jts.multicoord2JTS(iomObj.getattrobj(attrName, 0));
                    attrValue = geom.toText();
                }
            } else if (attrDesc.getBinding() == LineString.class) {
                if ((iomObj.getattrobj(attrName, 0)!=null)) {
                    LineString geom = Iox2jts.polyline2JTSlineString(iomObj.getattrobj(attrName, 0), false, 0);
                    attrValue = geom.toText();
                }
            } else if (attrDesc.getBinding() == MultiLineString.class) {
                if ((iomObj.getattrobj(attrName, 0)!=null)) {
                    MultiLineString geom = Iox2jts.multipolyline2JTS(iomObj.getattrobj(attrName, 0), 0);
                    attrValue = geom.toText();                    
                }
            } else if (attrDesc.getBinding() == Polygon.class) {
                if ((iomObj.getattrobj(attrName, 0)!=null)) {
                    Polygon geom = Iox2jts.surface2JTS(iomObj.getattrobj(attrName, 0), 0);
                    attrValue = geom.toText();
                }
            } else if (attrDesc.getBinding() == MultiPolygon.class) {
                if ((iomObj.getattrobj(attrName, 0)!=null)) {
                    MultiPolygon geom = Iox2jts.multisurface2JTS(iomObj.getattrobj(attrName, 0), 0, 2056);
                    attrValue = geom.toText();                
                }
            } else if (attrDesc.getBinding() == String.class) {
                attrValue = iomObj.getattrvalue(attrName);
            } else if (attrDesc.getBinding() == Integer.class) {
                if (iomObj.getattrvalue(attrName) != null) {
                    attrValue = Integer.valueOf(iomObj.getattrvalue(attrName)).intValue();                    
                }
            } else if (attrDesc.getBinding() == Double.class) {
                if (iomObj.getattrvalue(attrName) != null) {
                    attrValue = Double.valueOf(iomObj.getattrvalue(attrName)).doubleValue();
                }
            } else if (attrDesc.getBinding() == LocalDate.class) {
                if (iomObj.getattrvalue(attrName) != null) {
                    LocalDate localDate = LocalDate.parse(iomObj.getattrvalue(attrName), DateTimeFormatter.ISO_LOCAL_DATE);
                    attrValue = Integer.valueOf((int) localDate.toEpochDay());
                }
            } else if (attrDesc.getBinding() == LocalDateTime.class) {
                // https://stackoverflow.com/questions/75970956/write-parquet-file-with-local-timestamp-with-avro-schema
                // Ich rechne alle Datetimes auf UTC zurück, in der Annahme, dass das Datetime im "systemDefault"
                // vorliegt.
                // Apache Drill muss dann aber im mit der UTC-Zeitzone gestartet werden. Siehe drill-env.sh: export DRILL_JAVA_OPTS="$DRILL_JAVA_OPTS -Duser.timezone=UTC"
                // Komisch ist einfach, dass Beispiel-Parquet-Files aus dem Apache Drill Quellcode Timestamps ohne TZ haben, z.B. timestamp-table.parquet.
                // Es liegt an der parquet-Lib genauer glaub an parquet-avro, die die local-Varianten nicht unterstützt. Das Beispiel von apache-drill ist 
                // wohl nicht mit dieser Lib erstellt worden.
//                System.out.println("*****"+attrName);
//                System.out.println("*****"+iomObj.getattrvalue(attrName));
                if (iomObj.getattrvalue(attrName) != null) {
                    LocalDateTime localDateTime = LocalDateTime.parse(iomObj.getattrvalue(attrName));
                    long offset = ChronoUnit.MILLIS.between(localDateTime.atZone(ZoneId.systemDefault()),localDateTime.atZone(ZoneOffset.UTC));
//                    System.out.println(offset);
                    attrValue = Long.valueOf(localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli()) - offset;
//                    System.out.println(attrValue);
                }
                
                // FIXME
//                attrValue = localDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
//                System.out.println(attrValue);
            } else if (attrDesc.getBinding() == LocalTime.class) {
                if (iomObj.getattrvalue(attrName) != null) {
                    LocalTime localTime = LocalTime.parse(iomObj.getattrvalue(attrName));
//                  System.out.println("localTime" + localTime);
                  
                  LocalDateTime localDateTime = LocalDateTime.parse("1970-01-01T12:00:00");
                  long offset = ChronoUnit.MILLIS.between(localDateTime.atZone(ZoneId.systemDefault()),localDateTime.atZone(ZoneOffset.UTC));
                  int milliOfDay = (int) (localTime.toNanoOfDay() / 1_000_000);
                  attrValue = milliOfDay - offset;
                }
                // Auch wieder schön mühsam.
                // Damit daylight saving time nicht noch reinspielt, wird für die Berechnung des Offsets der Januar verwendet.
                // Dafür funktioniert DuckDB nicht. Mit Microsekunden würde es funktionieren. Dann aber Apache Drill nicht.
            } else if (attrDesc.getBinding() == Boolean.class) {
                if (iomObj.getattrvalue(attrName) != null) {
                    attrValue = Boolean.parseBoolean(iomObj.getattrvalue(attrName));                    
                }
            }
            else {
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
            Field field = null;
            if (attrDesc.isGeometry()) {
                field = new Schema.Field(attrDesc.getAttributeName(), Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)), null, null);
                // Mehr braucht es momentan nicht. Beim Record herstellen, loope ich nochmals über die AttributeDescriptions. Dort habe ich und brauche ich das Wissen über
                // den Geometrietyp für die Umwandlung nach WKT.
            } else if (attrDesc.getBinding() == String.class) {
                field = new Schema.Field(attrDesc.getAttributeName(), Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)), null, null);
            } else if (attrDesc.getBinding() == Integer.class) {
                field = new Schema.Field(attrDesc.getAttributeName(), Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL)), null, null);
            } else if (attrDesc.getBinding() == Double.class) {
                field = new Schema.Field(attrDesc.getAttributeName(), Schema.createUnion(Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.NULL)), null, null);
            } else if (attrDesc.getBinding() == LocalDate.class) {
                org.apache.avro.LogicalTypes.Date dateType = LogicalTypes.date();
                field = new Schema.Field(attrDesc.getAttributeName(), Schema.createUnion(dateType.addToSchema(Schema.create(Schema.Type.INT)), Schema.create(Schema.Type.NULL)), null, null);
            } else if (attrDesc.getBinding() == LocalDateTime.class) {
                TimestampMillis datetimeType = LogicalTypes.timestampMillis();
                // FIXME
                //LocalTimestampMillis datetimeType = LogicalTypes.localTimestampMillis();
                String doc = "UTC based timestamp.";
                field = new Schema.Field(attrDesc.getAttributeName(), Schema.createUnion(datetimeType.addToSchema(Schema.create(Schema.Type.LONG)), Schema.create(Schema.Type.NULL)), doc, null);
            } else if (attrDesc.getBinding() == LocalTime.class) {
                TimeMillis timeType = LogicalTypes.timeMillis();
                String doc = "UTC based time.";
                field = new Schema.Field(attrDesc.getAttributeName(), Schema.createUnion(timeType.addToSchema(Schema.create(Schema.Type.INT)), Schema.create(Schema.Type.NULL)), doc, null);
            } else if (attrDesc.getBinding() == Boolean.class) {
                field = new Schema.Field(attrDesc.getAttributeName(), Schema.createUnion(Schema.create(Schema.Type.BOOLEAN), Schema.create(Schema.Type.NULL)), null, null);
            }
            else {
                field = new Schema.Field(attrDesc.getAttributeName(), Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)), null, null);
            }

            fields.add(field);
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

package ch.interlis.ioxwkf.arcgen;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;

import java.io.File;
import java.io.IOException;

import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.ili2c.metamodel.Viewable;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxFactoryCollection;
import ch.interlis.iox.IoxWriter;
import ch.interlis.iox_j.EndBasketEvent;
import ch.interlis.iox_j.EndTransferEvent;
import ch.interlis.iox_j.ObjectEvent;
import ch.interlis.iox_j.StartBasketEvent;
import ch.interlis.iox_j.StartTransferEvent;
import ch.interlis.iox_j.jts.Iox2jts;
import ch.interlis.iox_j.jts.Iox2jtsException;
import ch.interlis.iox_j.jts.Iox2jtsext;
import ch.interlis.ioxwkf.dbtools.AttributeDescriptor;

// Bemerkungen:
// - Es wird ein Primary Key Attribute ("ID") immer hinzugefügt.
// - Es gibt ein Arc Generate Standardformat und ein Extended Format. Es wurde das Extended Format umgesetzt. So wie ich es verstehen
// benötigt sonARMS dieses Extended Format.
// - Es gibt kein Modellsupport und auch kein ad-hoc-Schema-Support, d.h. die Attributedescriptions müssen von aussen gesetzt werden.
// - Die Attributnamen werden mit Grossbuchstaben geschrieben.
// - Es wird nur eine Geometrie pro Feature unterstützt. Wird nicht geprüft.
// - Spezifikation: Siehe PDF in src/main/resources

// Fragen:
// - Ist das Encoding vorgegeben?
// - Muss/soll Delimiter wählbar sein (falls ja, siehe CSV-Format in iox-ili).

public class ArcGenWriter implements IoxWriter {
    private BufferedWriter writer = null;
    private TransferDescription td = null;
    private String[] headerAttrNames = null;
    private boolean firstObj = true;
    private int nextId = 1;

    private static final String ID_ATTR_NAME = "ID";
    //private Character currentValueDelimiter = DEFAULT_VALUE_DELIMITER;
    private char currentValueSeparator = '\t';

    private String iliGeomAttrName = null;
    private String geometryType = null;
    private Integer coordDimension = 2;
    private List<AttributeDescriptor> attrDescs = null;
    
    private static final String COORD="COORD";
    private static final String MULTICOORD="MULTICOORD";
    private static final String POLYLINE="POLYLINE";
    private static final String MULTIPOLYLINE="MULTIPOLYLINE";
    private static final String MULTISURFACE="MULTISURFACE";
    
    public ArcGenWriter(File file) throws IoxException {
        this(file, null);
    }

    public ArcGenWriter(File file, Settings settings) throws IoxException {
        init(file, settings);
    }

    private void init(File file, Settings settings) throws IoxException {        
        try {
            // Ohne encoding-Support. Falls notwendig, siehe CsvWriter.
            writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)));
        } catch (IOException e) {
            throw new IoxException("could not create file", e);
        }        
    }
    
    public void setModel(TransferDescription td) {
        if(headerAttrNames != null) {
            throw new IllegalStateException("attributes must not be set");
        }
        this.td = td;
    }
    
    public void setAttributeDescriptors(AttributeDescriptor[] attrDescs) throws IoxException {
        this.attrDescs = new ArrayList<AttributeDescriptor>();
        for (AttributeDescriptor attrDesc : attrDescs) {
            if (attrDesc.getDbColumnGeomTypeName() != null) {
                if (iliGeomAttrName != null) {
                    throw new IoxException("only one geometry attribute allowed");
                }
                iliGeomAttrName = attrDesc.getIomAttributeName();
                geometryType = attrDesc.getDbColumnGeomTypeName();
                coordDimension = attrDesc.getCoordDimension();
            }
            this.attrDescs.add(attrDesc); 
        }
        if (iliGeomAttrName == null) {
            throw new IoxException("no geometry attribute found");
        } 
    } 
    
    @Override
    public void close() throws IoxException {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                throw new IoxException(e);
            }
            writer = null;
        }
    }

    @Override
    public void write(IoxEvent event) throws IoxException {
        if (event instanceof StartTransferEvent) {
        } else if (event instanceof StartBasketEvent) {
        } else if (event instanceof ObjectEvent) {
            ObjectEvent obj = (ObjectEvent) event;
            IomObject iomObj = (IomObject)obj.getIomObject();
            if(firstObj) {
                // get list of attr names
                if (td != null) {
                    // not supported
//                    Viewable resultViewableHeader=findViewable(iomObj);
//                    if(resultViewableHeader==null) {
//                        throw new IoxException("class "+iomObj.getobjecttag()+" in model not found");
//                    }
//                    headerAttrNames=getAttributeNames(resultViewableHeader);
                } else {
                    // Falls setAttributes() nicht von aussen verwendet.
                    // Aus erstem Objekt eruieren. 
                    // TODO
//                    if (headerAttrNames == null) {
//                        headerAttrNames = getAttributeNames(iomObj);
//                    }
                }
                try {
                    writeHeader(attrDescs);
                } catch (IOException e) {
                    throw new IoxException(e);
                }
                firstObj = false;
            }
            Map<String,String> validAttrValues = getAttributeValues(attrDescs, iomObj);
            try {
                writeRecord(validAttrValues);
            } catch (IOException e) {
                throw new IoxException(e);
            }
        } else if (event instanceof EndBasketEvent) {
        } else if (event instanceof EndTransferEvent) {
            try {
                writer.write("END");
            } catch (IOException e) {
               new IoxException(e.getMessage());
            }

            close();
        } else {
            throw new IoxException("unknown event type "+event.getClass().getName());
        }
    }
    
    private void writeRecord(Map<String,String> attrValues) throws IOException, IoxException {
        writer.write(getNextId());

        // Punktgeometrien
        for (AttributeDescriptor attrDesc : attrDescs) {
            if (attrDesc.getDbColumnGeomTypeName() != null) {
                String attrValue = attrValues.get(attrDesc.getIomAttributeName());
                if (attrDesc.getDbColumnGeomTypeName().equals(AttributeDescriptor.GEOMETRYTYPE_POINT)) {
                    writer.write(currentValueSeparator);
                    writer.write(attrValue);
                    break;
                }
            }
        }
        
        // Sachattribute
        for (AttributeDescriptor attrDesc : attrDescs) {
            if (attrDesc.getDbColumnGeomTypeName() != null) {
                continue;
            }
            writer.write(currentValueSeparator);
            String attrValue = attrValues.get(attrDesc.getIomAttributeName());
            writer.write(attrValue);            
        }
        
        // Linien und Flächen
        for (AttributeDescriptor attrDesc : attrDescs) {
            if (attrDesc.getDbColumnGeomTypeName() != null) {
                String attrValue = attrValues.get(attrDesc.getIomAttributeName());
                if (attrDesc.getDbColumnGeomTypeName().equals(AttributeDescriptor.GEOMETRYTYPE_LINESTRING)) {
                    writer.write(attrValue);
                    break;
                } else if (attrDesc.getDbColumnGeomTypeName().equals(AttributeDescriptor.GEOMETRYTYPE_POLYGON)) {
                    writer.write(attrValue);
                    break;
                }
            }
        }
                
        writer.newLine();
    }

    private void writeHeader(List<AttributeDescriptor> attrDescs) throws IOException {
        boolean firstName = true;
        
        // Hardcodiertes ID-Attribut mit Maschinenwert. 
        writer.write(ID_ATTR_NAME);
        
        // first loop to find out geometry attribute
        for (AttributeDescriptor attrDesc : attrDescs) {
            // FIXME: isGeometry() wirft NullPointer. -> Ticket machen
            if (attrDesc.getDbColumnGeomTypeName() != null) {                
                if (attrDesc.getDbColumnGeomTypeName().equals(AttributeDescriptor.GEOMETRYTYPE_POINT)) {
                    writer.write(currentValueSeparator);
                    writer.write("X");
                    writer.write(currentValueSeparator);
                    writer.write("Y");
                    
                    if (coordDimension == 3) {
                        writer.write(currentValueSeparator);
                        writer.write("Z");
                    }                   
                } else if (attrDesc.getDbColumnGeomTypeName().equals(AttributeDescriptor.GEOMETRYTYPE_LINESTRING)) {
                    // do nothing since linestring is appended on new line without attribute name in header
                } else if (attrDesc.getDbColumnGeomTypeName().equals(AttributeDescriptor.GEOMETRYTYPE_POLYGON)) {
                    // siehe oben                    
                }
            }
        }
        
        // second loop for all other attributes
        for (AttributeDescriptor attrDesc : attrDescs) {
            if (attrDesc.getDbColumnGeomTypeName() != null) {
                continue;
            }
            writer.write(currentValueSeparator);

            firstName = false;
            writer.write(attrDesc.getIomAttributeName().toUpperCase());
        }
        writer.newLine();
    }
    
    /*
     * Es werden nur die Attribute in die Datei geschrieben, die auch in attrNames
     * vorkommen. D.h. im IomObject können mehr Attribute vorhanden sein, als dann
     * tatsächlich exportiert werden.
     * Liefert die String-Repräsentation zurück (Geometrie so wie sie sein muss gemäss ArcGenerate).
     */    
    private Map<String,String> getAttributeValues(List<AttributeDescriptor> attrDescs, IomObject currentIomObject) throws IoxException {
        Map<String,String> attrValues = new HashMap<>();
        for (int i = 0; i < attrDescs.size(); i++) {
            String attrName = attrDescs.get(i).getIomAttributeName();
            String attrValue;
            if (attrDescs.get(i).getIomAttributeName().equals(iliGeomAttrName)) {
                attrValue = encodeGeometry(currentIomObject);
            } else {
                attrValue = currentIomObject.getattrvalue(attrDescs.get(i).getIomAttributeName());     
            }
            attrValues.put(attrName, attrValue);
        }        
        return attrValues;
    }
    
    private String encodeGeometry(IomObject iomObj) throws IoxException {
        IomObject geomObj = iomObj.getattrobj(iliGeomAttrName, 0);

        String attrValue = "";
        if (geomObj != null) {
            try {
                if (geomObj.getobjecttag().equals(COORD)) {
                    Coordinate coord = Iox2jts.coord2JTS(geomObj);
                    attrValue = String.valueOf(coord.x) + currentValueSeparator + String.valueOf(coord.y);
                    
                    if (coordDimension == 3) {
                        attrValue += currentValueSeparator + String.valueOf(coord.z);
                    }
                } else if (geomObj.getobjecttag().equals(POLYLINE)) {
                    attrValue += System.lineSeparator();
                    LineString line = Iox2jts.polyline2JTSlineString(geomObj, false, 0.01);
                    Coordinate[] coords = line.getCoordinates();
                    for (int i=0; i<coords.length; i++) {
                        Coordinate coord = coords[i];
                        attrValue += String.valueOf(coord.x) + currentValueSeparator + String.valueOf(coord.y);
                        if (coordDimension == 3) {
                            attrValue += currentValueSeparator + String.valueOf(coord.z);
                        }
                        attrValue += System.lineSeparator();
                    }
                    attrValue += "END";
                } else if (geomObj.getobjecttag().equals(MULTISURFACE)) {
                    attrValue += System.lineSeparator();
                    Polygon poly = Iox2jtsext.surface2JTS(geomObj, 0.01); // Die nicht ext-Variante hat doppelte Stützpunkte. Ist das nicht gefixed?
                    // Es wird nur der äussere Ring verwendet.
                    LineString line = poly.getExteriorRing();
                    Coordinate[] coords = line.getCoordinates();
                    for (int i=0; i<coords.length; i++) {
                        Coordinate coord = coords[i];
                        attrValue += String.valueOf(coord.x) + currentValueSeparator + String.valueOf(coord.y);
                        if (coordDimension == 3) {
                            attrValue += currentValueSeparator + String.valueOf(coord.z);
                        }
                        attrValue += System.lineSeparator();
                    }
                    attrValue += "END";
                }
            }
            catch (Iox2jtsException e) {
                throw new IoxException(e);
            }
        }
        return attrValue;
    }

    private String getNextId() {
        int count = nextId;
        nextId += 1;
        return String.valueOf(count);
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

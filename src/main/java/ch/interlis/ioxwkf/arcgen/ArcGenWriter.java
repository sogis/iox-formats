package ch.interlis.ioxwkf.arcgen;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
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
import ch.interlis.ioxwkf.dbtools.AttributeDescriptor;

// TODO
// - Muss Encoding etwas bestimmtes sein? 
// - Muss/soll Delimiter wählbar sein? Wiederverwenden von CSV...
// - Mechanismus, falls keine ID (eindeutiger Wert) mitgeliefert wird. -> Weil die Reihenfolge der Attribute nicht garantiert ist (?), 
// mache ich immer eine eigene ID.
// - Es gibt Standardformat und Extended Format. Wir brauchen anscheinend das Extended. Am besten wäre es steuerbar über settings.
// - Noch keine Modellsupport.
// - Hardcodiert Uppercase-Attributnamen. Weiss nicht, ob notwendig für sonARMS
// - writeChars()-Methode in CsvWriter sehr elaboriert. Ich verzichte darauf. Es sind somit z.B. keine Carriage returns innerhalb eines 
// Wertes möglich.
// - Nur eine Geometrie wird unterstützt, wird nicht geprüft.

// FIXME
// Aaaah, brauche doch AttributeDescriptor, weil ich wissen muss, welches Attribut die Geometrie ist.


// "Spezifikation": siehe PDF in src/main/resources
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
    private List<AttributeDescriptor> attrDescs = null;
    
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

    public void setAttributes(String [] attr) {
        if(td != null) {
            throw new IllegalStateException("interlis model must not be set");
        }
        headerAttrNames = attr.clone();
    }

    public void setAttributeDescriptors(AttributeDescriptor[] attrDescs) {
        this.attrDescs = new ArrayList<AttributeDescriptor>();
        for (AttributeDescriptor attrDesc : attrDescs) {
            if (attrDesc.getDbColumnGeomTypeName() != null) {
                // Nur ein Geometrieattribut möglich.
                iliGeomAttrName = attrDesc.getIomAttributeName();
            }
            this.attrDescs.add(attrDesc);
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
            ObjectEvent obj=(ObjectEvent) event;
            IomObject iomObj=(IomObject)obj.getIomObject();
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
                    writeHeader(headerAttrNames);
                } catch (IOException e) {
                    throw new IoxException(e);
                }
                firstObj = false;
            }
            String[] validAttrValues = getAttributeValues(headerAttrNames, iomObj);
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
    
    private void writeRecord(String[] attrValues) throws IOException, IoxException {
        boolean first = true;
        
        writer.write(getNextId());
        writer.write(currentValueSeparator);

        for (String value : attrValues) {
            if (!first) {
                writer.write(currentValueSeparator);
            }
            //writeChars(value);
            writer.write(value);
            first = false;
        }
        writer.newLine();
    }

    private void writeHeader(String[] attrNames) throws IOException {
        boolean firstName = true;
        
        writer.write(ID_ATTR_NAME);
        writer.write(currentValueSeparator);
        
        for (String name : attrNames) {
            System.out.println("**: " + name);
            if (!firstName) {
                writer.write(currentValueSeparator);
            }
            firstName = false;
//            if (currentValueDelimiter != null) {
//                writer.write(currentValueDelimiter);                
//            }
            writer.write(name.toUpperCase());
//            if (currentValueDelimiter != null) {
//                writer.write(currentValueDelimiter);                
//            }
        }
        writer.newLine();
    }
    
    /*
     * Es werden nur die Attribute in die Datei geschrieben, die auch in attrNames
     * vorkommen. D.h. im IomObject können mehr Attribute vorhanden sein, als dann
     * tatsächlich exportiert werden.
     */
    private String[] getAttributeValues(String[] attrNames, IomObject currentIomObject) {
        String[] attrValues = new String[attrNames.length];
        for (int i = 0; i < attrNames.length; i++) {
            String attrValue = currentIomObject.getattrvalue(attrNames[i]);
            attrValues[i] = attrValue;
        }
        return attrValues;
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

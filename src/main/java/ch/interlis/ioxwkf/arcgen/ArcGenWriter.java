package ch.interlis.ioxwkf.arcgen;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
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

// TODO
// - Muss Encoding etwas bestimmtes sein? 
// - Muss/soll Delimiter wählbar sein? Wiederverwenden von CSV...
// - Mechanismus, falls keine ID (eindeutiger Wert) mitgeliefert wird.
// - Es gibt Standardformat und Extended Format. Wir brauchen anscheinend das Extended. Am besten wäre es steuerbar über settings.
// - Noch keine Modellsupport.
// - Hardcodiert Uppercase-Attributnamen. Weiss nicht, ob notwendig für sonARMS

// "Spezifikation": siehe PDF in src/main/resources
public class ArcGenWriter implements IoxWriter {
    private BufferedWriter writer = null;
    private TransferDescription td = null;
    private String[] headerAttrNames = null;
    private boolean firstObj = true;
    
    //private Character currentValueDelimiter = DEFAULT_VALUE_DELIMITER;
    private char currentValueSeparator = '\t';
    
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
//            try {
//                writeRecord(validAttrValues);
//            } catch (IOException e) {
//                throw new IoxException(e);
//            }
            
            
        } else if (event instanceof EndBasketEvent) {
        } else if (event instanceof EndTransferEvent) {
            close();
        } else {
            throw new IoxException("unknown event type "+event.getClass().getName());
        }
    }
    
    private void writeHeader(String[] attrNames) throws IOException {
        boolean firstName = true;
        for (String name : attrNames) {
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

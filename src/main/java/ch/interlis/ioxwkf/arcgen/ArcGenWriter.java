package ch.interlis.ioxwkf.arcgen;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.File;
import java.io.IOException;

import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxFactoryCollection;
import ch.interlis.iox.IoxWriter;

// TODO
// - Muss Encoding etwas bestimmtes sein? 
// - Muss/soll Delimiter wählbar sein? Wiederverwenden von CSV...
// - Mechanismus, falls keine ID (eindeutiger Wert) mitgeliefert wird.

// "Spezifikation": siehe PDF in src/main/resources
public class ArcGenWriter implements IoxWriter {
    private BufferedWriter writer = null;
    private TransferDescription td = null;
    private String[] headerAttrNames = null;
    
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

    @Override
    public void write(IoxEvent arg0) throws IoxException {
        // TODO Auto-generated method stub

    }

}

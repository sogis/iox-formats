package ch.interlis.ioxwkf.dbtools;

import java.io.File;

import ch.ehi.basics.logging.EhiLogger;
import ch.ehi.basics.settings.Settings;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxWriter;
import ch.interlis.ioxwkf.arcgen.ArcGenWriter;

public class Db2ArcGen extends AbstractExportFromdb {

    @Override
    protected IoxWriter createWriter(File file, Settings config, AttributeDescriptor dbColumns[]) throws IoxException {
        if (file != null) {
            EhiLogger.logState("file to write to: <"+file.getName()+">");
        } else {
            throw new IoxException("file==null.");
        }

        ArcGenWriter writer = new ArcGenWriter(file, config);
        writer.setAttributeDescriptors(dbColumns);
        
        return writer;
    }

}

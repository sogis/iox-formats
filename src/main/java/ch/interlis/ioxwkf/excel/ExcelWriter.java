package ch.interlis.ioxwkf.excel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.Normalizer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.io.FilenameUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CreationHelper;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;

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

public class ExcelWriter implements IoxWriter {
    private File outputFile;

    private Row headerRow = null;
    private List<ExcelAttributeDescriptor> attrDescs = null;
    private Map<String, Integer> attrOrder = null;
    private XSSFWorkbook workbook = null;
    private XSSFSheet sheet = null;
    private CreationHelper createHelper = null;
    //private String fileName = null;
    private String sheetName = null;
    
    private TransferDescription td = null;
//    private String iliGeomAttrName = null;
    
    // ili types
    private static final String COORD="COORD";
    private static final String MULTICOORD="MULTICOORD";
    private static final String POLYLINE="POLYLINE";
    private static final String MULTIPOLYLINE="MULTIPOLYLINE";
    private static final String MULTISURFACE="MULTISURFACE";

    private static final String SHEET_NAME = "ch.interlis.ioxwkf.excel.sheetName";
    
    private Integer srsId = null;
    private Integer defaultSrsId = 2056; // TODO: null

    public ExcelWriter(File file) throws IoxException {
        this(file, null);
    }

    public ExcelWriter(File file, Settings settings) throws IoxException {
        System.setProperty("log4j2.loggerContextFactory","org.apache.logging.log4j.simple.SimpleLoggerContextFactory");
        System.setProperty("org.apache.logging.log4j.simplelog.StatusLogger.level", "OFF");
        init(file, settings);
    }

    private void init(File file, Settings settings) throws IoxException {
        this.outputFile = file;
        
        if (settings != null) {
            this.sheetName = settings.getValue(SHEET_NAME)!=null ? settings.getValue(SHEET_NAME) : file.getName();
        } else {
            this.sheetName = file.getName();
        }
    }

    public void setModel(TransferDescription td) {
        this.td = td;
    }

    public void setAttributeDescriptors(List<ExcelAttributeDescriptor> attrDescs) {
        this.attrDescs = attrDescs;
    }
    
    @Override
    public void write(IoxEvent event) throws IoxException {
        if (event instanceof StartTransferEvent) {
            // ignore
        } else if (event instanceof StartBasketEvent) {
            try {
                if (outputFile.exists()) {
                    try (FileInputStream fis = new FileInputStream(outputFile)) {
                        workbook = new XSSFWorkbook(fis);
                    }
                } else {
                    workbook = new XSSFWorkbook();
                }
            } catch (IOException e) {
                throw new IoxException(e);
            }

            if (headerRow == null) { 
                String normalizedFileName = this.normalizeFileName(this.sheetName);
                
                sheet = workbook.createSheet(normalizedFileName);
                headerRow = sheet.createRow(0);
                createHelper = workbook.getCreationHelper();

                // Header wird nicht geschrieben, wenn man von aussen nicht setAttrDescs setzt.
                if (attrDescs != null) {
                    int cellnum = 0;
                    for (ExcelAttributeDescriptor attrDesc : attrDescs) {
                        Cell cell = headerRow.createCell(cellnum++);
                        cell.setCellValue(attrDesc.getAttributeName());
                    }                    
                }
            }
        } else if (event instanceof ObjectEvent) {            
            ObjectEvent obj = (ObjectEvent) event;
            IomObject iomObj = obj.getIomObject();
            String tag = iomObj.getobjecttag();
            
            // Wenn null, dann gibt es noch kein "Schema".
            if (attrDescs == null) {
                attrDescs = new ArrayList<>();
                attrOrder = new HashMap<String, Integer>();
                if(td != null) {
                    Viewable aclass = (Viewable) XSDGenerator.getTagMap(td).get(tag);
                    if (aclass == null) {
                        throw new IoxException("class "+iomObj.getobjecttag()+" not found in model");
                    }
                    Iterator viewableIter = aclass.getAttributes();
                    while(viewableIter.hasNext()) {
                        ExcelAttributeDescriptor attrDesc = new ExcelAttributeDescriptor();

                        Object attrObj = viewableIter.next();
                        //System.out.println(attrObj);

                        if(attrObj instanceof LocalAttribute) {
                            LocalAttribute localAttr = (LocalAttribute)attrObj;
                            String attrName = localAttr.getName();
                            //System.out.println(attrName);
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
                        //create the builder
                        ExcelAttributeDescriptor attrDesc = new ExcelAttributeDescriptor();

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
            
            // HeaderRow kann initialisert worden sein, aber nicht abgefüllt, wenn
            // setAttrDescs nicht von aussen gesetzt wurde.
            if (headerRow.getRowNum() == 0) {
                int cellnum = 0;
                for (ExcelAttributeDescriptor attrDesc : attrDescs) {
                    Cell cell = headerRow.createCell(cellnum++);
                    cell.setCellValue(attrDesc.getAttributeName());
                }
            }
                         
            Row row = sheet.createRow(sheet.getLastRowNum()+1);
            for (ExcelAttributeDescriptor attrDesc : attrDescs) {
                String attrName = attrDesc.getAttributeName();

                int cellnum = getColumnIndex(attrName);
                Cell cell = row.createCell(cellnum);

                // TODO Geometry
                if (attrDesc.getBinding() == String.class) {
                    String attrValue = iomObj.getattrvalue(attrName);
                    cell.setCellValue(attrValue);
                } else if (attrDesc.getBinding() == Integer.class) {
                    if (iomObj.getattrvalue(attrName) != null) {
                        int attrValue = Integer.valueOf(iomObj.getattrvalue(attrName)).intValue(); 
                        cell.setCellValue(attrValue);
                    }
                } else if (attrDesc.getBinding() == Double.class) {
                    if (iomObj.getattrvalue(attrName) != null) {
                        double attrValue = Double.valueOf(iomObj.getattrvalue(attrName)).doubleValue();
                        cell.setCellValue(attrValue);
                    }
                } else if (attrDesc.getBinding() == LocalDate.class) {
                    if (iomObj.getattrvalue(attrName) != null) {
                        LocalDate localDate = LocalDate.parse(iomObj.getattrvalue(attrName), DateTimeFormatter.ISO_LOCAL_DATE);
                        CellStyle cellStyle = workbook.createCellStyle();
//                        cellStyle.setDataFormat(createHelper.createDataFormat().getFormat("d.mm.yyyy"));
                        cellStyle.setDataFormat((short) 14);
                        cell.setCellValue(localDate);
                        cell.setCellStyle(cellStyle);
                    }
                } else if (attrDesc.getBinding() == LocalDateTime.class) { 
                    if (iomObj.getattrvalue(attrName) != null) {
                        LocalDateTime localDateTime = LocalDateTime.parse(iomObj.getattrvalue(attrName));
                        CellStyle cellStyle = workbook.createCellStyle();
//                        cellStyle.setDataFormat(createHelper.createDataFormat().getFormat("d.mm.yyyy h:mm:ss"));
                        cellStyle.setDataFormat((short) 22);
                        cell.setCellValue(localDateTime);
                        cell.setCellStyle(cellStyle);
                    }
                } else if (attrDesc.getBinding() == LocalTime.class) {
                    if (iomObj.getattrvalue(attrName) != null) {
                        LocalTime localTime = LocalTime.parse(iomObj.getattrvalue(attrName));
                        CellStyle cellStyle = workbook.createCellStyle();
                        // Subseconds würde gehen: HH:mm:ss.000
                        // Dann get die Abkürzung mit setDataFormat nicht es müsste wohl createHelper.createDataFormat().getFormat()
                        // verwendet werden.
                        cellStyle.setDataFormat((short) 21);
                        cell.setCellValue(DateUtil.convertTime(localTime.format(DateTimeFormatter.ofPattern("HH:mm:ss"))));
                        cell.setCellStyle(cellStyle);
                    }
                } else if (attrDesc.getBinding() == Boolean.class) {
                    if (iomObj.getattrvalue(attrName) != null) {
                        boolean attrValue = Boolean.parseBoolean(iomObj.getattrvalue(attrName));
                        cell.setCellValue(attrValue);
                    }
                } else {
                    String attrValue = iomObj.getattrvalue(attrName);
                    cell.setCellValue(attrValue);
                }
            }
        }
    }
    
    private int getColumnIndex(String attrName) throws NoSuchElementException {        
        Iterator<Cell> it = headerRow.cellIterator();
        while (it.hasNext()) {
            Cell cell = it.next();
            String cellValue = cell.getStringCellValue();
            if (cellValue.equalsIgnoreCase(attrName)) {
                return cell.getColumnIndex();
            }
        }
        throw new NoSuchElementException();
    }
    
    @Override
    public void close() throws IoxException {
        try {
            FileOutputStream out = new FileOutputStream(outputFile);
            workbook.write(out);                
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
            throw new IoxException(e.getMessage());
        }
    }

    @Override
    public IomObject createIomObject(String arg0, String arg1) throws IoxException {
        return null;
    }

    @Override
    public void flush() throws IoxException {        
    }

    @Override
    public IoxFactoryCollection getFactory() throws IoxException {
        return null;
    }

    @Override
    public void setFactory(IoxFactoryCollection arg0) throws IoxException {        
    }

    private String normalizeFileName(String fileName) {
        // Normalize the string to decompose characters
        String normalized = Normalizer.normalize(FilenameUtils.getBaseName(fileName), Normalizer.Form.NFD);
        
        // Remove diacritical marks (accents, etc.)
        String ascii = normalized.replaceAll("\\p{M}", "");
        
        // Remove any remaining non-ASCII characters
        ascii = ascii.replaceAll("[^\\x00-\\x7F]", "");
        
        return ascii.toLowerCase();        
    }
}

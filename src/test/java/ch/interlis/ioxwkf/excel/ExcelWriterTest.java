package ch.interlis.ioxwkf.excel;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.vividsolutions.jts.geom.Point;

import ch.interlis.ili2c.Ili2cFailure;
import ch.interlis.ili2c.config.FileEntry;
import ch.interlis.ili2c.config.FileEntryKind;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.iom.IomObject;
import ch.interlis.iom_j.Iom_jObject;
import ch.interlis.iox.IoxException;
import ch.interlis.iox_j.EndBasketEvent;
import ch.interlis.iox_j.EndTransferEvent;
import ch.interlis.iox_j.ObjectEvent;
import ch.interlis.iox_j.StartBasketEvent;
import ch.interlis.iox_j.StartTransferEvent;

import org.junit.jupiter.api.Assertions;

public class ExcelWriterTest {
    
    private static final String TEST_IN="src/test/data/ExcelWriter/";
    private static final String TEST_OUT="build/test/data/ExcelWriter";


    @BeforeAll
    public static void setupFolder() {
        new File(TEST_OUT).mkdirs();
    }

    public TransferDescription compileModel(String iliFileName) throws Ili2cFailure {
        // compile model
        ch.interlis.ili2c.config.Configuration ili2cConfig = new ch.interlis.ili2c.config.Configuration();
        FileEntry fileEntry = new FileEntry(TEST_IN+"/"+iliFileName, FileEntryKind.ILIMODELFILE);
        ili2cConfig.addFileEntry(fileEntry);
        TransferDescription td = ch.interlis.ili2c.Ili2c.runCompiler(ili2cConfig);
        return td;
    }

    @Test
    public void model_set_Ok() throws Exception {
        // Prepare
        File parentDir = new File(TEST_OUT, "model_set_Ok");
        parentDir.mkdirs();
        TransferDescription td = compileModel("Test1.ili");
        
        Iom_jObject inputObj = new Iom_jObject("Test1.Topic1.Class1", "o1");
        inputObj.setattrvalue("id1", "1");
        inputObj.setattrvalue("aText", "text1");
        inputObj.setattrvalue("aDouble", "53434.123");
        inputObj.setattrvalue("aDate", "1977-09-23");
        inputObj.setattrvalue("aDatetime", "1977-09-23T19:51:35.123");
        inputObj.setattrvalue("aTime", "19:51:35.123");
        inputObj.setattrvalue("aBoolean", "true");

        // Run
        ExcelWriter writer = null;
        File file = new File(parentDir,"model_set_Ok.xlsx");
        try {
            writer = new ExcelWriter(file);
            writer.setModel(td);
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
            writer.write(new ObjectEvent(inputObj));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
        } catch(IoxException e) {
            throw new IoxException(e);
        } finally {
            if(writer != null) {
                try {
                    writer.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                writer=null;
            }
        }

        // Validate
        FileInputStream fis = new FileInputStream(file);        
        XSSFWorkbook workbook = new XSSFWorkbook(fis);
        XSSFSheet sheet = workbook.getSheetAt(0);

        Row headerRow = sheet.getRow(0);
        Assertions.assertEquals(7, headerRow.getLastCellNum());
        
        Row dataRow = sheet.getRow(1);
        Assertions.assertEquals(7, dataRow.getLastCellNum());

        Iterator<Cell> dataCellIterator = dataRow.cellIterator();
        while (dataCellIterator.hasNext()) {
            Cell cell = dataCellIterator.next();
            String attrName = this.getAttrName(headerRow, cell.getColumnIndex());

            switch (attrName) {
                case "aText":
                    Assertions.assertEquals("text1", cell.getStringCellValue());
                    break;
                case "id1":
                    Assertions.assertEquals(1, cell.getNumericCellValue(), 0.00001);
                    break;
                case "aDouble":
                    Assertions.assertEquals(53434.123, cell.getNumericCellValue(), 0.00001);
                    break;
                case "aDate":
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");      
                    Date expectedDate = formatter.parse("1977-09-23");
                    Assertions.assertEquals(expectedDate, cell.getDateCellValue());
                    break;
                case "aDatetime":
                    SimpleDateFormat formatterDatetime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");                          
                    Date resultDatetime = cell.getDateCellValue();
                    formatterDatetime.format(resultDatetime);
                    // Achtung: Subsekunden gehen verloren. Siehe ExcelWriter. Eventuell lösbar.
                    Assertions.assertEquals("1977-09-23T19:51:35", formatterDatetime.format(resultDatetime));
                    break;
                case "aTime":
                    SimpleDateFormat formatterTime = new SimpleDateFormat("HH:mm:ss");                          
                    Date resultTime = cell.getDateCellValue();
                    Assertions.assertEquals("19:51:35", formatterTime.format(resultTime));
                    break;
                case "aBoolean":
                    Assertions.assertEquals(true, cell.getBooleanCellValue());
                    break;
                case "attrPoint":
                    // TODO
                    break;
                default:
                    throw new IllegalArgumentException("Invalid attribute name found: " + attrName);
            }
        }
        
        Assertions.assertEquals(1, sheet.getLastRowNum());
        
        workbook.close();
        fis.close();
    }
    
    @Test
    public void attributes_description_set_Ok() throws Exception {
        // Prepare
        File parentDir = new File(TEST_OUT, "attributes_description_set_Ok");
        parentDir.mkdirs();

        List<ExcelAttributeDescriptor> attrDescs = new ArrayList<>();
        {
            ExcelAttributeDescriptor attrDesc = new ExcelAttributeDescriptor();
            attrDesc.setAttributeName("id1");
            attrDesc.setBinding(Integer.class);
            attrDescs.add(attrDesc);
        }
        {
            ExcelAttributeDescriptor attrDesc = new ExcelAttributeDescriptor();
            attrDesc.setAttributeName("aText");
            attrDesc.setBinding(String.class);
            attrDescs.add(attrDesc);
        }
        {
            ExcelAttributeDescriptor attrDesc = new ExcelAttributeDescriptor();
            attrDesc.setAttributeName("aDouble");
            attrDesc.setBinding(Double.class);
            attrDescs.add(attrDesc);
        }
        {
            ExcelAttributeDescriptor attrDesc = new ExcelAttributeDescriptor();
            attrDesc.setAttributeName("aDate");
            attrDesc.setBinding(LocalDate.class);
            attrDescs.add(attrDesc);
        }
        {
            ExcelAttributeDescriptor attrDesc = new ExcelAttributeDescriptor();
            attrDesc.setAttributeName("aDatetime");
            attrDesc.setBinding(LocalDateTime.class);
            attrDescs.add(attrDesc);
        }
        {
            ExcelAttributeDescriptor attrDesc = new ExcelAttributeDescriptor();
            attrDesc.setAttributeName("aTime");
            attrDesc.setBinding(LocalTime.class);
            attrDescs.add(attrDesc);
        }
        {
            ExcelAttributeDescriptor attrDesc = new ExcelAttributeDescriptor();
            attrDesc.setAttributeName("aBoolean");
            attrDesc.setBinding(Boolean.class);
            attrDescs.add(attrDesc);
        }
        {
            ExcelAttributeDescriptor attrDesc = new ExcelAttributeDescriptor();
            attrDesc.setAttributeName("attrPoint");
            attrDesc.setBinding(Point.class);
            attrDescs.add(attrDesc);
        }

        Iom_jObject inputObj = new Iom_jObject("Test1.Topic1.Obj1", "o1");
        inputObj.setattrvalue("id1", "1");
        inputObj.setattrvalue("aText", "text1");
        inputObj.setattrvalue("aDouble", "53434.123");
        inputObj.setattrvalue("aDate", "1977-09-23");
        inputObj.setattrvalue("aDatetime", "1977-09-23T19:51:35.123");
        inputObj.setattrvalue("aTime", "19:51:35.123");
        inputObj.setattrvalue("aBoolean", "true");
        IomObject coordValue = inputObj.addattrobj("attrPoint", "COORD");
        {
            coordValue.setattrvalue("C1", "2600000.000");
            coordValue.setattrvalue("C2", "1200000.000");
        }

        // Run
        ExcelWriter writer = null;
        File file = new File(parentDir,"attributes_description_set_Ok.xlsx");
        try {
            writer = new ExcelWriter(file);
            writer.setAttributeDescriptors(attrDescs);
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
            writer.write(new ObjectEvent(inputObj));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
        } catch(IoxException e) {
            throw new IoxException(e);
        } finally {
            if(writer != null) {
                try {
                    writer.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                writer=null;
            }
        }

        // Validate
        FileInputStream fis = new FileInputStream(file);        
        XSSFWorkbook workbook = new XSSFWorkbook(fis);
        XSSFSheet sheet = workbook.getSheetAt(0);

        Row headerRow = sheet.getRow(0);
        Assertions.assertEquals(8, headerRow.getLastCellNum());
        
        Row dataRow = sheet.getRow(1);
        Assertions.assertEquals(8, dataRow.getLastCellNum());

        Iterator<Cell> dataCellIterator = dataRow.cellIterator();
        while (dataCellIterator.hasNext()) {
            Cell cell = dataCellIterator.next();
            String attrName = this.getAttrName(headerRow, cell.getColumnIndex());

            switch (attrName) {
                case "aText":
                    Assertions.assertEquals("text1", cell.getStringCellValue());
                    break;
                case "id1":
                    Assertions.assertEquals(1, cell.getNumericCellValue(), 0.00001);
                    break;
                case "aDouble":
                    Assertions.assertEquals(53434.123, cell.getNumericCellValue(), 0.00001);
                    break;
                case "aDate":
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");      
                    Date expectedDate = formatter.parse("1977-09-23");
                    Assertions.assertEquals(expectedDate, cell.getDateCellValue());
                    break;
                case "aDatetime":
                    SimpleDateFormat formatterDatetime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");                          
                    Date resultDatetime = cell.getDateCellValue();
                    formatterDatetime.format(resultDatetime);
                    // Achtung: Subsekunden gehen verloren. Siehe ExcelWriter. Eventuell lösbar.
                    Assertions.assertEquals("1977-09-23T19:51:35", formatterDatetime.format(resultDatetime));
                    break;
                case "aTime":
                    SimpleDateFormat formatterTime = new SimpleDateFormat("HH:mm:ss");                          
                    Date resultTime = cell.getDateCellValue();
                    Assertions.assertEquals("19:51:35", formatterTime.format(resultTime));
                    break;
                case "aBoolean":
                    Assertions.assertEquals(true, cell.getBooleanCellValue());
                    break;
                case "attrPoint":
                    // TODO
                    break;
                default:
                    throw new IllegalArgumentException("Invalid attribute name found: " + attrName);
            }
        }

        Assertions.assertEquals(1, sheet.getLastRowNum());
        
        workbook.close();
        fis.close();
    }

    
    @Test
    public void attributes_no_description_set_Ok() throws Exception {
        // Prepare
        File parentDir = new File(TEST_OUT, "attributes_no_description_set_Ok");
        parentDir.mkdirs();

        Iom_jObject inputObj = new Iom_jObject("Test1.Topic1.Obj1", "o1");
        inputObj.setattrvalue("id1", "1");
        inputObj.setattrvalue("aText", "text1");
        inputObj.setattrvalue("aDouble", "53434.123");
//        IomObject coordValue = inputObj.addattrobj("attrPoint", "COORD");
//        {
//            coordValue.setattrvalue("C1", "2600000.000");
//            coordValue.setattrvalue("C2", "1200000.000");
//        }

        // Run
        ExcelWriter writer = null;
        File file = new File(parentDir,"attributes_no_description_set_Ok.xlsx");
        try {
            writer = new ExcelWriter(file);
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
            writer.write(new ObjectEvent(inputObj));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
        } catch(IoxException e) {
            throw new IoxException(e);
        } finally {
            if(writer != null) {
                try {
                    writer.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                writer=null;
            }
        }

        // Validate
        FileInputStream fis = new FileInputStream(file);        
        XSSFWorkbook workbook = new XSSFWorkbook(fis);
        XSSFSheet sheet = workbook.getSheetAt(0);

        Row headerRow = sheet.getRow(0);
        Assertions.assertEquals(3, headerRow.getLastCellNum());
        
        Row dataRow = sheet.getRow(1);
        Assertions.assertEquals(3, dataRow.getLastCellNum());

        Iterator<Cell> dataCellIterator = dataRow.cellIterator();
        while (dataCellIterator.hasNext()) {
            Cell cell = dataCellIterator.next();
            String attrName = this.getAttrName(headerRow, cell.getColumnIndex());

            switch (attrName) {
                case "aText":
                    Assertions.assertEquals("text1", cell.getStringCellValue());
                    break;
                case "id1":
                    Assertions.assertEquals("1", cell.getStringCellValue());
                    break;
                case "aDouble":
                    Assertions.assertEquals("53434.123", cell.getStringCellValue());
                    break;
                default:
                    throw new IllegalArgumentException("Invalid attribute name found: " + attrName);
            }
        }
        
        Assertions.assertEquals(1, sheet.getLastRowNum());
        
        workbook.close();
        fis.close();
    }
    
    @Test
    public void attributes_no_description_set_null_value_Ok() throws IoxException, IOException {
        // Prepare
        File parentDir = new File(TEST_OUT, "attributes_no_description_set_null_value_Ok");
        parentDir.mkdirs();

        Iom_jObject inputObj1 = new Iom_jObject("Test1.Topic1.Obj1", "o1");
        inputObj1.setattrvalue("id1", "1");
        inputObj1.setattrvalue("aText", "text1");

        Iom_jObject inputObj2 = new Iom_jObject("Test1.Topic1.Obj1", "o2");
        inputObj2.setattrvalue("id1", "2");

        // Run
        ExcelWriter writer = null;
        File file = new File(parentDir,"attributes_no_description_set_null_value_Ok.xlsx");
        try {
            writer = new ExcelWriter(file);
            writer.write(new StartTransferEvent());
            writer.write(new StartBasketEvent("Test1.Topic1","bid1"));
            writer.write(new ObjectEvent(inputObj1));
            writer.write(new ObjectEvent(inputObj2));
            writer.write(new EndBasketEvent());
            writer.write(new EndTransferEvent());
        } catch(IoxException e) {
            throw new IoxException(e);
        } finally {
            if(writer != null) {
                try {
                    writer.close();
                } catch (IoxException e) {
                    throw new IoxException(e);
                }
                writer=null;
            }
        }
        
        // Validate
        FileInputStream fis = new FileInputStream(file);        
        XSSFWorkbook workbook = new XSSFWorkbook(fis);
        XSSFSheet sheet = workbook.getSheetAt(0);

        Row headerRow = sheet.getRow(0);
        Assertions.assertEquals(2, headerRow.getLastCellNum());

        Row dataRow = sheet.getRow(2);
        Assertions.assertEquals(2, dataRow.getLastCellNum());

        Iterator<Cell> dataCellIterator = dataRow.cellIterator();
        while (dataCellIterator.hasNext()) {
            Cell cell = dataCellIterator.next();
            String attrName = this.getAttrName(headerRow, cell.getColumnIndex());

            switch (attrName) {
                case "aText":
                    Assertions.assertEquals("", cell.getStringCellValue());
                    break;
                case "id1":
                    Assertions.assertEquals("2", cell.getStringCellValue());
                    break;
                default:
                    throw new IllegalArgumentException("Invalid attribute name found: " + attrName);
            }
        }
        
        Assertions.assertEquals(2, sheet.getLastRowNum());

        workbook.close();
        fis.close();
    }
    
    private String getAttrName(Row row, int columnIndex) {
        Cell cell = row.getCell(columnIndex);
        return cell.getStringCellValue();
    }
}

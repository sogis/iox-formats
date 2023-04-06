package ch.interlis.ioxwkf.parquet;

// TODO ParquetAttributeDescripton
// - attrbiuteGeomTypeName -> GPKG (die hoffentlich auch PostGIS entsprechen) Namen, damit man auch Kurven abbilden kann. Resp. das Wissen hat.
// Bindings wäre immer ohne Arcs. Eher was für generische AttributeDescriptor.
public class ParquetAttributeDescriptor {
    private String attributeName;
    private Class<?> binding;
    private String attributeType;
    private String attributeTypeName;
    private String attributeGeomTypeName;
    private Integer coordDimension = null;
    private Integer srId = null;
    private Integer precision = null;
    private Boolean mandatory = null;
    private Boolean geometry = false;
    
    public String getAttributeName() {
        return attributeName;
    }
    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }
    public Class<?> getBinding() {
        return binding;
    }
    public void setBinding(Class<?> binding) {
        this.binding = binding;
    }
    public String getAttributeType() {
        return attributeType;
    }
    public void setAttributeType(String attributeType) {
        this.attributeType = attributeType;
    }
    public String getAttributeTypeName() {
        return attributeTypeName;
    }
    public void setAttributeTypeName(String attributeTypeName) {
        this.attributeTypeName = attributeTypeName;
    }
    public String getAttributeGeomTypeName() {
        return attributeGeomTypeName;
    }
    public void setAttributeGeomTypeName(String attributeGeomTypeName) {
        this.attributeGeomTypeName = attributeGeomTypeName;
    }
    public Integer getCoordDimension() {
        return coordDimension;
    }
    public void setCoordDimension(Integer coordDimension) {
        this.coordDimension = coordDimension;
    }
    public Integer getSrId() {
        return srId;
    }
    public void setSrId(Integer srId) {
        this.srId = srId;
    }
    public Integer getPrecision() {
        return precision;
    }
    public void setPrecision(Integer precision) {
        this.precision = precision;
    }
    public Boolean getMandatory() {
        return mandatory;
    }
    public void isMandatory(Boolean mandatory) {
        this.mandatory = mandatory;
    }
    public Boolean isGeometry() {
        return geometry;
    }
    public void setGeometry(Boolean geometry) {
        this.geometry = geometry;
    }
    
}
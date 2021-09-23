package fdbs.sql.meta;

import java.sql.ResultSet;
import java.sql.SQLException;

public class BinaryConstraint extends Constraint{
    private int binaryConstraintId;
    private BinaryType binaryType;
    private String leftColumnName;
    private String leftColumnType;
    private String rightLiteralValue;
    private String rightLiteralType;
    private ComparisonOperator operator;
    private String binaryOperator;
    
    public BinaryConstraint(ResultSet constraint) throws SQLException {
        super(constraint);
        switch(constraint.getString("binary_type")){
            case "a":
                this.binaryType = BinaryType.attribute_attribute;
                break;
            case "c":
                this.binaryType = BinaryType.attribute_constant;
                break;
            default:
                throw new SQLException(String.format("%s: Unsupported BinaryConstraintType.", this.getClass().getSimpleName()));
        }
        
        switch(constraint.getString("operator"))
        {
            case "EQ":
                this.operator = ComparisonOperator.equals;
                binaryOperator = "=";
                break;
            case "NEQ":
                this.operator = ComparisonOperator.notequals;
                binaryOperator = "!=";
                break;
            case "LT":
                this.operator = ComparisonOperator.lower;
                binaryOperator = "<";
                break;
            case "GT":
                this.operator = ComparisonOperator.greater;
                binaryOperator = ">";
                break;
            case "LEQ":
                this.operator = ComparisonOperator.lowerequal;
                binaryOperator = "<=";
                break;
            case "GEQ":
                this.operator = ComparisonOperator.greaterequal;
                binaryOperator = ">=";
                break;
            default:
                throw new SQLException(String.format("%s: Unsupported BinaryConstraintOperator.", this.getClass().getSimpleName()));
        }
        
        this.leftColumnName = constraint.getString("left_column_name");
        this.leftColumnType = constraint.getString("left_column_type");
        this.rightLiteralValue = constraint.getString("right_literal_value");
        this.rightLiteralType = constraint.getString("right_literal_type");
    }

    public ComparisonOperator getOperator() {
        return operator;
    }

    public void setOperator(ComparisonOperator operator) {
        this.operator = operator;
    }

    public int getBinaryConstraintId() {
        return binaryConstraintId;
    }

    public void setBinaryConstraintId(int binaryConstraintId) {
        this.binaryConstraintId = binaryConstraintId;
    }

    public BinaryType getBinaryType() {
        return binaryType;
    }

    public void setBinaryType(BinaryType binaryType) {
        this.binaryType = binaryType;
    }

    public String getLeftColumnName() {
        return leftColumnName;
    }

    public void setLeftColumnName(String leftColumnName) {
        this.leftColumnName = leftColumnName;
    }

    public String getLeftColumnType() {
        return leftColumnType;
    }

    public void setLeftColumnType(String leftColumnType) {
        this.leftColumnType = leftColumnType;
    }

    public String getRightLiteralValue() {
        return rightLiteralValue;
    }

    public void setRightLiteralValue(String rightLiteralValue) {
        this.rightLiteralValue = rightLiteralValue;
    }

    public String getRightLiteralType() {
        return rightLiteralType;
    }

    public void setRightLiteralType(String rightLiteralType) {
        this.rightLiteralType = rightLiteralType;
    }

    public String getBinaryOperator() {
        return binaryOperator;
    }

    public void setBinaryOperator(String binaryOperator) {
        this.binaryOperator = binaryOperator;
    }

    @Override
    public String toString() {
        return "BinaryConstraint{" + "binaryConstraintId=" + binaryConstraintId + ", binaryType=" + binaryType + ", leftColumnName=" + leftColumnName + ", leftColumnType=" + leftColumnType + ", rightLiteralValue=" + rightLiteralValue + ", rightLiteralType=" + rightLiteralType + ", operator=" + operator + ", binaryOperator=" + binaryOperator + '}';
    }
}

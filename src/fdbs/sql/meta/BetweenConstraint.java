package fdbs.sql.meta;

import java.sql.ResultSet;
import java.sql.SQLException;

public class BetweenConstraint extends Constraint{
    private int betweenConstraintId;
    private String minValue;
    private String minType;
    private String maxValue;
    private String maxType;


    public BetweenConstraint(ResultSet constraint) throws SQLException {
        super(constraint);
        betweenConstraintId = constraint.getInt("between_constraint_id");
        minType = constraint.getString("min_type");
        maxType = constraint.getString("max_type");
        minValue = constraint.getString("min_value");
        maxValue = constraint.getString("max_value");
    }
    
    public int getBetweenConstraintId() {
        return betweenConstraintId;
    }

    public void setBetweenConstraintId(int betweenConstraintId) {
        this.betweenConstraintId = betweenConstraintId;
    }

    public String getMinValue() {
        return minValue;
    }

    public void setMinValue(String minValue) {
        this.minValue = minValue;
    }

    public String getMaxValue() {
        return maxValue;
    }

    /**
     * Set MaxValue of BetweenConstraint
     * @param maxValue The max balue.
     */
    public void setMaxValue(String maxValue) {
        this.maxValue = maxValue;
    }

    public String getMinType() {
        return minType;
    }

    public void setMinType(String minType) {
        this.minType = minType;
    }

    public String getMaxType() {
        return maxType;
    }

    public void setMaxType(String maxType) {
        this.maxType = maxType;
    }

    @Override
    public String toString() {
        return "BetweenConstraint{" + "betweenConstraintId=" + betweenConstraintId + ", minValue=" + minValue + ", minType=" + minType + ", maxValue=" + maxValue + ", maxType=" + maxType + '}';
    }
}

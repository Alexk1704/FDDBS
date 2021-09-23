package fdbs.sql.fedresultset.select.groupbystatement;

public class GroupResult {
    
    private int count;
    private int sum;
    private int max;
    
    public GroupResult() {
        count = 0;
        sum = 0;
        max = 0;
    }
    
    public int getCount() {
        return count;
    }
    
    public int getSum() {
        return sum;
    }
    
    public int getMax() {
        return max;
    }
    
    public void setCount(int count) {
        this.count = count;
    }
    
    public void setSum(int sum) {
        this.sum = sum;
    }
    
    public void setMax(int max) {
        this.max = max;
    }
}

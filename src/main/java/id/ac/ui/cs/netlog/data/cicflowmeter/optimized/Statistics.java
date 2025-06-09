package id.ac.ui.cs.netlog.data.cicflowmeter.optimized;

import lombok.Data;

@Data
public class Statistics {
    private Long count;
    private Long sum;
    private Double curAvg;
    private Double curVar;
    private Double lastAvg;
    private Double lastVar;
    private Long degree;
    private Long min;
    private Long max;

    public Statistics() {
        this.count = 0L;
        this.sum = 0L;
        this.curAvg = 0.0;
        this.curVar = 0.0;
        this.lastAvg = 0.0;
        this.lastVar = 0.0;
        this.degree = 1L;
        this.min = Long.MAX_VALUE;
        this.max = Long.MIN_VALUE;
    }

    public void add(Long num) {
        if (this.count == 0) {
            this.count++;
            this.sum += num;
            this.curAvg = num.doubleValue();
            this.curVar = 0.0;
            this.min = num;
            this.max = num;
            return;
        }

        this.lastAvg = this.curAvg;
        this.lastVar = this.curVar;

        this.count++;
        this.sum += num;
        this.curAvg = this.sum / this.count.doubleValue();
        this.curVar = ((double) (
            ((this.count - 1 - this.degree) * this.lastVar)
            + ((this.count - 1) * (this.lastAvg - this.curAvg) * (this.lastAvg - this.curAvg))
            + ((num - this.curAvg) * (num - this.curAvg))
        )) / ((double) (this.count - this.degree));
        this.min = Math.min(this.min, num);
        this.max = Math.max(this.max, num);
    }

    public Long calculateCount() {
        return this.count;
    }

    public Double calculateAvg() {
        return this.curAvg;
    }

    public Long calculateSum() {
        return this.sum;
    }

    public Double calculateVariance() {
        return this.curVar;
    }

    public Double calculateStd() {
        return Math.sqrt(this.calculateVariance());
    }

    public Long calculateMin() {
        return this.min;
    }

    public Long calculateMax() {
        return this.max;
    }
}

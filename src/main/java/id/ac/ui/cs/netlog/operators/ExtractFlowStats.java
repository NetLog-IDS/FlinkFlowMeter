package id.ac.ui.cs.netlog.operators;

import org.apache.flink.api.common.functions.MapFunction;

import id.ac.ui.cs.netlog.data.cicflowmeter.Flow;
import id.ac.ui.cs.netlog.data.cicflowmeter.FlowStats;

public class ExtractFlowStats implements MapFunction<Flow, FlowStats> {
    @Override
    public FlowStats map(Flow flow) throws Exception {
        FlowStats stats = new FlowStats(flow);
        // System.out.println(stats);
        return stats;
    }
}

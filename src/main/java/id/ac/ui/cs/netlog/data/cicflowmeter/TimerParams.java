package id.ac.ui.cs.netlog.data.cicflowmeter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@AllArgsConstructor
public class TimerParams {
    private Long timestamp;
    private String flowId;
}

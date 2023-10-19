package spendreport;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

public class FraudDetector extends KeyedProcessFunction<Long, Tuple2<Long, Double>, Alert> {

	private static final long serialVersionUID = 1L;
	private static final Double THRESHOLD = 10000.0;

	private transient ValueState<Double> sumState;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		ValueStateDescriptor<Double> sumStateDescriptor = new ValueStateDescriptor<>(
				"transaction-sum",
				Double.class
		);
		sumState = getRuntimeContext().getState(sumStateDescriptor);
	}

	@Override
	public void processElement(Tuple2<Long, Double> transaction, Context context, Collector<Alert> out) throws Exception {
		Double currentSum = sumState.value();
		if (currentSum == null) {
			currentSum = 0.0;
		}
		currentSum += transaction.f1;
		sumState.update(currentSum);
		if (currentSum > THRESHOLD) {
			out.collect(new Alert());
			sumState.clear();
		}
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
		// Do nothing
	}
}
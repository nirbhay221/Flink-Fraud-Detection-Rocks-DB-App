package spendreport;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;

import java.util.ArrayList;
import java.util.List;

public class FraudDetectorSliding extends KeyedProcessFunction<Long, Tuple2<Long, Double>, Alert> {

	private static final long serialVersionUID = 1L;
	private static final Double THRESHOLD = 10000.0;
	private static final int WINDOW_SIZE = 5; // In seconds

	private transient ValueState<Double> sumState;
	private transient ValueState<Integer> countState;

	@Override
	public void open(Configuration parameters) throws Exception {
		ValueStateDescriptor<Double> sumDescriptor = new ValueStateDescriptor<>("sum", Double.class);
		sumState = getRuntimeContext().getState(sumDescriptor);

		ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<>("count", Integer.class);
		countState = getRuntimeContext().getState(countDescriptor);
	}

	@Override
	public void processElement(Tuple2<Long, Double> transaction, Context context, Collector<Alert> collector) throws Exception {
		Double currentSum = sumState.value();
		if (currentSum == null) {
			currentSum = 0.0;
		}

		Integer currentCount = countState.value();
		if (currentCount == null) {
			currentCount = 0;
		}

		Double newSum = currentSum + transaction.f1;
		Integer newCount = currentCount + 1;

		sumState.update(newSum);
		countState.update(newCount);

		if (newSum / newCount > THRESHOLD) {
			collector.collect(new Alert());
			sumState.clear();
			countState.clear();
		}
//		System.out.println("sumState : "+sumState.value());
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext context, Collector<Alert> out) throws Exception {
		Double currentSum = sumState.value();
		Integer currentCount = countState.value();
		if (currentCount != null && currentSum != null && currentCount > 0) {
			Double average = currentSum / currentCount;
			if (average > THRESHOLD) {
				out.collect(new Alert());
			}
		}
		sumState.clear();
		countState.clear();
	}
}

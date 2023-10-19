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

public class FraudDetector extends KeyedProcessFunction<Long, Tuple2<Long, Double>, Alert> {

	private static final long serialVersionUID = 1L;
	private static final Double THRESHOLD = 10000.0;
	private static final int WINDOW_SIZE = 5; // In seconds

	private transient ListState<Tuple2<Long, Double>> recordBufferState;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		ListStateDescriptor<Tuple2<Long, Double>> recordBufferStateDescriptor = new ListStateDescriptor<>(
				"record-buffer",
				TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {})
		);
		recordBufferState = getRuntimeContext().getListState(recordBufferStateDescriptor);
	}

	@Override
	public void processElement(Tuple2<Long, Double> transaction, Context context, Collector<Alert> out) throws Exception {
		recordBufferState.add(transaction); // Add record to buffer

		//timer to trigger window
		context.timerService().registerEventTimeTimer(context.timestamp() + WINDOW_SIZE * 1000);
	}

	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
		// Retrieve records from buffer that fall within sliding window interval bounds
		List<Double> transactions = new ArrayList<>();
		for (Tuple2<Long, Double> record : recordBufferState.get()) {
			if (record.f0 >= timestamp - (WINDOW_SIZE * 1000) && record.f0 <= timestamp) {
				transactions.add(record.f1);
			}
		}

		// the mean of the transaction amounts
		Double sum = 0.0;
		for (Double transaction : transactions) {
			sum += transaction;
		}
		Double mean = sum / transactions.size();

		// Check if mean exceeds threshold
		if (mean > THRESHOLD) {
			out.collect(new Alert());
		}

		// Remove records that are outside the sliding window interval
		List<Tuple2<Long, Double>> recordsToRemove = new ArrayList<>();
		for (Tuple2<Long, Double> record : recordBufferState.get()) {
			if (record.f0 < timestamp - (WINDOW_SIZE * 1000)) {
				recordsToRemove.add(record);
			}
		}
		for (Tuple2<Long, Double> record : recordsToRemove) {
			recordBufferState.clear();
		}
	}

	@Override
	public void close() throws Exception {
		recordBufferState.clear();
	}
}
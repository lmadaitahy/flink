package org.apache.flink.streaming.api.datastream;

import org.apache.flink.streaming.api.operators.OneInputStreamOperator;

public interface InputParaSettable<IN, OUT> extends OneInputStreamOperator<IN, OUT> {
	void setInputPara(int p);
	void setName(String name);
}

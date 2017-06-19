package org.apache.flink.streaming.runtime.gg;

// Marker interface for preventing Flink of closing the operator when the inputs run out.
public interface NoAutoClose {
}

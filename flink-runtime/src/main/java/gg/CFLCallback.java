package gg;

import java.util.List;

public interface CFLCallback {

	// Note: always only one element is added
	void notify(List<Integer> newCFL);

	void notifyTerminalBB();
}

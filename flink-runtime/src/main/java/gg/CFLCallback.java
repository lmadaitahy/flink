package gg;

import java.util.List;

public interface CFLCallback {

	// Note: always only one element is added
	void notify(List<Integer> newCFL);

	void notifyTerminalBB();

	void notifyCloseInput(BagID bagID); // todo: az implementacio-kor majd figyelni kell, hogy a ket input lehet ugyanaz a bag is
}

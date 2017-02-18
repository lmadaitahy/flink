package gg;

public class CFLElement {

	public int seqNum;
	public int bbId;

	public CFLElement(int seqNum, int bbId) {
		this.seqNum = seqNum;
		this.bbId = bbId;
	}

	@Override
	public String toString() {
		return "CFLElement{" +
				"seqNum=" + seqNum +
				", bbId=" + bbId +
				'}';
	}
}

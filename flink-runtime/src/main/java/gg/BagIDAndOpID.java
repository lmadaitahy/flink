package gg;

public class BagIDAndOpID {

	public BagID bagID;
	public int opID;

	public BagIDAndOpID(BagID bagID, int opID) {
		this.bagID = bagID;
		this.opID = opID;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		BagIDAndOpID that = (BagIDAndOpID) o;

		if (opID != that.opID) return false;
		return bagID.equals(that.bagID);
	}

	@Override
	public int hashCode() {
		int result = bagID.hashCode();
		result = 31 * result + opID;
		return result;
	}
}

package gg;

public class BagID {

    public int cflSize;
    public int opID;

    public BagID() {}

    public BagID(int cflSize, int opID) {
        this.cflSize = cflSize;
        this.opID = opID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BagID bagID = (BagID) o;

        if (cflSize != bagID.cflSize) return false;
        if (opID != bagID.opID) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = cflSize;
        result = 31 * result + opID;
        return result;
    }
}

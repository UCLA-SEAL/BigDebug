package newt.actor;

public interface LocatableDataType<U, T> extends ProvenanceDataType<T>, Comparable<U> {
    public int overlaps( LocatableDataType l );
}

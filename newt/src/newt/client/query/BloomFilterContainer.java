package newt.client.query;

public class BloomFilterContainer implements ProvenanceContainer {
    
    public static final double falsePositiveProbability = 0.0000001;
    
    private BloomFilter<RecordSpecification> bfilter;

    public BloomFilterContainer(int expectedNumberOfElements) {
	bfilter = new BloomFilter<RecordSpecification>(
		falsePositiveProbability, expectedNumberOfElements);
    }
    
    public void add(RecordSpecification r) {
	bfilter.add(r);
    }
    
    public boolean contains(RecordSpecification r) {
	return bfilter.contains(r);
    }

}

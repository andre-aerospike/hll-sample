import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Log.Level;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.operation.HLLOperation;
import com.aerospike.client.operation.HLLPolicy;

public class SimpleCountingHLL extends ExampleBase
{
	// Add dataset to a HLL.
	void AddData(String groupName, List<Value> group, String binName, int logbits, int hashbits)
	{
		Key key = new Key(m_namespace, m_set, groupName);
		Bin bin = new Bin("group", groupName);
		Operation[] ops = new Operation[]
		{
			HLLOperation.add(HLLPolicy.Default, binName, group, logbits, hashbits),
			Operation.put(bin),
		};

		m_console.info("-- AddData --\t%s\tbin:%s\t(%d, %d)", groupName, binName, logbits, hashbits);
		Record record = m_client.operate(null, key, ops);
		m_console.debug("  Operate result: " + record);
		m_console.debug("  Entries that updated registers: %d", record.getLong(binName));
	}


	// Print count from HLL bin.
	void CountUsingHLL(String groupName, String binName)
	{
		Key key = new Key(m_namespace, m_set, groupName);
		Operation[] ops = new Operation[]
		{
			HLLOperation.getCount(binName),
			Operation.get(binName),
		};

		Record record = m_client.operate(null, key, ops);
		m_console.info("-- Get Count --\t%s\tbin:%s", groupName, binName);
		m_console.debug("  Operate result: " + record);

		List<?> results = record.getList(binName);
		long count = (Long)results.get(0);
		Value hllval = (Value)results.get(1);
		m_console.write("Using HLL (%s bits)\t%s\tcount:%d\tmemory:%d", binName, groupName, count, hllval.estimateSize());
	}

	// Get the HLL Value.
	Value GetHLLValue(String groupName, String binName)
	{
		Key key = new Key(m_namespace, m_set, groupName);
		Operation[] ops = new Operation[]
		{
			Operation.get(binName),
		};

		Record record = m_client.operate(null, key, ops);
		m_console.info("-- Get HLL Value --\t%s\tbin:%s", groupName, binName);
		m_console.debug("  Operate result: " + record);

		return record.getHLLValue(binName);
	}


	// Get HLL records
	List<Value.HLLValue> GetHLLValues(String binName)
	{
		List<Value.HLLValue> hllvalues = new ArrayList<Value.HLLValue>();
		Key[] keys = new Key[]
				{
						new Key(m_namespace, m_set, "A"),
						new Key(m_namespace, m_set, "B"),
						new Key(m_namespace, m_set, "C"),
						new Key(m_namespace, m_set, "D"),
						new Key(m_namespace, m_set, "E"),
				};
		Record[] records = m_client.get(null, keys, binName);
		for (int i=0; i<records.length; i++)
		{
			hllvalues.add(records[i].getHLLValue(binName));
		}
		return hllvalues;
	}

	// Get count from HLL bin.
	void GetIntersectCount(String groupName, String binName)
	{
		Key key = new Key(m_namespace, m_set, groupName);
		Operation[] ops = new Operation[]
		{
			HLLOperation.getCount(binName),
			Operation.get(binName),
		};

		Record record = m_client.operate(null, key, ops);
		m_console.info("-- Get Count --\t%s\tbin:%s", groupName, binName);
		m_console.debug("  Operate result: " + record);

		List<?> results = record.getList(binName);
		long count = (Long)results.get(0);
		Value hllval = (Value)results.get(1);
		m_console.write("Using HLL (%s bits)\t%s\tcount:%d\tmemory:%d", binName, groupName, count, hllval.estimateSize());
	}



	// Create a HLL to estimate the cardinality of a dataset.
	void EstimateCardinality() throws InterruptedException
	{
		String binName = "12-0";

		// Initialise dataset
		List<Value> A = GetGroup("A");
		CountUsingHashSet("A", A);

		// Add dataset to HLL record, then estimate cardinality
		AddData("A", A, binName, 12, 0);
		CountUsingHLL("A", binName);
	}

	void Run2() throws InterruptedException
	{
		String binName = "12-4";

		// Initialise datasets
		List<Value> A = GetGroup("A");
		List<Value> B = GetGroup("B");
		List<Value> C = GetGroup("C");
		List<Value> D = GetGroup("D");
		List<Value> E = GetGroup("E");

		// Add datasets to HLL record
		AddData("A", A, binName, 12, 0);
		AddData("B", B, binName, 12, 4);
		AddData("C", C, binName, 12, 4);
		AddData("D", D, binName, 12, 4);
		AddData("E", E, binName, 12, 4);

		// Get number of unique items in each dataset
		CountUsingHashSet("A", A);
		CountUsingHLL("A", binName);
		CountUsingHashSet("B", B);
		CountUsingHLL("B", binName);
		CountUsingHashSet("C", C);
		CountUsingHLL("C", binName);
		CountUsingHashSet("D", D);
		CountUsingHLL("D", binName);
		CountUsingHashSet("E", E);
		CountUsingHLL("E", binName);

		// Get HLL records
		List<Value.HLLValue> hllvalues = GetHLLValues(binName);

		/*
		List<Value.HLLValue> hllvalues = new ArrayList<Value.HLLValue>();
		Key[] keys = new Key[]
				{
						new Key(m_namespace, m_set, "A"),
						new Key(m_namespace, m_set, "B"),
						new Key(m_namespace, m_set, "C"),
						new Key(m_namespace, m_set, "D"),
						new Key(m_namespace, m_set, "E"),
				};
		Record[] records = m_client.get(null, keys, binName);
		for (int i=0; i<records.length; i++)
		{
			hllvalues.add(records[i].getHLLValue(binName));
		}
		//m_console.write("Got " + records[0]);
		//hllvalues.add(e)
		*/
	}


	void SummitHLL() throws InterruptedException
	{
		String groupName = "A";

		// Initialise dataset
		List<Value> A = GetGroup(groupName);
		CountUsingHashSet(groupName, A);

		String binName = "HLL-12";
		int logbits = 12;
		int hashbits = 0;

		// Add dataset to HLL record, then estimate cardinality
		Key key = new Key(m_namespace, m_set, groupName);
		Operation[] ops = new Operation[] {
			HLLOperation.add(HLLPolicy.Default, binName, A, logbits, hashbits),
			HLLOperation.getCount(binName),
		};
		Record record = m_client.operate(null, key, ops);

		List<?> results = record.getList(binName);
		long count = (Long)results.get(1);
		m_console.write("Using HLL (%s bits)\t%s\tcount:%d", binName, groupName, count);
	}


	@Override
	boolean Run() throws InterruptedException
	{
		m_console.setLevel(Level.WARN);
		m_client.truncate(null,  m_namespace, null, null);

		SummitHLL();
		EstimateCardinality();

		return true;
	}
}

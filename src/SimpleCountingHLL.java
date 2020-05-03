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
	// Add dataset to a HLL
	void AddData(String groupName, List<Value> group, String binName, int logbits, int hashbits)
	{
		Key key = new Key(m_namespace, m_set, groupName);
		Bin bin = new Bin("group", groupName);
		Operation[] ops = new Operation[]
		{
			HLLOperation.add(HLLPolicy.Default, binName, group, logbits, hashbits),
			Operation.put(bin),
		};

		Record record = m_client.operate(m_writePolicy, key, ops);
		m_console.debug("-- Group %s --\tadd bin %s (%d, %d)", groupName, binName, logbits, hashbits);
		m_console.debug("  Operate result: " + record);
	}


	// Get count from HLL bin
	void GetCount(String groupName, String binName)
	{
		Key key = new Key(m_namespace, m_set, groupName);
		Operation[] ops = new Operation[]
		{
			//HLLOperation.init(HLLPolicy.Default, binName, 16, 4),
			//HLLOperation.add(HLLPolicy.Default, binName, group),
			HLLOperation.getCount(binName),
			HLLOperation.getCount(binName),
		};

		Record record = m_client.operate(m_writePolicy, key, ops);
		m_console.debug("  Operate result: " + record);

		List<?> result_list = record.getList(binName);
		/*
		long count1 = (Long)result_list.get(2);
		long count2 = (Long)result_list.get(3);
		List<?> description = (List<?>)result_list.get(4);
		*/

		/*
		m_console.write("Count1 - " + count1);
		m_console.write("Count2 - " + count2);
		m_console.write("Desc - " + description);
		m_console.write(result_list.toString());
		*/
		m_console.debug(result_list.toString());

		long count = (Long)result_list.get(0);
		m_console.info("-- Group %s --\tHLL Count: %d", groupName, count);
		m_console.info("  Operate result: " + record);
	}

	@Override
	boolean Run()
	{
		m_console.setLevel(Level.WARN);

		List<Value> A = GetGroup("A");
		List<Value> B = GetGroup("B");
		List<Value> C = GetGroup("C");
		List<Value> D = GetGroup("D");
		List<Value> E = GetGroup("E");

		PrintStats("A", A);
		AddData("A", A, "4-4", 4, 4);
		AddData("B", B, "4-4", 4, 4);
		AddData("C", C, "4-4", 4, 4);
		AddData("D", D, "4-4", 4, 4);
		AddData("E", E, "4-4", 4, 4);
		AddData("A", A, "16-4", 16, 4);
		AddData("B", B, "16-4", 16, 4);
		AddData("C", C, "16-4", 16, 4);
		AddData("D", D, "16-4", 16, 4);
		AddData("E", E, "16-4", 16, 4);

		GetCount("A", "4-4");
		GetCount("A", "16-4");
		GetCount("B", "4-4");
		GetCount("C", "4-4");
		GetCount("D", "4-4");
		GetCount("E", "4-4");

		return true;
	}

}

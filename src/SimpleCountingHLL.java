import java.util.List;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.operation.HLLOperation;
import com.aerospike.client.operation.HLLPolicy;

public class SimpleCountingHLL extends ExampleBase
{
	// Add dataset to a HLL
	void GetCount(String groupName, List<Value> group)
	{
		Key key = new Key(m_namespace, m_set, groupName);
		String binName = "hll";
		Operation[] ops = new Operation[]
		{
			HLLOperation.init(HLLPolicy.Default, binName, 16, 4),
			HLLOperation.add(HLLPolicy.Default, binName, group),
			HLLOperation.getCount(binName),
			HLLOperation.refreshCount(binName),
			HLLOperation.describe(binName),
		};

		Record record = m_client.operate(m_writePolicy, key, ops);
		m_console.info("Read - " + record);
		List<?> result_list = record.getList(binName);
		long count1 = (Long)result_list.get(2);
		long count2 = (Long)result_list.get(3);
		List<?> description = (List<?>)result_list.get(4);

		m_console.info("Count1 - " + count1);
		m_console.info("Count2 - " + count2);
		m_console.info("Desc - " + description);
		m_console.info(result_list.toString());
	}

	@Override
	boolean Run()
	{
		List<Value> A = GetGroup("A");
		List<Value> B = GetGroup("B");
		List<Value> C = GetGroup("C");
		List<Value> D = GetGroup("D");
		List<Value> E = GetGroup("E");

		GetCount("A", A);
		GetCount("B", B);
		GetCount("C", C);
		GetCount("D", D);
		GetCount("E", E);

		return true;
	}

}

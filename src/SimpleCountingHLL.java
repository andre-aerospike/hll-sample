import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.Value.IntegerValue;
import com.aerospike.client.operation.HLLOperation;
import com.aerospike.client.operation.HLLPolicy;

public class SimpleCountingHLL extends ExampleBase
{
	public SimpleCountingHLL()
	{
		super("s1");
	}

	enum GroupType
	{
		Even,
		Odd,
		All,
	}


	class GroupParams
	{
		int m_count;
		GroupType m_type;
		int	m_min;
		int m_max;
		boolean m_hide_range;

		public GroupParams(int count, GroupType type, int min, int max, boolean hide_range)
		{
			m_count = count;
			m_type = type;
			m_min = min;
			m_max = max;
			m_hide_range = hide_range;
		}
	}

	int m_MAX    =  100000;
	int m_SIZE   = 1000000; // Number of elements in each group

	Entry<String, GroupParams> e;
	final Map<String, GroupParams> m_groupdef = Map.ofEntries(
			Map.entry("A", new GroupParams(m_SIZE,		GroupType.Even,		0,	m_MAX,		false)),
			Map.entry("B", new GroupParams(m_SIZE,		GroupType.Odd,		0,	m_MAX,		false)),
			Map.entry("C", new GroupParams(m_SIZE,		GroupType.All,		0,	m_MAX/2,	true)),
			Map.entry("D", new GroupParams(m_SIZE/20,	GroupType.All,	 4000,	8000,		true))
			);

	int GetValue(GroupType type, int min, int max)
	{
		int x = m_random.nextInt(max - min) + min;
		switch (type)
		{
		case Odd:
			x = x | 1;
			break;
		case Even:
			x = x & ~1;
			break;
		case All:
		default:
			break;
		}
		return x;
	}


	List<Value> GetGroup(String name)
	{
		GroupParams params = m_groupdef.get(name);

		List<Value> list = new ArrayList<>();
		int i=0;
		if (params.m_hide_range)
		{
			list.add(new IntegerValue(m_MAX-1));
			list.add(new IntegerValue(0));
			i += 2;
		}
		for (; i<params.m_count; i++)
		{
			list.add(new IntegerValue(GetValue(params.m_type, params.m_min, params.m_max)));
		}
		return list;
	}

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

		GetCount("A", A);
		GetCount("B", B);
		GetCount("C", C);
		GetCount("D", D);

		return true;
	}

}

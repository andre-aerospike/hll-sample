//import java.util.List;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.Value.IntegerValue;
import com.aerospike.client.policy.WritePolicy;

import common.Console;

public abstract class ExampleBase
{
	String m_host = "192.168.56.10";
	int m_port = 3000;
	String m_namespace = "test";
	String m_set = "set";

	Console m_console;
	WritePolicy m_writePolicy;
	AerospikeClient m_client;

	Random m_random;

	public ExampleBase()
	{
        m_client = new AerospikeClient(m_host, m_port);
		m_console = new Console();
		m_writePolicy = new WritePolicy();

		m_random = new Random();
		m_random.setSeed(12345);	// So we can get consistent runs

	}



	// Data generation modelling
	enum GroupType
	{
		Even,
		Odd,
		All,
	}

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


	// Dataset definitions
	final class GroupParams
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
			//		  name				   count		values			min		max			hide range
			Map.entry("A", new GroupParams(m_SIZE,		GroupType.All,		0,	m_MAX,		false)),
			Map.entry("B", new GroupParams(m_SIZE,		GroupType.Even,		0,	m_MAX,		false)),
			Map.entry("C", new GroupParams(m_SIZE,		GroupType.Odd,		0,	m_MAX,		false)),
			Map.entry("D", new GroupParams(m_SIZE,		GroupType.All,		0,	m_MAX/2,	true)),
			Map.entry("E", new GroupParams(m_SIZE/20,	GroupType.All,	 4000,	8000,		true))
			);

	// Get (generate) the data for a named group dataset
	List<Value> GetGroup(String name)
	{
		GroupParams params = m_groupdef.get(name);

		List<Value> list = new ArrayList<>();
		int i=0;
		if (params.m_hide_range)
		{
			list.add(new IntegerValue(0));
			list.add(new IntegerValue(m_MAX-1));
			i += 2;
		}
		for (; i<params.m_count; i++)
		{
			list.add(new IntegerValue(GetValue(params.m_type, params.m_min, params.m_max)));
		}
		return list;
	}



	abstract boolean Run();



}

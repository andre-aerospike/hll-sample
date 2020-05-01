//import java.util.List;

import java.util.Random;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.WritePolicy;

import common.Console;

public abstract class ExampleBase
{
	String m_host = "192.168.56.10";
	int m_port = 3000;
	String m_namespace = "test";
	String m_set;

	Console m_console;
	WritePolicy m_writePolicy;
	AerospikeClient m_client;

	Random m_random;

	public ExampleBase(String set)
	{
        m_set = set;

        m_client = new AerospikeClient(m_host, m_port);
		m_console = new Console();
		m_writePolicy = new WritePolicy();

		m_random = new Random();
		m_random.setSeed(12345);	// So we can get consistent runs

	}

	abstract boolean Run();



}

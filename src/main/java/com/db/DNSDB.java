package com.db;

import com.db.bplustree.BPlusTreeFile;
import com.db.bplustree.KeyValuePair;

import java.io.*;
import java.util.List;
import java.util.Scanner;

public class DNSDB {

	private BPlusTreeIntToString20 hostNames;
	private BPlusTreeString60toInt ipAddresses;

	private static final String HOSTNAMES_FILE = "hostnames.b+tree";
	private static final String IPS_FILE = "ipds.b+tree";

	public DNSDB() {
		hostNames = new BPlusTreeIntToString20(HOSTNAMES_FILE,1024);
		ipAddresses = new BPlusTreeString60toInt(IPS_FILE,1024);
	}

	public static void main(String args[]){
		DNSDB dnsdb = new DNSDB();
		File file = new File("host-list.txt");
		long start = System.currentTimeMillis();
		dnsdb.load(file);
		try {
			dnsdb.flush();
		}catch (Exception e){
			e.printStackTrace();

		}

		long end = System.currentTimeMillis();
		System.out.println(end-start);
	}

	/**
	 * Loads all the host-IP pairs into the B+ trees.
	 *
	 * @param file
	 */
	public void load(File file) {
		if (!file.exists()) {
			System.out.println(file + " not found");
			return;
		}

		if (new File(HOSTNAMES_FILE).length() > BPlusTreeFile.BLOCK_SIZE && new File(IPS_FILE).length() > BPlusTreeFile.BLOCK_SIZE) {
			System.out.println("Tree found on disk. No need to reconstruct");
			return;
		}

		BufferedReader data;
		System.out.println("Loading....");
		try {
			data = new BufferedReader(new FileReader(file));
			while (true) {
				String line = data.readLine();
				if (line == null) {
					break;
				}
				String[] pair = line.split("\t");
				String host = pair[0];
				int IP = stringToIP(pair[1]);
				hostNames.put(IP, host);
				ipAddresses.put(host, IP);
			}
		} catch (IOException e) {
			System.out.println("Fail: " + e);
		}
		System.out.println("Loading Done");
	}

	public void flush() throws IOException {
		hostNames.flush();
		ipAddresses.flush();
	}

	/**
	 * Finds an IP address given the host name.
	 *
	 * @param hostName
	 * @return integer representation of an IP address, null if not found.
	 */
	public List<Integer> findIP(String hostName) {
		return ipAddresses.find(hostName);
	}

	/**
	 * Finds the host name given the IP address.
	 *
	 * @param ip
	 * @return null if not found
	 */
	public List<String> findHostName(int ip) {
		return hostNames.find(ip);
	}

	/**
	 * Tests whether the given IP-name pair is valid.
	 *
	 * @param ip
	 *            integer representation of an IP address
	 * @param hostName
	 * @return true if valid, false otherwise
	 */
	public boolean testPair(int ip, String hostName) {
		List<String> host = findHostName(ip);
		if (host == null) {
			List<Integer> foundIP = findIP(hostName);
			if (foundIP == null)
				return false;
			return foundIP.contains(ip);
		}
		else {
			return host.contains(hostName);
		}
	}

	/**
	 * Tests whether the given name-IP pair is valid.
	 *
	 * @param hostName
	 * @param ip
	 *            integer representation of an IP address
	 * @return true if valid, false otherwise
	 */
	public boolean testPair(String hostName, int ip) {
		List<Integer> foundIP = findIP(hostName);
		if (foundIP == null) {
			List<String> foundName = findHostName(ip);
			if (foundName == null)
				return false;
			return foundName.contains(hostName);
		}
		else {
			return foundIP.contains(ip);
		}
	}

	/**
	 * Adds an host-IP pair to the database (ie, to both B+ trees)
	 *
	 * @param hostName
	 * @param ip
	 * @return whether successfully added to the database
	 */
	public boolean add(String hostName, int ip) {
		return ipAddresses.put(hostName, ip) && hostNames.put(ip, hostName);
	}

	/**
	 * Prints (to System.out) all the pairs in the HostNames index.
	 */
	public void iterateAll() {
		for (KeyValuePair<Integer, String> entry : hostNames) {
			System.out.println(entry);
		}

		// System.out.println("Iterated " + entries.size() + " values");
	}

	// FOR MARKING! PLEASE DO NOT MODIFY.
	/**
	 * Look up all the values in the file. If the value is not present, then it
	 * will print that value to System.out.
	 */
	public void testAllPairs(File file) {
		try {
			Scanner scan = new Scanner(file);
			while (scan.hasNextLine()) {
				String[] line = scan.nextLine().split("\t");
				if (line.length != 2)
					continue;
				int ip = stringToIP(line[0].trim());
				String host = line[1].trim();
				if (!testPair(ip, host)) {
					System.out.println("Missing: " + IPToString(ip) + " -> " + host);
				}
				if (!testPair(host, ip)) {
					System.out.println("Missing: " + host + " -> " + IPToString(ip));
				}
			}
			scan.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	// Utilities

	public static Integer stringToIP(String text) {
		String[] bytes = text.trim().split("\\.");

		if (bytes.length != 4)
			return null;

		try {
			int ip = 0;
			for (int i = 0; i < 4; i++) {
				int b = Integer.parseInt(bytes[i].trim());
				ip |= b << (24 - 8 * i);
			}

			return ip;
		} catch (NumberFormatException e) {
			return null;
		}
	}

	public static String IPToString(int ip) {
		StringBuilder sb = new StringBuilder();
		for (int i = 3; i >= 0; i--) {
			sb.append((ip & (0xFF << (i * 8))) >> (i * 8));
			if (i > 0)
				sb.append('.');
		}
		return sb.toString();
	}
}

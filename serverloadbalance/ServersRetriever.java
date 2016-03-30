package net.floodlightcontroller.serverloadbalance;

import org.projectfloodlight.openflow.types.IPv4Address;

import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceListener;

public class ServersRetriever implements IDeviceListener{

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "AdvancedLoadBalancer";
	}

	@Override
	public boolean isCallbackOrderingPrereq(String type, String name) {
		
		return (name.equals("topology") || name.equals("devicemanager" )|| name.equals("arp"));
	}

	@Override
	public boolean isCallbackOrderingPostreq(String type, String name) {
		
		return (name.equals("forwarding"));
	}

	@Override
	public void deviceAdded(IDevice device) {
		System.out.println("device added"+device.getIPv4Addresses()[0]);
		for(IPv4Address s:device.getIPv4Addresses())
		{
			if(AdvancedLoadBalancer.Servers.contains(s))
			{
				AdvancedLoadBalancer.Device2IP.put(device.getDeviceKey(), s);
				if(!AdvancedLoadBalancer.ServerDevicesList.contains(device.getDeviceKey()))
					AdvancedLoadBalancer.ServerDevicesList.add(device.getDeviceKey());
			}
		}
	}

	@Override
	public void deviceRemoved(IDevice device) {
		System.out.println("device removed"+device.getIPv4Addresses()[0]);	
		AdvancedLoadBalancer.Device2IP.remove(device.getDeviceKey());
			AdvancedLoadBalancer.ServerDevicesList.remove(device.getDeviceKey());
			System.out.println("ServerDevicesList"+AdvancedLoadBalancer.ServerDevicesList);
			System.out.println("Device2IP"+AdvancedLoadBalancer.Device2IP);
	}

	@Override
	public void deviceMoved(IDevice device) {
		System.out.println("device moved"+device.getIPv4Addresses()[0]);
		for(IPv4Address s:device.getIPv4Addresses())
		{
			if(AdvancedLoadBalancer.Servers.contains(s))
			{
				//System.out.println(containing(device)+" "+ device.toString()+"\n"+AdvancedLoadBalancer.ServerDevicesList.peek().toString());
				
				if(AdvancedLoadBalancer.ServerDevicesList.contains(device.getDeviceKey()))	
				{
					AdvancedLoadBalancer.Device2IP.remove(device.getDeviceKey());
					AdvancedLoadBalancer.ServerDevicesList.remove(device.getDeviceKey());
				}
				else 
				{
					AdvancedLoadBalancer.Device2IP.put(device.getDeviceKey(), s);
					AdvancedLoadBalancer.ServerDevicesList.add(device.getDeviceKey());
				}
			}
		}
		System.out.println("ServerDevicesList"+AdvancedLoadBalancer.ServerDevicesList);
		System.out.println("Device2IP"+AdvancedLoadBalancer.Device2IP);
	}

	@Override
	public void deviceIPV4AddrChanged(IDevice device) {
		System.out.println("IP changed of device "+device.getIPv4Addresses()[0]);
		for(IPv4Address s:device.getIPv4Addresses())
		{
			if(AdvancedLoadBalancer.Servers.contains(s))
			{
				AdvancedLoadBalancer.Device2IP.put(device.getDeviceKey(), s);
				
				if(!AdvancedLoadBalancer.ServerDevicesList.contains(device.getDeviceKey()))
					AdvancedLoadBalancer.ServerDevicesList.add(device.getDeviceKey());
			}
		}
		if(AdvancedLoadBalancer.Device2IP.containsKey(device.getDeviceKey())) 
		{
			AdvancedLoadBalancer.Device2IP.remove(device.getDeviceKey());
			AdvancedLoadBalancer.ServerDevicesList.remove(device.getDeviceKey());
		}
		System.out.println("ServerDevicesList"+AdvancedLoadBalancer.ServerDevicesList);
		System.out.println("Device2IP"+AdvancedLoadBalancer.Device2IP);
	}

	@Override
	public void deviceIPV6AddrChanged(IDevice device) {
		return;
		
	}

	@Override
	public void deviceVlanChanged(IDevice device) {
		return;
		
	}

}

package net.floodlightcontroller.proxyarp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFPacketOut;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.IPv6Address;
import org.projectfloodlight.openflow.types.MacAddress;
import org.projectfloodlight.openflow.types.OFBufferId;
import org.projectfloodlight.openflow.types.OFPort;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.packet.ARP;
import net.floodlightcontroller.packet.Data;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPacket;
import net.floodlightcontroller.routing.IRoutingDecision;
import net.floodlightcontroller.routing.IRoutingDecision.RoutingAction;
import net.floodlightcontroller.routing.RoutingDecision;
import net.floodlightcontroller.topology.ITopologyService;

public class ProxyArp  implements IOFMessageListener, IFloodlightModule {
	protected static final long BROADCAST_MAC = 0xffffffffffffL;	
	protected IFloodlightProviderService floodlightProvider;
	protected IDeviceService deviceManager;
	protected ITopologyService topologyManager;
	protected IOFSwitchService switchManager;
	HashMap<IPv4Address, MacAddress> vmac;
	//SET VIRTUAL DEVICES LIST HERE
	protected void setVirtualDevices()
	{
		vmac.put(IPv4Address.of("10.0.2.1"), MacAddress.of("00:00:00:00:00:ff"));
	}
	protected class ARPRequest
	{
		private MacAddress sourceMACAddress;
		private IPv4Address sourceIPAddress;
		private MacAddress targetMACAddress;
		private IPv4Address targetIPAddress;
		private DatapathId switchId;
		private OFPort inPort;	
		public ARPRequest setSourceMACAddress(MacAddress sourceMACAddress) {
			this.sourceMACAddress = sourceMACAddress;
			return this;
		}
		public ARPRequest setSourceIPAddress(IPv4Address sourceIPAddress) {
			this.sourceIPAddress = sourceIPAddress;
			return this;
		}		
		public ARPRequest setTargetMACAddress(MacAddress targetMACAddress) {
			this.targetMACAddress = targetMACAddress;
			return this;
		}
		public ARPRequest setTargetIPAddress(IPv4Address targetIPAddress) {
			this.targetIPAddress = targetIPAddress;
			return this;
		}	
		public ARPRequest setSwitchId(DatapathId switchId) {
			this.switchId = switchId;
			return this;
		}
		public ARPRequest setInPort(OFPort portId) {
			this.inPort = portId;
			return this;
		}
		public MacAddress getSourceMACAddress() {
			return this.sourceMACAddress;
		}
		public IPv4Address getSourceIPAddress() {
			return this.sourceIPAddress;
		}
		public MacAddress getTargetMACAddress() {
			return this.targetMACAddress;
		}
		public IPv4Address getTargetIPAddress() {
			return this.targetIPAddress;
		}
		public   DatapathId getSwitchId() {
			return this.switchId;
		}
		public OFPort getInPort() {
			
			return this.inPort;
		}
		
	}

	@Override
	public String getName() {
		return "arp";
	}

	
	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		return (type.equals(OFType.PACKET_IN) && (name.equals("topology") || name.equals("devicemanager")));
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		return (type.equals(OFType.PACKET_IN) && name.equals("forwarding"));
	}
	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		return null;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l =
				new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		l.add(IDeviceService.class);
		l.add(ITopologyService.class);
		l.add(IOFSwitchService.class);
		return l;
	}

	@Override
	public void init(FloodlightModuleContext context) {
		floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
		topologyManager = context.getServiceImpl(ITopologyService.class);
		deviceManager = context.getServiceImpl(IDeviceService.class);
		switchManager=context.getServiceImpl(IOFSwitchService.class);	
		vmac=new HashMap<>();
		setVirtualDevices();
	}

	@Override
	public void startUp(FloodlightModuleContext context) {
		floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
		
	}

	@Override
	public net.floodlightcontroller.core.IListener.Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) 
	{
		switch (msg.getType()) 
		{
			case PACKET_IN:
				IRoutingDecision decision = null;
				if (cntx != null) 
				{
					decision = RoutingDecision.rtStore.get(cntx, IRoutingDecision.CONTEXT_DECISION);
					if(decision!=null && !(decision.getRoutingAction()==RoutingAction.FORWARD || decision.getRoutingAction()==RoutingAction.FORWARD_OR_FLOOD)) 
					return Command.CONTINUE;
				}
				return this.processPacketInMessage(sw, (OFPacketIn) msg, decision, cntx);
			default:
				break;
		}
		return Command.CONTINUE;		
	}
	
	
	protected Command processPacketInMessage(IOFSwitch sw, OFPacketIn piMsg, IRoutingDecision decision, FloodlightContext cntx) 
	{
		Ethernet ethPacket = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
		if (ethPacket.getEtherType()!=EthType.ARP)
			return Command.CONTINUE;
		
		ARP arp;
		
		if (ethPacket.getPayload() instanceof ARP) 
            arp = (ARP) ethPacket.getPayload(); 
		else return Command.CONTINUE;
		
		
		if (arp.isGratuitous()){return Command.STOP;}
		
		if (arp.getOpCode() == ARP.OP_REQUEST) {
			return this.handleARPRequest(arp, sw.getId(), piMsg.getInPort(), cntx);
		}
		
		// Handle ARP reply.
		if (arp.getOpCode() == ARP.OP_REPLY) {
			return Command.STOP;
		}
			
		return Command.CONTINUE;
	}

	protected Command handleARPRequest(ARP arp, DatapathId switchId, OFPort portId, FloodlightContext cntx) 
	{
		
		IPv4Address sourceIPAddress = (arp.getSenderProtocolAddress());
		MacAddress sourceMACAddress = (arp.getSenderHardwareAddress());
		IPv4Address targetIPAddress = (arp.getTargetProtocolAddress());
		MacAddress targetMACAddress = MacAddress.NONE;
		@SuppressWarnings("unchecked")
		Iterator<IDevice> diter = (Iterator<IDevice>) deviceManager.queryDevices(MacAddress.NONE, null,targetIPAddress, IPv6Address.NONE, DatapathId.NONE, OFPort.ZERO);	

		if (diter.hasNext()) 
	{
			
			// If we know the destination device, get the corresponding MAC address and send an ARP reply.
			IDevice device = diter.next();
			System.out.println(device);
			targetMACAddress = device.getMACAddress();
			if (targetMACAddress!=MacAddress.NONE) 
			{
					ARPRequest arpRequest = new ARPRequest()
					.setSourceMACAddress(sourceMACAddress)
					.setSourceIPAddress(sourceIPAddress)
					.setTargetMACAddress(targetMACAddress)
					.setTargetIPAddress(targetIPAddress)
					.setSwitchId(switchId)
					.setInPort(portId);
					// Send ARP reply.
					this.sendARPReply(arpRequest);
			}		
			
		} 
		else 
		{
					targetMACAddress=vmac.get(targetIPAddress);
					if(targetMACAddress!=null)
					{
						//System.out.println("in vmac portion");
					ARPRequest arpRequest = new ARPRequest()
					.setSourceMACAddress(sourceMACAddress)
					.setSourceIPAddress(sourceIPAddress)
					.setTargetMACAddress(targetMACAddress)
					.setTargetIPAddress(targetIPAddress)
					.setSwitchId(switchId)
					.setInPort(portId);
					// Send ARP reply.
					this.sendARPReply(arpRequest);}
			}
		return Command.STOP;
	}


	protected void sendARPReply(ARPRequest arpRequest) 
	{
		IPacket arpReply = new Ethernet()
    		.setSourceMACAddress((arpRequest.getTargetMACAddress()))
        	.setDestinationMACAddress((arpRequest.getSourceMACAddress()))
        	.setEtherType(EthType.ARP)
        	.setPayload(new ARP()
				.setHardwareType(ARP.HW_TYPE_ETHERNET)
				.setProtocolType(ARP.PROTO_TYPE_IP)
				.setOpCode(ARP.OP_REPLY)
				.setHardwareAddressLength((byte)6)
				.setProtocolAddressLength((byte)4)
				.setSenderHardwareAddress(arpRequest.getTargetMACAddress())
				.setSenderProtocolAddress(arpRequest.getTargetIPAddress())
				.setTargetHardwareAddress(arpRequest.getSourceMACAddress())
				.setTargetProtocolAddress(arpRequest.getSourceIPAddress())
				.setPayload(new Data(new byte[] {0x01})));
			sendPOMessage(arpReply, switchManager.getSwitch(arpRequest.getSwitchId()), arpRequest.getInPort());
		
		}
	



	protected void sendPOMessage(IPacket packet, IOFSwitch sw, OFPort port)
	{		
		OFPacketOut po = sw.getOFFactory().buildPacketOut()
			    .setData(packet.serialize())
			    .setBufferId(OFBufferId.NO_BUFFER)
			    .setActions(Collections.singletonList((OFAction)sw.getOFFactory().actions().output(port, 0xffFFffFF)))
			    .setInPort(OFPort.CONTROLLER)
			    .build();
          		sw.write(po);
	}
}
package net.floodlightcontroller.serverloadbalance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFlowAdd;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFPacketOut;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.action.OFActionOutput;
import org.projectfloodlight.openflow.protocol.action.OFActionSetDlDst;
import org.projectfloodlight.openflow.protocol.action.OFActionSetDlSrc;
import org.projectfloodlight.openflow.protocol.action.OFActionSetNwDst;
import org.projectfloodlight.openflow.protocol.action.OFActionSetNwSrc;
import org.projectfloodlight.openflow.protocol.action.OFActions;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.IpProtocol;
import org.projectfloodlight.openflow.types.MacAddress;
import org.projectfloodlight.openflow.types.OFBufferId;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.TransportPort;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.packet.TCP;

public class ServerLoadBalance implements IOFMessageListener, IFloodlightModule {
protected IFloodlightProviderService floodLightProvider;
protected static Server VirtualServer;
protected static Queue<Server> ServerList;
	@Override
	public String getName() {
		return "ServerLoadBalancer";
	}

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> s=new ArrayList<Class<? extends IFloodlightService>>();
		s.add(IFloodlightProviderService.class);
		return s;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
	ServerList=new LinkedList<Server>();
	ServerList.add(new Server(MacAddress.of("00:00:00:00:00:02"), IPv4Address.of("10.0.0.2"),2)); 
	ServerList.add(new Server(MacAddress.of("00:00:00:00:00:03"), IPv4Address.of("10.0.0.3"),3));
	ServerList.add(new Server(MacAddress.of("00:00:00:00:00:04"), IPv4Address.of("10.0.0.4"),4));
	floodLightProvider=context.getServiceImpl(IFloodlightProviderService.class);
	VirtualServer=new Server(MacAddress.of("00:00:00:00:00:06"),IPv4Address.of("10.0.0.6"),-1);
	
	}

	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodLightProvider.addOFMessageListener(OFType.PACKET_IN, this);
	}

	public void addFlows(
			IOFSwitch sw, Server source,TransportPort srcPrt) 
	{
		//Flow entry from client to server
	OFFactory myFactory = sw.getOFFactory();
	Match myMatch = myFactory.buildMatch()
		    .setExact(MatchField.ETH_TYPE, EthType.IPv4)
		    .setExact(MatchField.IP_PROTO,IpProtocol.TCP)
		    .setExact(MatchField.TCP_DST,TransportPort.of(8000))
		     .setExact(MatchField.IPV4_DST,VirtualServer.getIp())
		    .setExact(MatchField.TCP_SRC, srcPrt)
		     .setExact(MatchField.IPV4_SRC, source.getIp())
		    .build();
	ArrayList<OFAction> actionList = new ArrayList<OFAction>();
	OFActions actions =myFactory.actions();
	System.out.println(ServerList.peek().getMac()+" "+ServerList.peek().getIp()+" "+ServerList.peek().getPort());
	OFActionSetDlDst setDlDst=actions.buildSetDlDst().setDlAddr(ServerList.peek().getMac()).build();
	actionList.add(setDlDst);
	OFActionSetNwDst setNwDst=actions.buildSetNwDst().setNwAddr(ServerList.peek().getIp()).build();
	actionList.add(setNwDst);
	OFActionOutput output = actions.buildOutput()
		    .setMaxLen(0xFFffFFff)
		    .setPort(OFPort.of(ServerList.peek().getPort())).build();
	actionList.add(output);
	OFFlowAdd flowAdd = (OFFlowAdd) myFactory.buildFlowAdd()
			 .setBufferId(OFBufferId.NO_BUFFER)
			    .setHardTimeout(0)
			    .setIdleTimeout(0)
			    .setPriority(32768)
			    .setMatch(myMatch)
			    .setActions(actionList)
			    .setOutPort(OFPort.of(ServerList.peek().getPort()))
			.build();
		sw.write(flowAdd);
		//Flow entry from server to client
		myMatch = myFactory.buildMatch()
			    .setExact(MatchField.ETH_TYPE, EthType.IPv4)
			    .setExact(MatchField.IP_PROTO,IpProtocol.TCP)
			    .setExact(MatchField.TCP_DST,srcPrt)
			     .setExact(MatchField.IPV4_DST,source.getIp())
			    .setExact(MatchField.TCP_SRC,TransportPort.of(8000) )
			     .setExact(MatchField.IPV4_SRC, ServerList.peek().getIp())
			    .build();
		actionList = new ArrayList<OFAction>();
		OFActionSetDlSrc setDlSrc=actions.buildSetDlSrc().setDlAddr(VirtualServer.getMac()).build();
		actionList.add(setDlSrc);
		OFActionSetNwSrc setNwSrc=actions.buildSetNwSrc().setNwAddr(VirtualServer.getIp()).build();
		actionList.add(setNwSrc);
		output = actions.buildOutput()
			    .setMaxLen(0xFFffFFff)
			    .setPort(OFPort.of(source.getPort())).build();
		actionList.add(output);
		flowAdd = (OFFlowAdd) myFactory.buildFlowAdd()
				 .setBufferId(OFBufferId.NO_BUFFER)
				    .setHardTimeout(0)
				    .setIdleTimeout(0)
				    .setPriority(32768)
				    .setMatch(myMatch)
				    .setActions(actionList)
				    .setOutPort(OFPort.of(source.getPort()))
				.build();
			sw.write(flowAdd);
	}
	@Override
	public Command receive(
			IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		
		switch (msg.getType()) {
		case PACKET_IN:
			Ethernet eth=IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
			
			if (eth.getEtherType() == EthType.IPv4) {	
				//checking for ipv4 in network layer
				//Forming source client ip
				IPv4 ipv4=(IPv4)eth.getPayload();
				OFPacketIn myPacketIn = (OFPacketIn) msg;
				OFPort myInPort = (myPacketIn.getVersion().compareTo(OFVersion.OF_12) < 0) 
		                ? myPacketIn.getInPort() : myPacketIn.getMatch().get(MatchField.IN_PORT);
				Server source=new Server(eth.getSourceMACAddress(), ipv4.getSourceAddress(), myInPort.getPortNumber());
				TransportPort srcPrt;
				if(ipv4.getProtocol().equals(IpProtocol.TCP)) srcPrt=((TCP)ipv4.getPayload()).getSourcePort();
				else return Command.CONTINUE;
				//if not destination IP or if not destination port or if not tcp protocol
				
				if(!ipv4.getDestinationAddress().equals(VirtualServer.getIp()) || !(((TCP)ipv4.getPayload()).getDestinationPort().equals(TransportPort.of(8000))))return Command.CONTINUE;
				
				//FLOW ENTRY FOR client to server and server to client
				addFlows(sw, source, srcPrt);
				   //PACKET OUT
					eth.setDestinationMACAddress(ServerList.peek().getMac());
				   ipv4.setDestinationAddress(ServerList.peek().getIp());
				eth.setPayload(ipv4);
				OFPacketOut po = sw.getOFFactory().buildPacketOut()
					    .setData(eth.serialize())
					    .setActions(Collections.singletonList((OFAction)sw.getOFFactory().actions().output(OFPort.of(ServerList.peek().getPort()), 0xffFFffFF)))
					    .setInPort(OFPort.CONTROLLER)
					    .build();
					  ServerList.add(ServerList.remove());
					sw.write(po);
			}
			else if(eth.getEtherType() == EthType.ARP)
			{
				return Command.CONTINUE;
			}
			else return Command.CONTINUE;
			break;
		default:
			break;
		}
		return Command.CONTINUE;
	}
	private class Server
	{
		private MacAddress mac;
		private IPv4Address ip;
		private int port;
		
		public int getPort() {
			return port;
		}
		public Server(MacAddress mac, IPv4Address ip,int i) {
			super();
			this.mac = mac;
			this.ip = ip;
			this.port=i;
		}
		public MacAddress getMac() {
			return mac;
		}
		public IPv4Address getIp() {
			return ip;
		}
	}
}

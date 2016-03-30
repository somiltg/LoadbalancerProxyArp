package net.floodlightcontroller.serverloadbalance;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Vector;

import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFlowAdd;
import org.projectfloodlight.openflow.protocol.OFFlowModCommand;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
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
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.IPv6Address;
import org.projectfloodlight.openflow.types.IpProtocol;
import org.projectfloodlight.openflow.types.MacAddress;
import org.projectfloodlight.openflow.types.OFBufferId;
import org.projectfloodlight.openflow.types.OFPort;
import org.projectfloodlight.openflow.types.OFVlanVidMatch;
import org.projectfloodlight.openflow.types.U64;
import org.projectfloodlight.openflow.types.VlanVid;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.util.AppCookie;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.devicemanager.SwitchPort;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.packet.IPv6;
import net.floodlightcontroller.packet.TCP;
import net.floodlightcontroller.packet.UDP;
import net.floodlightcontroller.routing.ForwardingBase;
import net.floodlightcontroller.routing.IRoutingDecision;
import net.floodlightcontroller.routing.Route;
import net.floodlightcontroller.routing.IRoutingDecision.RoutingAction;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.routing.RoutingDecision;
import net.floodlightcontroller.topology.ITopologyService;
import net.floodlightcontroller.topology.NodePortTuple;
import net.floodlightcontroller.util.FlowModUtils;
import net.floodlightcontroller.util.MatchUtils;
public class AdvancedLoadBalancer  extends ForwardingBase implements
		IFloodlightModule,IOFMessageListener{
	 protected static Queue<Long> ServerDevicesList;
	protected static HashMap<Long,IPv4Address> Device2IP;
	protected static Vector<IPv4Address> Servers;
	protected static IPv4Address VirtualServerIP;
	protected static MacAddress VirtualServerMAC;
	
	//SET THE SERVER IDS AND VIP HERE
	public void setServerandVIP()
	{
		Servers.add(IPv4Address.of("10.0.0.2"));
		Servers.add(IPv4Address.of("10.0.0.3"));
		Servers.add(IPv4Address.of("10.0.0.4"));
		VirtualServerIP=IPv4Address.of("10.0.2.1");
		VirtualServerMAC=MacAddress.of("00:00:00:00:00:FF");
	}

	/* Added function to make a generic match using the flags given here
	 * From Forwarding module
	 */
		protected Match createMatchFromPacket(IOFSwitch sw, OFPort inPort, FloodlightContext cntx) {
			// The packet in match will only contain the port number.
			// We need to add in specifics for the hosts we're routing between.
			Ethernet eth = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
			VlanVid vlan = VlanVid.ofVlan(eth.getVlanID());
			MacAddress srcMac = eth.getSourceMACAddress();
			MacAddress dstMac = eth.getDestinationMACAddress();

			Match.Builder mb = sw.getOFFactory().buildMatch();
			mb.setExact(MatchField.IN_PORT, inPort);

			if (FLOWMOD_DEFAULT_MATCH_MAC) {
				mb.setExact(MatchField.ETH_SRC, srcMac)
				.setExact(MatchField.ETH_DST, dstMac);
			}

			if (FLOWMOD_DEFAULT_MATCH_VLAN) {
				if (!vlan.equals(VlanVid.ZERO)) {
					mb.setExact(MatchField.VLAN_VID, OFVlanVidMatch.ofVlanVid(vlan));
				}
			}

			// TODO Detect switch type and match to create hardware-implemented flow
			if (eth.getEtherType() == EthType.IPv4) { /* shallow check for equality is okay for EthType */
				IPv4 ip = (IPv4) eth.getPayload();
				IPv4Address srcIp = ip.getSourceAddress();
				IPv4Address dstIp = ip.getDestinationAddress();
				
				if (FLOWMOD_DEFAULT_MATCH_IP_ADDR) {
					mb.setExact(MatchField.ETH_TYPE, EthType.IPv4)
					.setExact(MatchField.IPV4_SRC, srcIp)
					.setExact(MatchField.IPV4_DST, dstIp);
				}

				if (FLOWMOD_DEFAULT_MATCH_TRANSPORT) {
					/*
					 * Take care of the ethertype if not included earlier,
					 * since it's a prerequisite for transport ports.
					 */
					if (!FLOWMOD_DEFAULT_MATCH_IP_ADDR) {
						mb.setExact(MatchField.ETH_TYPE, EthType.IPv4);
					}
					
					if (ip.getProtocol().equals(IpProtocol.TCP)) {
						TCP tcp = (TCP) ip.getPayload();
						mb.setExact(MatchField.IP_PROTO, IpProtocol.TCP)
						.setExact(MatchField.TCP_SRC, tcp.getSourcePort())
						.setExact(MatchField.TCP_DST, tcp.getDestinationPort());
					} else if (ip.getProtocol().equals(IpProtocol.UDP)) {
						UDP udp = (UDP) ip.getPayload();
						mb.setExact(MatchField.IP_PROTO, IpProtocol.UDP)
						.setExact(MatchField.UDP_SRC, udp.getSourcePort())
						.setExact(MatchField.UDP_DST, udp.getDestinationPort());
					}
				}
			} else if (eth.getEtherType() == EthType.ARP) { /* shallow check for equality is okay for EthType */
				mb.setExact(MatchField.ETH_TYPE, EthType.ARP);
			} else if (eth.getEtherType() == EthType.IPv6) {
				IPv6 ip = (IPv6) eth.getPayload();
				IPv6Address srcIp = ip.getSourceAddress();
				IPv6Address dstIp = ip.getDestinationAddress();
				
				if (FLOWMOD_DEFAULT_MATCH_IP_ADDR) {
					mb.setExact(MatchField.ETH_TYPE, EthType.IPv6)
					.setExact(MatchField.IPV6_SRC, srcIp)
					.setExact(MatchField.IPV6_DST, dstIp);
				}

				if (FLOWMOD_DEFAULT_MATCH_TRANSPORT) {
					/*
					 * Take care of the ethertype if not included earlier,
					 * since it's a prerequisite for transport ports.
					 */
					if (!FLOWMOD_DEFAULT_MATCH_IP_ADDR) {
						mb.setExact(MatchField.ETH_TYPE, EthType.IPv6);
					}
					
					if (ip.getNextHeader().equals(IpProtocol.TCP)) {
						TCP tcp = (TCP) ip.getPayload();
						mb.setExact(MatchField.IP_PROTO, IpProtocol.TCP)
						.setExact(MatchField.TCP_SRC, tcp.getSourcePort())
						.setExact(MatchField.TCP_DST, tcp.getDestinationPort());
					} else if (ip.getNextHeader().equals(IpProtocol.UDP)) {
						UDP udp = (UDP) ip.getPayload();
						mb.setExact(MatchField.IP_PROTO, IpProtocol.UDP)
						.setExact(MatchField.UDP_SRC, udp.getSourcePort())
						.setExact(MatchField.UDP_DST, udp.getDestinationPort());
					}
				}
			}
			return mb.build();
		}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		super.init();
		//service dependencies
		this.floodlightProviderService = context.getServiceImpl(IFloodlightProviderService.class);
		this.deviceManagerService = context.getServiceImpl(IDeviceService.class);
		this.routingEngineService = context.getServiceImpl(IRoutingService.class);
		this.topologyService = context.getServiceImpl(ITopologyService.class);
		this.switchService = context.getServiceImpl(IOFSwitchService.class);
		//Setting configurational fields
		Map<String, String> configParameters = context.getConfigParams(this);
			FLOWMOD_DEFAULT_HARD_TIMEOUT = FlowModUtils.INFINITE_TIMEOUT;
		String tmp = configParameters.get("idle-timeout");
		if (tmp != null) FLOWMOD_DEFAULT_IDLE_TIMEOUT = Integer.parseInt(tmp);
		else FLOWMOD_DEFAULT_IDLE_TIMEOUT = 50;	
			FLOWMOD_DEFAULT_PRIORITY = 35777; //priority change here does not matter since init of forwarding and advanced load are implemented one after the other
		tmp = configParameters.get("set-send-flow-rem-flag");
		if (tmp != null) 
			FLOWMOD_DEFAULT_SET_SEND_FLOW_REM_FLAG = Boolean.parseBoolean(tmp);
			tmp = configParameters.get("match");
		if (tmp != null) {
			tmp = tmp.toLowerCase();
			if (!tmp.contains("vlan") && !tmp.contains("mac") && !tmp.contains("ip") && !tmp.contains("port")) 
				return;
				FLOWMOD_DEFAULT_MATCH_VLAN = false;
				FLOWMOD_DEFAULT_MATCH_MAC = tmp.contains("mac") ? true : false;
				FLOWMOD_DEFAULT_MATCH_IP_ADDR = tmp.contains("ip") ? true : false;
				FLOWMOD_DEFAULT_MATCH_TRANSPORT = tmp.contains("port") ? true : false;
			}
		//Setting virtual server and Server ip's
		ServerDevicesList=new LinkedList<Long>();
		Servers=new Vector<IPv4Address>();
		VirtualServerIP=IPv4Address.of(0);
		VirtualServerMAC=MacAddress.of(0);
		Device2IP=new HashMap<Long,IPv4Address>();
		setServerandVIP();
		}			

	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProviderService.addOFMessageListener(OFType.PACKET_IN, this);
		deviceManagerService.addListener(new ServersRetriever());
	}
	
	//CREATES A ROUTE BASED ON THE SRC AND DESTINATION DEVICES with given switch and port: Based on Forwarding module
	protected Route createRoute(IDevice srcDevice, IDevice dstDevice,IOFSwitch sw, OFPort inPort )
	{
		/* Validate that the source and destination are not on the same switch port */
		DatapathId source = sw.getId();
		boolean on_same_if = false;
		for (SwitchPort dstDap : dstDevice.getAttachmentPoints()) {
			if (sw.getId().equals(dstDap.getSwitchDPID()) && inPort.equals(dstDap.getPort())) {
				on_same_if = true;
			}
			break;
		}
		if (on_same_if) {
			return null;
		}
		SwitchPort[] dstDaps = dstDevice.getAttachmentPoints();
		SwitchPort dstDap = null;

		/* 
		See the forwarding module doForward for info. Basically checks whether the port_switch is a part 
		of the overlay. So here we check whether we have any attachment that connects to the overlay.	
		 
		 */
		for (SwitchPort ap : dstDaps) {
			if (topologyService.isEdge(ap.getSwitchDPID(), ap.getPort())) {
				dstDap = ap;
				break;
			}
		}
		//if not a part of the overlay for the destination and source devices
		if (dstDap == null || !topologyService.isEdge(source, inPort)) return null;
		//Finding route
		Route route = routingEngineService.getRoute(source, 
				inPort,
				dstDap.getSwitchDPID(),
				dstDap.getPort(), U64.of(0)); //cookie = 0, i.e., default route
		
		if (route == null) {
			/* Route traverses no links --> src/dst devices on same switch */
			//Create your own route using path of Node port tuples and manually inserting it
			route = new Route(srcDevice.getAttachmentPoints()[0].getSwitchDPID(), dstDevice.getAttachmentPoints()[0].getSwitchDPID());
			List<NodePortTuple> path = new ArrayList<NodePortTuple>(2);
			path.add(new NodePortTuple(srcDevice.getAttachmentPoints()[0].getSwitchDPID(),
					srcDevice.getAttachmentPoints()[0].getPort()));
			path.add(new NodePortTuple(dstDevice.getAttachmentPoints()[0].getSwitchDPID(),
					dstDevice.getAttachmentPoints()[0].getPort()));
			route.setPath(path);
		}
		return route;
	}

	protected Command doForwardFlow(IOFSwitch sw, OFPacketIn pi, FloodlightContext cntx,boolean dir) {
		//Source device
		OFPort inPort = (pi.getVersion().compareTo(OFVersion.OF_12) < 0 ? pi.getInPort() : pi.getMatch().get(MatchField.IN_PORT));
		IDevice srcDevice = IDeviceService.fcStore.get(cntx, IDeviceService.CONTEXT_SRC_DEVICE);
		//Destination device
		IDevice dstDevice=null;
		//reverse flow case
		dstDevice=IDeviceService.fcStore.get(cntx, IDeviceService.CONTEXT_DST_DEVICE);
		if(dstDevice==null)
		{//forward flow case
		dstDevice = deviceManagerService.getDevice(ServerDevicesList.peek());
		}
		if (dstDevice == null|| srcDevice == null)  return Command.CONTINUE;
		//Finding route
		System.out.println("Src Device"+ srcDevice.getMACAddressString());
		System.out.println("Dst Device"+ dstDevice.getMACAddressString());
		Route route=createRoute(srcDevice, dstDevice, sw, inPort);
		U64 cookie = AppCookie.makeCookie(FORWARDING_APP_ID, 0);
		if(route==null) return Command.STOP;
		//changing pi and match as per direction
		FLOWMOD_DEFAULT_MATCH_VLAN=false;
		FLOWMOD_DEFAULT_MATCH_MAC=false;
		FLOWMOD_DEFAULT_MATCH_IP_ADDR=true;
		FLOWMOD_DEFAULT_MATCH_TRANSPORT=false;
		Match match,match1=createMatchFromPacket(sw, inPort, cntx);
		Match.Builder mb = MatchUtils.convertToVersion(match1, sw.getOFFactory().getVersion());
		if(dir)
			{//forward flow
			mb.setExact(MatchField.IPV4_DST,Device2IP.get(dstDevice.getDeviceKey()));
			match=mb.build();
			}
			else 
			{ //reverse flow
				mb.setExact(MatchField.IPV4_SRC,VirtualServerIP);
				match=mb.build();
			}
		System.out.println("match "+match);
		//finding the outport of the first switch in the route
		OFPort outPort= route.getPath().get(1).getPortId();
		//Inserting the intial route at all intermediate switches
		if(!pushRoute(route, match, pi, sw.getId(), cookie, 
					cntx, false,
					OFFlowModCommand.ADD))
			return Command.STOP;
		//adding flow entries to the first switch for change of the des/src in load balancing
		OFFactory myFactory = sw.getOFFactory();
		ArrayList<OFAction> actionList = new ArrayList<OFAction>();
		OFActions actions =myFactory.actions();
		if(dir)
		{//forward flow
			OFActionSetDlDst setDlDst=actions.buildSetDlDst().setDlAddr(dstDevice.getMACAddress()).build();
			actionList.add(setDlDst);
			OFActionSetNwDst setNwDst=actions.buildSetNwDst().setNwAddr(Device2IP.get(dstDevice.getDeviceKey())).build();
			actionList.add(setNwDst);
		}
		else 
		{ //reverse flow
			OFActionSetDlSrc setDlSrc=actions.buildSetDlSrc().setDlAddr(VirtualServerMAC).build();
			actionList.add(setDlSrc);
			OFActionSetNwSrc setNwSrc=actions.buildSetNwSrc().setNwAddr(VirtualServerIP).build();
			actionList.add(setNwSrc);
		}
		//Flow entry add
		OFActionOutput output = actions.buildOutput()
			    .setMaxLen(0xFFffFFff)
			    .setPort(outPort).build();
		actionList.add(output);
		OFFlowAdd flowAdd = (OFFlowAdd) myFactory.buildFlowAdd()
				 .setBufferId(OFBufferId.NO_BUFFER)
				    .setHardTimeout(FLOWMOD_DEFAULT_HARD_TIMEOUT)
				    .setIdleTimeout(FLOWMOD_DEFAULT_IDLE_TIMEOUT)
				    .setPriority(FLOWMOD_DEFAULT_PRIORITY+2) // to make the entry higher than the forwarding module entry
				    .setMatch(match1)
				    .setActions(actionList)
				    .setOutPort(outPort)
				.build();
			sw.write(flowAdd);
			if(dir)
			{//queue change
			ServerDevicesList.add(ServerDevicesList.remove());
			}
			return Command.STOP;// no need of the forwarding module as it may overwrite the flowtable
	}
	

	@Override
	public net.floodlightcontroller.core.IListener.Command processPacketInMessage(
			IOFSwitch sw, OFPacketIn pi, IRoutingDecision decision,
			FloodlightContext cntx) {
	    // serverDevicesLocator();  
		if(ServerDevicesList.isEmpty()){return Command.CONTINUE;}
		Ethernet eth = IFloodlightProviderService.bcStore.get(cntx, IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
		//if packet is multicast or broadcast and not arp
		if (eth.isBroadcast() && eth.getEtherType()!=EthType.ARP || eth.isMulticast()) 
			return Command.CONTINUE;
		if (eth.getEtherType() == EthType.IPv4) {	
			//checking for ipv4 in network layer
			IPv4 ipv4=(IPv4)eth.getPayload();
			Command m;
			//deciding flow
			System.out.println("process packet in message");
			if(ipv4.getDestinationAddress().equals(VirtualServerIP))
			 m=doForwardFlow(sw, pi, cntx, true);
			else if((ipv4.getProtocol().equals(IpProtocol.TCP) || ipv4.getProtocol().equals(IpProtocol.UDP)) && Servers.contains(ipv4.getSourceAddress())) m=doForwardFlow(sw, pi, cntx, false);
			else return Command.CONTINUE;
		
				return m;
		}
		
	 return Command.CONTINUE;
	
	}
	
	@Override
	public net.floodlightcontroller.core.IListener.Command receive(
			IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
		switch (msg.getType()) {
		case PACKET_IN:
			IRoutingDecision decision = null;
			if (cntx != null) {
				decision = RoutingDecision.rtStore.get(cntx, IRoutingDecision.CONTEXT_DECISION);
				if(decision!=null && !(decision.getRoutingAction()==RoutingAction.FORWARD || decision.getRoutingAction()==RoutingAction.FORWARD_OR_FLOOD || decision.getRoutingAction()==RoutingAction.NONE)) 
					return Command.CONTINUE;
			}
			System.out.println("reached packet in");
			return this.processPacketInMessage(sw, (OFPacketIn) msg, decision, cntx);
		default:
			break;
		}
		return Command.CONTINUE;		
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
		l.add(IRoutingService.class);
		l.add(ITopologyService.class);
		l.add(IOFSwitchService.class);
		return l;
	}	
		
	@Override
	public String getName() {
		return "AdvancedLoadBalancer";
	}

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
		return (type.equals(OFType.PACKET_IN) && (name.equals("topology") || name.equals("devicemanager")|| name.equals("arp")));
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		return (type.equals(OFType.PACKET_IN) && (name.equals("forwarding")));
	}

}

# LoadbalancerProxyArp
SDN Floodlight controller modules for balancing load and address resolution within a subnet


Instructions for using it

1. Download floodlight module 1.0.
2. Add the two modules i.e. ServerLoadBalance and ProxyArp as net.floodlightcontroller.serverloadbalance and net.floodlightcontroller.proxyarp inside src/main/java
3. Register the modules by adding the following entries in the file with path as  src/main/resources/META-INF/services/net.floodlightcontroller.core.module.IFloodlightModule-
net.floodlightcontroller.proxyarp.ProxyArp
net.floodlightcontroller.serverloadbalance.AdvancedLoadBalancer
4. In /src/main/resources/floodlightdefault.properties, add net.floodlightcontroller.proxyarp.ProxyArp and 
net.floodlightcontroller.serverloadbalance.AdvancedLoadBalancer to floodlight.modules variable
Module is ready to run

5. In mininet OS, execute topo1.py to create sample topology. However one can also use mn to create topology.
6. NOTE: to change vip or set virtual proxy address and virtual macs find setServerandVIP() in AdvancedLoadbalancer and setVirtualDevices() in proxyarp module.

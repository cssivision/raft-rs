1, when to send msgs.
- remove node: The quorum size is now smaller, so see if any pending entries can be committed. if commit, broadcast updated committed index to peers.
- receive propose: broadcast new append entries to all peers.
- receive append response message: repsonse may cause commit new entries, if commit, broadcast updated committed index to peers.
- receive heartbeat response: if the sender's matched index less than leader's last index, send append entries. and the read request maybe advanced. send `MsgReadIndexResp` message, if the read only request is forwarded from peers.
- become leader: send empty entry to commit last term entries. 
- leader transfer: send append to transferee, when transferee's matched index less than leader's last index.
- normal step: step leader, step candidate, step follower.

2, benefit of pre vote.
In simplest form, Raft leader steps down to follower when it receives a message with higher term. For instance, a flaky(or rejoining) member drops in and out, and starts campaign. This member will end up with a higher term, and ignore all incoming messages with lower term. In this case, a new leader eventually need to get elected, thus disruptive to cluster availability. Same happens with isolated member or slow incoming network. isolated member with higher term would send pb.MsgAppResp, in response to pb.MsgHeartbeat from leader, and pb.MsgAppResp to leader forces leader to step down, and cluster becomes unavailable.

Raft implements Pre-Vote phase to prevent this kind of disruptions. If enabled, Raft runs an additional phase of election to check if pre-candidate can get enough votes to win an election. If not, it would remain as follower, and most likely, receive leader heartbeats to reset its elapsed election ticks.

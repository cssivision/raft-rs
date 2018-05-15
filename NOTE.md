When to send msgs.
- remove node: The quorum size is now smaller, so see if any pending entries can be committed. if commit, broadcast updated committed index to peers.
- receive propose: broadcast new append entries to all peers.
- receive append response message: repsonse may cause commit new entries, if commit, broadcast updated committed index to peers.
- receive heartbeat response: if the sender's matched index less than leader's last index, send append entries. and the read request maybe advanced. send `MsgReadIndexResp` message, if the read only request is forwarded from peers.
- become leader: send empty entry to commit last term entries. 
- leader transfer: send append to transferee, when transferee's matched index less than leader's last index.
- normal step: step leader, step candidate, step follower.
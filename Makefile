generate: 
	protoc --rust_out ./src/ ./raftpb/raftpb.proto
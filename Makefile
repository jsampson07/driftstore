CC      = g++ -std=c++17
RM      = rm -rf

# -g added: you rely on gdb/lldb as a fallback debugger, which needs symbols to be useful.
CFLAGS  := -Wall -g -I src `pkg-config --cflags protobuf grpc`
LFLAGS  := `pkg-config --libs protobuf grpc++` -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed -ldl

PROTOC = protoc
GRPC_PLUGIN := $(shell which grpc_cpp_plugin)
PROTO_SRC = src/driftstore.proto
PROTO_GEN = src/driftstore.pb.cc src/driftstore.grpc.pb.cc

BIN_DIR = bin

# Fix: targets now declared .PHONY so they don't silently no-op if a same-named
# file ever shows up in the repo root, and don't rely on "target never matches
# a real file" as an accident of naming.
.PHONY: all clean

all: $(BIN_DIR)/node $(BIN_DIR)/client

$(PROTO_GEN): $(PROTO_SRC)
	$(PROTOC) --grpc_out=src --plugin=protoc-gen-grpc=$(GRPC_PLUGIN) -I src $(PROTO_SRC)
	$(PROTOC) --cpp_out=src -I src $(PROTO_SRC)

# Single "node" binary, not manager+storage: Driftstore has no centralized
# manager role, so every process is the same symmetric peer.
$(BIN_DIR)/node: src/node.cpp $(PROTO_GEN)
	mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) src/node.cpp $(PROTO_GEN) $(LFLAGS) -o $(BIN_DIR)/node

$(BIN_DIR)/client: src/client.cpp $(PROTO_GEN)
	mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) src/client.cpp $(PROTO_GEN) $(LFLAGS) -o $(BIN_DIR)/client

# Not a dependency of "all" — test binaries are dev tooling, not something every
# ordinary build should pay to compile. Build explicitly with "make bin/test_ring".
$(BIN_DIR)/test_ring: src/test_ring.cpp src/ring.hpp $(PROTO_GEN)
	mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) src/test_ring.cpp src/driftstore.pb.cc $(LFLAGS) -o $(BIN_DIR)/test_ring

$(BIN_DIR)/vnode_experiment: src/vnode_experiment.cpp src/ring.hpp $(PROTO_GEN)
	mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) src/vnode_experiment.cpp src/driftstore.pb.cc $(LFLAGS) -o $(BIN_DIR)/vnode_experiment

$(BIN_DIR)/test_vector_clocks: src/test_vector_clocks.cpp src/vector_clock.hpp src/logging.hpp $(PROTO_GEN)
	mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) src/test_vector_clocks.cpp src/driftstore.pb.cc $(LFLAGS) -o $(BIN_DIR)/test_vector_clocks

# driftclient.cpp makes real RPC calls (Stub::Put/Ping, NewStub), unlike
# test_ring/vnode_experiment above — needs the grpc-generated .cc linked in
# too, not just the plain protobuf one, or it fails at link time.
$(BIN_DIR)/driftclient: src/driftclient.cpp src/driftclient.hpp $(PROTO_GEN)
	mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) src/driftclient.cpp $(PROTO_GEN) $(LFLAGS) -o $(BIN_DIR)/driftclient

clean:
	$(RM) src/*.pb.cc src/*.pb.h $(BIN_DIR)/*
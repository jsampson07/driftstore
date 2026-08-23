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

clean:
	$(RM) src/*.pb.cc src/*.pb.h $(BIN_DIR)/*

BINARY   := ma-irc
CARGO    := cargo
DEBUG    := target/debug/$(BINARY)
RELEASE  := target/release/$(BINARY)
SRCS     := Cargo.toml Cargo.lock $(shell find src -name '*.rs')
PREFIX   ?= /usr/local/bin
RUN_ARGS ?=

.PHONY: all build clean distclean fmt install lint release run test

all: build

build: $(DEBUG)

release: $(RELEASE)

$(DEBUG): $(SRCS)
	$(CARGO) build

$(RELEASE): $(SRCS)
	$(CARGO) build --release

run: $(DEBUG)
	$(DEBUG) $(RUN_ARGS)

fmt:
	$(CARGO) fmt

lint:
	$(CARGO) clippy -- -D warnings
	$(CARGO) fmt --check

test: lint
	$(CARGO) test

clean:
	$(CARGO) clean

install: $(RELEASE)
	sudo mkdir -p $(PREFIX)
	sudo install -m 0755 $(RELEASE) $(PREFIX)/$(BINARY)

distclean: clean
	rm -rf target

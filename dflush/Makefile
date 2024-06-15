DEBUG?=0
BUILD_DIR:=build
KERNELS_DIR:=kernels
KERNEL_INCLUDES:=$(wildcard $(KERNELS_DIR)/*/*.h)
HOST_APP_SRCS:=$(wildcard *.cpp)
HOST_INCLUDES:=$(wildcard *.h)

CC:=gcc
CXX:=g++

CFLAGS=--std=c++20
ifeq ($(DEBUG),1)
	CFLAGS+=-g -O0 -DDEBUG
else
	CFLAGS+=-O3 -DNDEBUG
endif
CFLAGS+=-Wall -Wextra -Wpedantic -Wno-deprecated-declarations -Wno-variadic-macros -Wno-gnu-zero-variadic-macro-arguments
CFLAGS+=$(shell pkg-config --cflags doca-common doca-dpa doca-rdma doca-erasure-coding libflexio libibverbs libmlx5)
LDFLAGS=$(shell pkg-config --libs doca-common doca-dpa doca-rdma doca-erasure-coding libflexio libibverbs libmlx5) -static-libgcc -static-libstdc++


DEVCC_HOST_CFLAGS:=-Wno-deprecated-declarations
DEVCC_DEV_CFLAGS:=-D__DPACC,-D__linux__,-DE_MODE_LE,-ffreestanding,-mabi=lp64,-mno-relax,-mcmodel=medany,-nostdlib,-Wdouble-promotion,-Wall,-Wextra,-Wpedantic,-Wno-deprecated-declarations,-Wno-extra-semi
ifeq ($(DEBUG),1)
	DEVCC_DEV_CFLAGS+=,-g,-O0,-DDEBUG
else
	DEVCC_DEV_CFLAGS+=,-O3,-DNDEBUG
endif
DEVCC_DEV_LD_DIR:=$(shell pkg-config --libs-only-L doca-common doca-dpa doca-rdma doca-erasure-coding)
DEVCC_DEV_INC_DIR:=$(shell pkg-config --cflags-only-I doca-common doca-dpa doca-rdma doca-erasure-coding) -Ikernels
DEVCC_CFLAGS:=--hostcc=$(CC) --hostcc-options="$(DEVCC_HOST_CFLAGS)" --devicecc-options="$(DEVCC_DEV_CFLAGS)" $(DEVCC_DEV_INC_DIR) -flto
DEVCC_LDFLAGS:=--device-libs="$(DEVCC_DEV_LD_DIR)" -ldpa

HOST_APP_NAMES:= $(patsubst %.cpp,%.exe,$(HOST_APP_SRCS))
HOST_APP_OBJS:= $(addprefix $(BUILD_DIR)/,$(patsubst %.cpp,%.o,$(HOST_APP_SRCS)))

KERNEL_DIRS:=$(shell ls -d $(KERNELS_DIR)/*/ 2>/dev/null | sed -e 's/\/$$//g')
KERNEL_BUILD_DIRS:=$(addprefix $(BUILD_DIR)/,$(KERNEL_DIRS))
KERNEL_NAMES:= $(notdir $(KERNEL_DIRS))
KERNEL_LIBS:=$(addprefix $(BUILD_DIR)/,$(addsuffix .a,$(KERNEL_DIRS)))

BUILD_DIRS:=$(BUILD_DIR) $(KERNEL_BUILD_DIRS)
ASMS:= $(addsuffix .asm,$(KERNEL_NAMES))

all: create_dirs $(HOST_APP_NAMES) $(ASMS)

%.asm: $(BUILD_DIR)/$(KERNELS_DIR)/%.elf
	dpa-objdump -sSdxl --mcpu=nv-dpa-bf3 $< > $@

%.elf: %.a
	dpacc-extract $< -o $@

%.exe: $(BUILD_DIR)/%.o  $(KERNEL_LIBS)
	$(CXX) -o $@ $^ $(CFLAGS) $(LDFLAGS)

.PRECIOUS: $(HOST_APP_OBJS) $(HOST_APP_NAMES)

define KERNEL_RULE
KERNEL_DIR:=$(KERNELS_DIR)/$1
KERNEL_LIB:=$(BUILD_DIR)/$$(KERNEL_DIR).raw.a
KERNEL_LIB_PATCHED:=$(BUILD_DIR)/$$(KERNEL_DIR).a
KERNEL_SRCS:=$$(wildcard $$(KERNEL_DIR)/*.c)
KERNEL_OBJS:=$$(addprefix $(BUILD_DIR)/,$$(patsubst %.c,%.dpa.o,$$(KERNEL_SRCS)))

$$(KERNEL_LIB_PATCHED): $$(KERNEL_LIB)
	python3 patch.py $1

$$(KERNEL_LIB): $$(KERNEL_OBJS)
	dpacc -o $$@ $$^ --app-name=$1 $(DEVCC_CFLAGS) $(DEVCC_LDFLAGS)
endef

$(foreach e,$(KERNEL_NAMES),$(eval $(call KERNEL_RULE,$(e))))

$(BUILD_DIR)/%.o: %.c* $(HOST_INCLUDES)
	$(CXX) -c -o $@ $< $(CFLAGS)

$(BUILD_DIR)/%.dpa.o: %.c $(KERNEL_INCLUDES)
	dpacc -c -o $@ $< $(DEVCC_CFLAGS)

create_dirs: $(BUILD_DIRS)
$(BUILD_DIRS):
	mkdir -p $@

veryclean: clean
	rm -rf $(KERNELS_DIR)/*/*.gen.h 

clean:
	rm -rf $(BUILD_DIR) *.exe *.asm

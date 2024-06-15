import os
import sys

patches_dir = "patches"
kernels_dir = "kernels"
build_dir = "build"

if len(sys.argv) < 2:
    print("Usage: python patch.py <app_name>")
    sys.exit(1)

app_name = sys.argv[1]
raw_dpa_lib = os.path.join(build_dir, kernels_dir, f"{app_name}.raw.a")
tmp_dpa_lib = os.path.join(build_dir, kernels_dir, f"{app_name}.tmp.a")
patched_dpa_lib = os.path.join(build_dir, kernels_dir, f"{app_name}.a")
elf_file = os.path.join(build_dir, kernels_dir, f"{app_name}.elf")
if not os.path.exists(raw_dpa_lib):
    print(f"Error: {raw_dpa_lib} does not exist. make sure to build the app first.")
    sys.exit(1)

res = os.system(f"dpacc-extract {raw_dpa_lib} -o {elf_file}")
if res != 0:
    print(f"Error: dpacc-extract failed with code {res}.")
    sys.exit(1)

os.system(f"cp {raw_dpa_lib} {tmp_dpa_lib}")

# dpa-objdump build/kernels/stub_app.elf --headers | grep .text
_, section_name, section_size, section_start, section_type = (
    os.popen(f"dpa-objdump {elf_file} --headers | grep .text").read().split()
)
assert section_name == ".text"
assert section_type == "TEXT"
section_size = int(section_size, 16)
section_start = int(section_start, 16)

with open(elf_file, "rb") as f:
    elf_data = f.read()
with open(raw_dpa_lib, "rb") as f:
    dpa_lib_data = f.read()
dpa_elf_offset = dpa_lib_data.find(elf_data)
text_section_offset = dpa_elf_offset + 4096


def find_sym(sym):
    res = os.popen(f"dpa-objdump {elf_file} --syms | grep -E '\\b{sym}\\b'").readlines()
    if len(res) != 1:
        print(f"Error: symbol {sym} not found.")
        return 0, 0
    addr, *_, sec_name, size, name = res[0].split()
    assert sec_name == ".text"
    assert name == sym
    offset = int(addr, 16) - section_start + text_section_offset
    size = int(size, 16)
    return offset, size


if not os.path.exists(os.path.join(patches_dir, app_name)):
    os.system(f"mv {tmp_dpa_lib} {patched_dpa_lib}")
    sys.exit(0)

patches = [
    fn
    for fn in os.listdir(os.path.join(patches_dir, app_name))
    if fn.endswith(".patch")
]


def apply_patch(patch):
    sym = patch.replace(".patch", "")
    patch_file = os.path.join(patches_dir, app_name, patch)
    offset, size = find_sym(sym)
    if size == 0:
        return
    with open(patch_file, "rb") as f:
        data = f.read()
    assert len(data) <= size
    with open(tmp_dpa_lib, "r+b") as f:
        f.seek(offset)
        f.write(data)
        f.write(b"\x00" * (size - len(data)))


for patch in patches:
    apply_patch(patch)

print("Patching done.")
os.system(f"mv {tmp_dpa_lib} {patched_dpa_lib}")
#### 需要安装的东西
安装 grpc 到一个位置，并且修改 CMakeLists 到对应位置。
执行之前 readme 里面的 nas 安装脚本。
#### Arm 端需要额外修改的东西
CMakeLists 中 PKG_CONFIG_PATH 的 x 86 路径需要改成这个路径 `aarch64-linux-gnu`。
#### 运行步骤
Arm 端
```
git clone ...
git checkout --track ...
cd dflush
make -j
cd ..
./cmake_configure.sh
./cmake_build.sh dpu_server
./build/dpu_server
```
Host 端运行无变化
#### 具体代码问题再讨论


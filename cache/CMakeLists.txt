add_library(rdmalib cache_rdma.cc)
find_package(ibverbs REQUIRED)
target_link_libraries(rdmalib ibverbs::ibverbs)
find_package(rdmacm REQUIRED)
target_link_libraries(rdmalib rdmacm)
target_include_directories(rdmalib INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(server cache_rdma_server.cc)
target_link_libraries(server rdmalib)

add_executable(client cache_rdma_client.cc)
target_link_libraries(client rdmalib)
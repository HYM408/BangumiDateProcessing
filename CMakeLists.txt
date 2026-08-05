cmake_minimum_required(VERSION 3.16)

project(BangumiDateProcessing)

set(CMAKE_CXX_STANDARD 17)

find_package(Qt6 REQUIRED COMPONENTS
        Core
        Network
)

add_executable(BangumiDateProcessing main.cpp)

target_link_libraries(BangumiDateProcessing
        PRIVATE
        Qt6::Core
        Qt6::Network
)
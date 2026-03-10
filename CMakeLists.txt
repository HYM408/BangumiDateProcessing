cmake_minimum_required(VERSION 3.16)

project(BangumiDateProcessing VERSION 0.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)

find_package(Qt6 REQUIRED COMPONENTS
        Core
        Sql
        Network
        Concurrent
)

add_executable(BangumiDateProcessing
        main.cpp
)

target_link_libraries(BangumiDateProcessing
        PRIVATE
        Qt6::Core
        Qt6::Sql
        Qt6::Network
        Qt6::Concurrent
)
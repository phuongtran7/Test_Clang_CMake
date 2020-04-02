cmake_minimum_required (VERSION 3.14)

add_executable (Test_Clang_CMake "Test_Clang_CMake.cpp" "Test_Clang_CMake.h"
		"MQTT_Client.h" "MQTT_Client.cpp"
		"synchronized_value.h")

find_package(fmt CONFIG REQUIRED)
find_package(PahoMqttCpp CONFIG REQUIRED)
target_link_libraries(Test_Clang_CMake PRIVATE PahoMqttCpp::paho-mqttpp3 fmt::fmt fmt::fmt-header-only)

set_target_properties(Test_Clang_CMake PROPERTIES CXX_STANDARD 17)
#include "Test_Clang_CMake.h"

std::shared_ptr<synchronized_value<std::string>> buffer_;
std::unique_ptr<MQTT_Client> subscriber_;

void init(std::string_view address, std::string_view topic) {
	buffer_ = std::make_shared<synchronized_value<std::string>>();
	subscriber_ = std::make_unique<MQTT_Client>(address, topic, 0, buffer_);
}

void read(std::string_view received_data) {
	auto data = flexbuffers::GetRoot(reinterpret_cast<const uint8_t*>(received_data.data()), received_data.size()).AsMap();

	fmt::print("Altitude: {}\n", data["altitude_pilot"].AsFloat());
}

int main()
{
	//MQTT_Client c1("tcp://127.0.0.1:1883", "Test_String_view", 0);
	//auto c2 = std::move(c1);

	//auto test_buf = std::make_shared<synchronized_value<std::string>>();
	//MQTT_Client r1("tcp://127.0.0.1:1883", "Test_String_view_Sub", 0, test_buf);
	//auto r2(std::move(r1));

	init("tcp://127.0.0.1:1883", "XP-S76-Debug");

	while (1) {
		auto received_data = apply([](std::string& s) { return std::move(s); }, *buffer_);

		if (!received_data.empty()) {
			read(received_data);
		}
		std::this_thread::sleep_for(std::chrono::seconds(1));
	}
}

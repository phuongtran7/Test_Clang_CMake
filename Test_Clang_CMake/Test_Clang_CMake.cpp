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
	init("tcp://127.0.0.1:1883", "XP-S76-Debug");

	while (true) {
		auto received_data = apply([](std::string& s) { return std::move(s); }, *buffer_);

		if (!received_data.empty()) {
			read(received_data);
		}
		std::this_thread::sleep_for(std::chrono::seconds(1));
	}
}

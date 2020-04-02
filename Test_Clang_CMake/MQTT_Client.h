#pragma once
#include "mqtt/async_client.h"
#include "mqtt/callback.h"
#include "fmt/format.h"
#include "synchronized_value.h"

/*
 * This callback is used to display the result of subscribing event
 */
class subscribe_listener : public virtual mqtt::iaction_listener
{
private:
	void on_failure(const mqtt::token& tok) override;
	void on_success(const mqtt::token& tok) override;
};

/*
 * This callback is used to display the result of publishing event
 */
class publish_listener : public virtual mqtt::iaction_listener
{
private:
	void on_failure(const mqtt::token& tok) override;
	void on_success(const mqtt::token& tok) override;
};

/*
 * Local callback & listener class for use with the client connection.
 * This is primarily intended to receive messages, but it will also monitor
 * the connection to the broker. If the connection is lost, it will attempt
 * to restore the connection and re-subscribe to the topic.
 */
class action_callback : public virtual mqtt::callback,
	public virtual mqtt::iaction_listener
{
private:
	// The MQTT client
	mqtt::async_client& cli_;
	// Options to use if we need to reconnect
	mqtt::connect_options& connOpts_;
	// An action listener to display the result of actions. In this case, the subscribe action
	std::shared_ptr<subscribe_listener> subscribe_listener_;
	// Topic we are publishing/subscribing to
	std::string topic_;
	// Buffer to store message
	std::shared_ptr<synchronized_value<std::string>> buffer_;

private:
	// Try to reconnect and using sublistener to display the result of the action
	void reconnect();

	// Handle re-connect if the first connect event failed
	void on_failure(const mqtt::token& tok) override;

	// (Re)connection success
	// Either this or connected() can be used for callbacks.
	void on_success(const mqtt::token& tok) override;

	// Connection success
	void connected(const std::string& cause) override;

	// Callback for when the connection is lost.
	// This will initiate the attempt to manually reconnect.
	void connection_lost(const std::string& cause) override;

	// Callback for when a message arrives.
	void message_arrived(mqtt::const_message_ptr msg) override;

	void delivery_complete(mqtt::delivery_token_ptr tok) override;

public:
	action_callback(mqtt::async_client& cli, mqtt::connect_options& connOpts, std::string topic, std::shared_ptr<synchronized_value<std::string>> buffer);
	action_callback(mqtt::async_client& cli, mqtt::connect_options& connOpts, std::string topic);
};

/*
 * The class manages the underlying connection to the MQTT.
 * Move-only.
 * Pass a shared_ptr to synchronized_value object to to create a Subscriber
 * Otherwise default to Publisher
 */
class MQTT_Client
{
private:
	std::string address_;
	std::string topic_;
	int qos_;
	mqtt::async_client_ptr client_;
	mqtt::connect_options conn_options_;
	std::shared_ptr<synchronized_value<std::string>> buffer_;
	std::shared_ptr<action_callback> callback_; // Main callback for connection to the MQTT broker
	std::shared_ptr<publish_listener> publish_listener_; // An action listener to display the result of actions, in this case the publish action

private:
	void initialize();

public:
	// Publisher
	//MQTT_Client(std::string address, std::string topic, int qos);
	MQTT_Client(std::string_view address, std::string_view topic, int qos);

	// Subscriber
	//MQTT_Client(std::string address, std::string topic, int qos, std::shared_ptr<synchronized_value<std::string>> buffer);
	MQTT_Client(std::string_view address, std::string_view topic, int qos, std::shared_ptr<synchronized_value<std::string>> buffer);

	~MQTT_Client();

	// Copy constructor
	MQTT_Client(const MQTT_Client& other) = delete;
	// Copy assignment
	MQTT_Client& operator=(const MQTT_Client& other) = delete;

	// Move constructor
	MQTT_Client(MQTT_Client&& other) noexcept;
	// Move assignment
	MQTT_Client& operator=(MQTT_Client&& other) noexcept;

	void send_message(const std::string& message);
	void send_message(const std::vector<uint8_t>& pointer);
};
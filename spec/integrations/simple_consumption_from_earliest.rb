# frozen_string_literal: true

# Karafka should be able to consume all the data from beginning

ROOT_PATH = Pathname.new(File.expand_path(File.join(File.dirname(__FILE__), '../../')))
require ROOT_PATH.join('spec/integrations_helper.rb')

setup_karafka

elements = Array.new(100) { SecureRandom.uuid }

class Consumer < Karafka::BaseConsumer
  def consume
    messages.each do |message|
      DataCollector.data[message.metadata.partition] << message.raw_payload
    end
  end
end

Karafka::App.consumer_groups.draw do
  consumer_group DataCollector.topic do
    topic DataCollector.topic do
      consumer Consumer
    end
  end
end

Thread.new do
  sleep(0.1) while DataCollector.data[0].size < 100
  Karafka::App.stop!
end

elements.each { |data| produce(DataCollector.topic, data) }

Karafka::Server.run

assert_equal elements, DataCollector.data[0]
assert_equal 1, DataCollector.data.size

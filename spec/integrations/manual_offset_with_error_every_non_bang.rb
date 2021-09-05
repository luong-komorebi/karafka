# frozen_string_literal: true

ROOT_PATH = Pathname.new(File.expand_path(File.join(File.dirname(__FILE__), '../../')))
require ROOT_PATH.join('spec/integrations_helper.rb')

# When manual offset management is on, upon error Karafka should start again from the place
# it had in the checkpoint. If we checkpoint after each message is processed (here adding to array)
# it should not have any duplicates as the error happens befor checkpointing

setup_karafka do |config|
  config.manual_offset_management = true
end

numbers = Array.new(100) { rand.to_s }

class Consumer < Karafka::BaseConsumer
  def consume
    @consumed ||= 0

    messages.each do |message|
      @consumed += 1

      raise StandardError if @consumed == 50

      DataCollector.data[0] << message.raw_payload

      mark_as_consumed(message)
    end
  end
end

Karafka::App.consumer_groups.draw do
  consumer_group DataCollector.consumer_group do
    topic DataCollector.topic do
      consumer Consumer
    end
  end
end

numbers.each { |data| produce(DataCollector.topic, data) }

start_karafka_and_wait_until do
  DataCollector.data[0].size >= 100
end

assert_equal numbers, DataCollector.data[0]
assert_equal 100, DataCollector.data[0].size
assert_equal 1, DataCollector.data.size

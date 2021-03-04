# frozen_string_literal: true

module Karafka
  module Routing
    # rdkafka allows us to group topics subscriptions when they have same settings
    # This builder groups topics from a single consumer group into subscription groups that can be
    # subscribed with one rdkafka connection.
    # This way we save resources as having several rdkafka consumers under the hood is not the
    # cheapest thing in bigger systems
    class SubscriptionGroupsBuilder
      # Keys used to build up a hash for subscription groups distribution
      DISTRIBUTION_KEYS = %i[
        kafka
        max_messages
        max_wait_time
      ].freeze

      private_constant :DISTRIBUTION_KEYS

      # @param topics [Array<Topic>] array with topics based on which we want to build subscription
      #   groups
      # @return [Array<SubscriptionGroup>] all subscription groups we need in separate threads
      def call(topics)
        topics
          .map { [checksum(_1), _1] }
          .group_by(&:first)
          .values
          .map { _1.map(&:last) }
          .map { SubscriptionGroup.new(_1) }
      end

      private

      # @param topic [Karafka::Routing::Topic] topic for which we compute the grouping checksum
      # @return [Integer] checksum that we can use to check if topics have the same set of
      #   settings based on which we group
      def checksum(topic)
        accu = {}

        DISTRIBUTION_KEYS.each { accu[_1] = topic.to_h[_1] }

        accu.hash
      end
    end
  end
end

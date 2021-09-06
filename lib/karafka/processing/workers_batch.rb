# frozen_string_literal: true

module Karafka
  module Processing
    # Abstraction layer around workers batch.
    class WorkersBatch
      include Enumerable

      # @param jobs_queue [JobsQueue]
      # @return [WorkersBatch]
      def initialize(jobs_queue)
        @batch = Array.new(App.config.concurrency) { Processing::Worker.new(jobs_queue) }
      end

      # Iterates over available workers and yields each worker
      def each
        @batch.each { |worker| yield(worker) }
      end
    end
  end
end

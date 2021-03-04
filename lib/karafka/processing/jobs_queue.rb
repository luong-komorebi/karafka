# frozen_string_literal: true

module Karafka
  module Processing
    # This is the key work component for Karafka jobs distribution. It provides API for running
    # jobs in parallel while operating within more than one subscription group.
    #
    # We need to take into consideration fact, that more than one subscription group can operate
    # on this queue, that's why internally we keep track of processing per group
    #
    # We work with the assumption, that partitions data is evenly distributed.
    class JobsQueue
      # How many things we can have in the buffer per executor before triggering `#wait`
      BUFFERS_LIMIT = 5

      private_constant :BUFFERS_LIMIT

      # @return [Karafka::Processing::JobsQueue]
      def initialize
        @queue = ::Queue.new
        @in_processing = Hash.new { |h, k| h[k] = Hash.new  }
        @mutex = Mutex.new
      end

      # Adds the job to the internal main queue, scheduling it for execution in a worker and marks
      # this job as in processing pipeline
      #
      # @param job [Jobs::Base] job that we want to run
      def <<(job)
        # We do not push the job if the queue is closed as it means that it would anyhow not be
        # executed
        return if @queue.closed?

        @mutex.synchronize do
          @in_processing[job.group_id][job.id] = true
        end

        @queue << job
      end

      # @return [Jobs::Base, nil] waits for a job from the main queue and returns it once available
      #   or returns nil if the queue has been stopped and there won't be anything more to process
      #   ever
      # @note This command is blocking and will wait until any job is available on the main queue
      def pop
        @queue.pop
      end

      # Marks a given job from a given group as completed. When there are no more jobs from a given
      # group to be executed, we won't wait.
      def complete(job)
        @mutex.synchronize do
          @in_processing[job.group_id].delete(job.id)
        end
      end

      # Clears the processing states for a provided grup. Useful when a recovery happens and we
      # need to clean up state but only for a given subscription group
      #
      # @param group_id [String]
      def clear(group_id)
        @mutex.synchronize do
          @in_processing[group_id].clear
        end
      end

      # Stops the whole processing queue
      def stop
        @queue.close unless @queue.closed?
      end

      # @param group_id [String]
      # @note Blocking
      def wait(group_id)
        while wait?(group_id)
          Thread.pass
          sleep(0.01)
        end
      end

      private

      def wait?(group_id)
        return false if Karafka::App.stopping?
        return false if @queue.closed?
        return false if @in_processing[group_id].empty?

        true
      end
    end
  end
end

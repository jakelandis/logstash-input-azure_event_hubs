module LogStash
  module Inputs
    module Azure
      class NamedThreadFactory
        include java.util.concurrent.ThreadFactory
        java_import java.util.concurrent.atomic.AtomicInteger

        def initialize(name)
          @name = name
          @counter = AtomicInteger.new(-1)
        end

        def newThread(runnable)
          java.lang.Thread.new(runnable, @name + "-" + @counter.increment_and_get.to_s)
        end
      end
    end
  end
end

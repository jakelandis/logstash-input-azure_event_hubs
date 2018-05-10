module LogStash
  module Inputs
    module Azure
      class NamedThreadFactory
        include java.util.concurrent.ThreadFactory

        def initialize(name)
          @name = name
          @count = -1
        end

        def newThread(runnable)
           @count += 1
           java.lang.Thread.new(runnable, @name + "-" + @count.to_s)
        end

      end
    end
  end
end

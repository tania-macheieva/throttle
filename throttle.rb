# frozen_string_literal: true

require 'monitor'

class Throttler
  class MonotonicClock
    def now
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
  end

  class RealScheduler
    def schedule(delay_seconds, &block)
      Thread.new do
        sleep(delay_seconds)
        block.call
      end
    end
  end

  # Віконне обмеження: не більше `rate` викликів за будь-який інтервал `interval`.
  class SlidingWindow
    def initialize(rate, interval)
      @rate = rate
      @interval = interval
      @stamps = []
    end

    def period
      @interval
    end

    def try_acquire(now)
      purge(now)
      return false if @stamps.size >= @rate

      @stamps << now
      true
    end

    def wait_time(now)
      purge(now)
      @stamps.size < @rate ? 0.0 : @stamps.first + @interval - now
    end

    private

    def purge(now)
      @stamps.shift while @stamps.any? && @stamps.first + @interval <= now
    end
  end

  # Token bucket: ємність `capacity`, поповнення `refill_rate` токенів за секунду.
  class TokenBucket
    EPSILON = 1e-9

    def initialize(capacity, refill_rate)
      @capacity = capacity.to_f
      @refill_rate = refill_rate.to_f
      @tokens = @capacity
      @updated = nil
    end

    def period
      1.0 / @refill_rate
    end

    def try_acquire(now)
      refill(now)
      return false if @tokens < 1 - EPSILON

      @tokens -= 1
      true
    end

    def wait_time(now)
      refill(now)
      @tokens >= 1 - EPSILON ? 0.0 : (1 - @tokens) / @refill_rate
    end

    private

    def refill(now)
      @updated ||= now
      @tokens = [@capacity, @tokens + (now - @updated) * @refill_rate].min
      @updated = now
    end
  end

  def initialize(rate: nil, interval: 1.0, capacity: nil, refill_rate: nil,
                 leading: true, trailing: false, queue: false,
                 clock: MonotonicClock.new, scheduler: RealScheduler.new)
    @limiter = build_limiter(rate, interval, capacity, refill_rate)
    unless leading || trailing || queue
      raise ArgumentError, 'at least one of leading/trailing must be true (or queue: true)'
    end

    @leading = leading
    @trailing = trailing
    @queue = queue
    @clock = clock
    @scheduler = scheduler

    @lock = Monitor.new
    @pending = []
    @timer = nil
  end

  def throttle(fn)
    ->(*args, &block) { invoke([fn, args, block]) }
  end

  private

  def build_limiter(rate, interval, capacity, refill_rate)
    if rate && !capacity && !refill_rate
      SlidingWindow.new(rate, interval)
    elsif capacity && refill_rate && !rate
      TokenBucket.new(capacity, refill_rate)
    else
      raise ArgumentError, 'specify either rate/interval or capacity/refill_rate'
    end
  end

  def invoke(item)
    run_now = false

    @lock.synchronize do
      if @leading && @pending.empty? && @limiter.try_acquire(@clock.now)
        run_now = true
      elsif @queue
        @pending << item
        arm_timer
      elsif @trailing
        @pending = [item]
        arm_timer
      end
    end

    execute(item) if run_now
  end

  def arm_timer
    return if @timer

    wait = @limiter.wait_time(@clock.now)
    @timer = @scheduler.schedule(wait.positive? ? wait : @limiter.period) { tick }
  end

  def tick
    due = []

    @lock.synchronize do
      @timer = nil
      due << @pending.shift while @pending.any? && @limiter.try_acquire(@clock.now)
      arm_timer if @pending.any?
    end

    due.each { |item| execute(item) }
  end

  def execute(item)
    fn, args, block = item
    fn.call(*args, &block)
  end
end

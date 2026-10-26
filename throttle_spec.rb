# frozen_string_literal: true

require 'timeout'
require_relative 'throttle'

# Керований час: водночас годинник і планувальник.
class FakeTime
  Task = Struct.new(:due, :block)

  attr_reader :now

  def initialize
    @now = 0.0
    @tasks = []
  end

  def schedule(delay_seconds, &block)
    Task.new(@now + delay_seconds, block).tap { |task| @tasks << task }
  end

  def advance(seconds)
    target = @now + seconds
    while (task = @tasks.select { |t| t.due <= target }.min_by(&:due))
      @tasks.delete(task)
      @now = [task.due, @now].max
      task.block.call
    end
    @now = target
  end
end

RSpec.describe Throttler do
  let(:time) { FakeTime.new }
  let(:calls) { [] }
  let(:fn) { ->(*args) { calls << args } }

  def build(**options)
    described_class.new(clock: time, scheduler: time, **options)
  end

  describe 'window limit (3 per second)' do
    let(:throttled) { build(rate: 3, interval: 1.0).throttle(fn) }

    it 'executes only the allowed number of 10 calls made within 1 second' do
      10.times do |i|
        throttled.call(i)
        time.advance(0.1)
      end

      expect(calls).to eq([[0], [1], [2]])
    end

    it 'allows new calls once the window has slid' do
      3.times { |i| throttled.call(i) }
      time.advance(1.0)
      3.times { |i| throttled.call(i + 10) }

      expect(calls.size).to eq(6)
    end
  end

  describe 'token bucket (capacity 2, refill 1/s)' do
    let(:throttled) { build(capacity: 2, refill_rate: 1).throttle(fn) }

    it 'allows a burst up to capacity and drops the rest' do
      5.times { |i| throttled.call(i) }

      expect(calls).to eq([[0], [1]])
    end

    it 'refills tokens over time' do
      2.times { |i| throttled.call(i) }
      time.advance(1.0)
      2.times { |i| throttled.call(i + 10) }

      expect(calls).to eq([[0], [1], [10]])
    end
  end

  describe 'leading / trailing (1 per second, excess dropped)' do
    it 'leading only: first call runs at once, the rest are dropped' do
      throttled = build(rate: 1, leading: true, trailing: false).throttle(fn)
      3.times { |i| throttled.call(i) }
      time.advance(1.0)

      expect(calls).to eq([[0]])
    end

    it 'trailing only: nothing at once, the last call runs after the interval' do
      throttled = build(rate: 1, leading: false, trailing: true).throttle(fn)
      3.times { |i| throttled.call(i) }
      expect(calls).to be_empty

      time.advance(0.99)
      expect(calls).to be_empty

      time.advance(0.02)
      expect(calls).to eq([[2]])
    end

    it 'leading + trailing: first call at once, last call after the interval' do
      throttled = build(rate: 1, leading: true, trailing: true).throttle(fn)
      3.times { |i| throttled.call(i) }
      expect(calls).to eq([[0]])

      time.advance(1.0)
      expect(calls).to eq([[0], [2]])
    end

    it 'leading + trailing: a single call is not duplicated' do
      throttled = build(rate: 1, leading: true, trailing: true).throttle(fn)
      throttled.call(:solo)
      time.advance(5.0)

      expect(calls).to eq([[:solo]])
    end
  end

  describe 'queue mode (2 per second)' do
    let(:throttled) { build(rate: 2, queue: true).throttle(fn) }

    it 'preserves the order of calls' do
      6.times { |i| throttled.call(i) }
      expect(calls).to eq([[0], [1]])

      time.advance(1.0)
      expect(calls).to eq([[0], [1], [2], [3]])

      time.advance(1.0)
      expect(calls).to eq([[0], [1], [2], [3], [4], [5]])
    end

    it 'never exceeds the limit while draining' do
      timed = ->(x) { calls << [x, time.now] }
      throttled = build(rate: 2, queue: true).throttle(timed)

      6.times { |i| throttled.call(i) }
      time.advance(2.0)

      expect(calls.map(&:last)).to eq([0.0, 0.0, 1.0, 1.0, 2.0, 2.0])
    end

    it 'does not let a new call jump ahead of queued ones' do
      3.times { |i| throttled.call(i) }
      time.advance(0.5)
      throttled.call(99)
      time.advance(0.5)

      expect(calls).to eq([[0], [1], [2], [99]])
    end

    it 'with leading: false defers every call to the timer' do
      deferred = build(rate: 2, queue: true, leading: false).throttle(fn)
      3.times { |i| deferred.call(i) }
      expect(calls).to be_empty

      time.advance(1.0)
      expect(calls).to eq([[0], [1]])

      time.advance(1.0)
      expect(calls).to eq([[0], [1], [2]])
    end
  end

  describe 'drop mode (2 per second)' do
    it 'discards excess calls permanently' do
      throttled = build(rate: 2).throttle(fn)
      5.times { |i| throttled.call(i) }
      time.advance(10.0)

      expect(calls).to eq([[0], [1]])
    end
  end

  describe 'constructor' do
    it 'requires either rate or capacity/refill_rate' do
      expect { build }.to raise_error(ArgumentError)
      expect { build(rate: 3, capacity: 2, refill_rate: 1) }.to raise_error(ArgumentError)
    end

    it 'rejects leading: false and trailing: false without a queue' do
      expect { build(rate: 1, leading: false, trailing: false) }.to raise_error(ArgumentError)
    end
  end

  describe 'real time' do
    it 'drains a queue in order under a real scheduler' do
      results = Queue.new
      throttled = described_class.new(rate: 2, interval: 0.05, queue: true)
                                 .throttle(->(x) { results << x })

      4.times { |i| throttled.call(i) }

      popped = Timeout.timeout(2) { 4.times.map { results.pop } }
      expect(popped).to eq([0, 1, 2, 3])
    end
  end
end

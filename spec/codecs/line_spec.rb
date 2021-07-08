# encoding: utf-8

require "logstash/devutils/rspec/spec_helper"
require "insist"
require "logstash/codecs/line"
require "logstash/event"
require 'logstash/plugin_mixins/ecs_compatibility_support/spec_helper'

describe LogStash::Codecs::Line, :ecs_compatibility_support do

  subject do
    next LogStash::Codecs::Line.new
  end

  context "#encode" do
    let (:event) {LogStash::Event.new({"message" => "hello world", "host" => "test"})}

    it "should return a default date formatted line" do
      expect(subject).to receive(:on_event).once.and_call_original
      subject.on_event do |e, d|
        insist {d} == event.to_s + "\n"
      end
      subject.encode(event)
    end

    it "should respect the supplied format" do
      format = "%{host}"
      subject.format = format
      expect(subject).to receive(:on_event).once.and_call_original
      subject.on_event do |e, d|
        insist {d} == event.sprintf(format) + "\n"
      end
      subject.encode(event)
    end

    context "when using custom :delimiter" do
      subject do
        next LogStash::Codecs::Line.new("delimiter" => "|")
      end

      it "should append the delimiter to the line" do
        expect(subject).to receive(:on_event).once.and_call_original
        subject.on_event do |e, d|
          insist {d} == event.to_s + "|"
        end
        subject.encode(event)
      end
    end
  end

  context "#decode" do
    it "should return an event from an ascii string" do
      decoded = false
      subject.decode("hello world\n") do |e|
        decoded = true
        insist { e.is_a?(LogStash::Event) }
        insist { e.get("message") } == "hello world"
      end
      insist { decoded } == true
    end

    ecs_compatibility_matrix(:disabled, :v1, :v8 => :v1) do |ecs_select|

      before(:each) do
        allow_any_instance_of(described_class).to receive(:ecs_compatibility).and_return(ecs_compatibility)
      end

      let(:message) { "München" }
      let(:line) { "#{message}\n" }

      it "should return an event from a valid utf-8 string" do
        subject.decode(line) do |e|
          expect( e ).to be_a LogStash::Event
          expect( e.get("message") ).to eql message
          expect( e.get("message").encoding ).to eql Encoding.find('UTF-8')
          expect( e.get("message").valid_encoding? ).to be true
        end
      end

      it "sets event.original in ECS mode" do
        subject.decode(line) do |event|
          expect( event.get("[event][original]") ).to eql message
        end
      end if ecs_select.active_mode != :disabled

    end

    context "when using custom :delimiter" do
      subject do
        next LogStash::Codecs::Line.new("delimiter" => "|")
      end

      it "should not break lines by '\n'" do
        line = "line1\nline2\nline3\n"
        result = []
        subject.decode(line) { |e| result << e }
        subject.flush { |e| result << e }
        expect(result.size).to eq(1)
        expect(result[0].get("message")).to eq(line)
      end

      it "should break lines by that delimiter" do
        result = []
        subject.decode("line1|line2|line3|") { |e| result << e }
        subject.flush { |e| result << e }
        expect(result.size).to eq(3)
        expect(result[0].get("message")).to eq("line1")
        expect(result[1].get("message")).to eq("line2")
        expect(result[2].get("message")).to eq("line3")
      end
    end
  end

  context "#flush" do
    it "should convert charsets" do
      garbage = [0xD0].pack("C")
      subject.decode(garbage) do |e|
        fail "Should not get here."
      end
      count = 0
      subject.flush do |event|
        count += 1
        insist { event.get("message").encoding } == Encoding::UTF_8
      end
      insist { count } == 1
    end
  end
end

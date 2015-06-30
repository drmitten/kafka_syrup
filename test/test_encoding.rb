# encoding: ASCII-8BIT

require 'minitest_helper'

class TestEncoding < Minitest::Test
  def obj
    @obj ||= KafkaSyrup::Encoding
  end

  def test_writing_integers
    assert_equal "\xC8", obj.write_int8(200)
    assert_equal "\x00\xC8", obj.write_int16(200)
    assert_equal "\x00\x00\x00\xC8", obj.write_int32(200)
    assert_equal "\x00\x00\x00\x00\x00\x00\x00\xC8", obj.write_int64(200)
  end

  def test_reading_integers
    assert_equal 45, obj.read_int8(StringIO.new("-"))
    assert_equal 456, obj.read_int16(StringIO.new("\x01\xC8"))
    assert_equal 456789, obj.read_int32(StringIO.new("\x00\x06\xF8U"))
    assert_equal 4567890123, obj.read_int64(StringIO.new("\x00\x00\x00\x01\x10Dx\xCB"))
  end

  def test_writing_strings_and_bytes
    assert_equal "\x00\x03foo", obj.write_string('foo')
    assert_equal "\x00\x00\x00\x03foo", obj.write_bytes('foo')
  end

  def test_reading_strings_and_bytes
    assert_equal 'foo', obj.read_string(StringIO.new("\x00\x03foo"))
    assert_equal 'foo', obj.read_bytes(StringIO.new("\x00\x00\x00\x03foo"))
  end

  def test_writing_arrays_with_block
    assert_equal "\x00\x00\x00\x02\x00\x03foo\x00\x03bar", obj.write_array(['foo', 'bar'], &obj.method(:write_string))
  end

  def test_writing_arrays_with_encodable_items
    item = Struct.new(:val) do
      def encode
        KafkaSyrup::Encoding.write_string(val)
      end
    end

    assert_equal "\x00\x00\x00\x02\x00\x03foo\x00\x03bar", obj.write_array([item.new(:foo), item.new(:bar)])
  end

  def test_reading_arrays
    assert_equal ['foo', 'bar'], obj.read_array(StringIO.new("\x00\x00\x00\x02\x00\x03foo\x00\x03bar"), &obj.method(:read_string))
  end
end

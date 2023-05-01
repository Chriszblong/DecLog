package com.yahoo.ycsb.db; /**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.BitSet;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.thrift.*;
import com.facebook.thrift.async.*;
import com.facebook.thrift.meta_data.*;
import com.facebook.thrift.server.*;
import com.facebook.thrift.transport.*;
import com.facebook.thrift.protocol.*;

@SuppressWarnings({ "unused", "serial" })
public class TimeSeriesBlock implements TBase, java.io.Serializable, Cloneable, Comparable<TimeSeriesBlock> {
  private static final TStruct STRUCT_DESC = new TStruct("TimeSeriesBlock");
  private static final TField COMPRESSION_FIELD_DESC = new TField("compression", TType.I32, (short)1);
  private static final TField COUNT_FIELD_DESC = new TField("count", TType.I32, (short)2);
  private static final TField DATA_FIELD_DESC = new TField("data", TType.STRING, (short)3);

  /**
   * 
   * @see Compression
   */
  public int compression;
  public int count;
  public byte[] data;
  public static final int COMPRESSION = 1;
  public static final int COUNT = 2;
  public static final int DATA = 3;
  public static boolean DEFAULT_PRETTY_PRINT = true;

  // isset id assignments
  private static final int __COMPRESSION_ISSET_ID = 0;
  private static final int __COUNT_ISSET_ID = 1;
  private BitSet __isset_bit_vector = new BitSet(2);

  public static final Map<Integer, FieldMetaData> metaDataMap;
  static {
    Map<Integer, FieldMetaData> tmpMetaDataMap = new HashMap<Integer, FieldMetaData>();
    tmpMetaDataMap.put(COMPRESSION, new FieldMetaData("compression", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    tmpMetaDataMap.put(COUNT, new FieldMetaData("count", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    tmpMetaDataMap.put(DATA, new FieldMetaData("data", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMetaDataMap);
  }

  static {
    FieldMetaData.addStructMetaDataMap(TimeSeriesBlock.class, metaDataMap);
  }

  public TimeSeriesBlock() {
  }

  public TimeSeriesBlock(
    int compression,
    int count,
    byte[] data)
  {
    this();
    this.compression = compression;
    setCompressionIsSet(true);
    this.count = count;
    setCountIsSet(true);
    this.data = data;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TimeSeriesBlock(TimeSeriesBlock other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    this.compression = TBaseHelper.deepCopy(other.compression);
    this.count = TBaseHelper.deepCopy(other.count);
    if (other.isSetData()) {
      this.data = TBaseHelper.deepCopy(other.data);
    }
  }

  public TimeSeriesBlock deepCopy() {
    return new TimeSeriesBlock(this);
  }

  @Deprecated
  public TimeSeriesBlock clone() {
    return new TimeSeriesBlock(this);
  }

  /**
   * 
   * @see Compression
   */
  public int  getCompression() {
    return this.compression;
  }

  /**
   * 
   * @see Compression
   */
  public TimeSeriesBlock setCompression(int compression) {
    this.compression = compression;
    setCompressionIsSet(true);
    return this;
  }

  public void unsetCompression() {
    __isset_bit_vector.clear(__COMPRESSION_ISSET_ID);
  }

  // Returns true if field compression is set (has been assigned a value) and false otherwise
  public boolean isSetCompression() {
    return __isset_bit_vector.get(__COMPRESSION_ISSET_ID);
  }

  public void setCompressionIsSet(boolean value) {
    __isset_bit_vector.set(__COMPRESSION_ISSET_ID, value);
  }

  public int  getCount() {
    return this.count;
  }

  public TimeSeriesBlock setCount(int count) {
    this.count = count;
    setCountIsSet(true);
    return this;
  }

  public void unsetCount() {
    __isset_bit_vector.clear(__COUNT_ISSET_ID);
  }

  // Returns true if field count is set (has been assigned a value) and false otherwise
  public boolean isSetCount() {
    return __isset_bit_vector.get(__COUNT_ISSET_ID);
  }

  public void setCountIsSet(boolean value) {
    __isset_bit_vector.set(__COUNT_ISSET_ID, value);
  }

  public byte[]  getData() {
    return this.data;
  }

  public TimeSeriesBlock setData(byte[] data) {
    this.data = data;
    return this;
  }

  public void unsetData() {
    this.data = null;
  }

  // Returns true if field data is set (has been assigned a value) and false otherwise
  public boolean isSetData() {
    return this.data != null;
  }

  public void setDataIsSet(boolean value) {
    if (!value) {
      this.data = null;
    }
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case COMPRESSION:
      if (value == null) {
        unsetCompression();
      } else {
        setCompression((Integer)value);
      }
      break;

    case COUNT:
      if (value == null) {
        unsetCount();
      } else {
        setCount((Integer)value);
      }
      break;

    case DATA:
      if (value == null) {
        unsetData();
      } else {
        setData((byte[])value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case COMPRESSION:
      return getCompression();

    case COUNT:
      return new Integer(getCount());

    case DATA:
      return getData();

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case COMPRESSION:
      return isSetCompression();
    case COUNT:
      return isSetCount();
    case DATA:
      return isSetData();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TimeSeriesBlock)
      return this.equals((TimeSeriesBlock)that);
    return false;
  }

  public boolean equals(TimeSeriesBlock that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_compression = true;
    boolean that_present_compression = true;
    if (this_present_compression || that_present_compression) {
      if (!(this_present_compression && that_present_compression))
        return false;
      if (!TBaseHelper.equalsNobinary(this.compression, that.compression))
        return false;
    }

    boolean this_present_count = true;
    boolean that_present_count = true;
    if (this_present_count || that_present_count) {
      if (!(this_present_count && that_present_count))
        return false;
      if (!TBaseHelper.equalsNobinary(this.count, that.count))
        return false;
    }

    boolean this_present_data = true && this.isSetData();
    boolean that_present_data = true && that.isSetData();
    if (this_present_data || that_present_data) {
      if (!(this_present_data && that_present_data))
        return false;
      if (!TBaseHelper.equalsSlow(this.data, that.data))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(TimeSeriesBlock other) {
    if (other == null) {
      // See java.lang.Comparable docs
      throw new NullPointerException();
    }

    if (other == this) {
      return 0;
    }
    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetCompression()).compareTo(other.isSetCompression());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(compression, other.compression);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetCount()).compareTo(other.isSetCount());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(count, other.count);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetData()).compareTo(other.isSetData());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(data, other.data);
    if (lastComparison != 0) {
      return lastComparison;
    }
    return 0;
  }

  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin(metaDataMap);
    while (true)
    {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) { 
        break;
      }
      switch (field.id)
      {
        case COMPRESSION:
          if (field.type == TType.I32) {
            this.compression = iprot.readI32();
            setCompressionIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case COUNT:
          if (field.type == TType.I32) {
            this.count = iprot.readI32();
            setCountIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case DATA:
          if (field.type == TType.STRING) {
            this.data = iprot.readBinary();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        default:
          TProtocolUtil.skip(iprot, field.type);
          break;
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();


    // check for required fields of primitive type, which can't be checked in the validate method
    validate();
  }

  public void write(TProtocol oprot) throws TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    oprot.writeFieldBegin(COMPRESSION_FIELD_DESC);
    oprot.writeI32(this.compression);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(COUNT_FIELD_DESC);
    oprot.writeI32(this.count);
    oprot.writeFieldEnd();
    if (this.data != null) {
      oprot.writeFieldBegin(DATA_FIELD_DESC);
      oprot.writeBinary(this.data);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    return toString(DEFAULT_PRETTY_PRINT);
  }

  @Override
  public String toString(boolean prettyPrint) {
    return toString(1, prettyPrint);
  }

  @Override
  public String toString(int indent, boolean prettyPrint) {
    String indentStr = prettyPrint ? TBaseHelper.getIndentedString(indent) : "";
    String newLine = prettyPrint ? "\n" : "";
String space = prettyPrint ? " " : "";
    StringBuilder sb = new StringBuilder("TimeSeriesBlock");
    sb.append(space);
    sb.append("(");
    sb.append(newLine);
    boolean first = true;

    sb.append(indentStr);
    sb.append("compression");
    sb.append(space);
    sb.append(":").append(space);
    String compression_name = Compression.VALUES_TO_NAMES.get(this. getCompression());
    if (compression_name != null) {
      sb.append(compression_name);
      sb.append(" (");
    }
    sb.append(this. getCompression());
    if (compression_name != null) {
      sb.append(")");
    }
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("count");
    sb.append(space);
    sb.append(":").append(space);
    sb.append(TBaseHelper.toString(this. getCount(), indent + 1, prettyPrint));
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("data");
    sb.append(space);
    sb.append(":").append(space);
    if (this. getData() == null) {
      sb.append("null");
    } else {
        int __data_size = Math.min(this. getData().length, 128);
        for (int i = 0; i < __data_size; i++) {
          if (i != 0) sb.append(" ");
          sb.append(Integer.toHexString(this. getData()[i]).length() > 1 ? Integer.toHexString(this. getData()[i]).substring(Integer.toHexString(this. getData()[i]).length() - 2).toUpperCase() : "0" + Integer.toHexString(this. getData()[i]).toUpperCase());
        }
        if (this. getData().length > 128) sb.append(" ...");
    }
    first = false;
    sb.append(newLine + TBaseHelper.reduceIndent(indentStr));
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check that fields of type enum have valid values
    if (isSetCompression() && !Compression.VALID_VALUES.contains(compression)){
      throw new TProtocolException("The field 'compression' has been assigned the invalid value " + compression);
    }
  }

}


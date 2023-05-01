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
public class DeleteSeriesDataStatus implements TBase, java.io.Serializable, Cloneable, Comparable<DeleteSeriesDataStatus> {
  private static final TStruct STRUCT_DESC = new TStruct("DeleteSeriesDataStatus");
  private static final TField KEY_FIELD_DESC = new TField("key", TType.STRUCT, (short)1);
  private static final TField BEGIN_FIELD_DESC = new TField("begin", TType.I64, (short)2);
  private static final TField END_FIELD_DESC = new TField("end", TType.I64, (short)3);
  private static final TField STATUS_FIELD_DESC = new TField("status", TType.I32, (short)4);

  public Key key;
  public long begin;
  public long end;
  /**
   * 
   * @see DeleteStatusCode
   */
  public int status;
  public static final int KEY = 1;
  public static final int BEGIN = 2;
  public static final int END = 3;
  public static final int STATUS = 4;
  public static boolean DEFAULT_PRETTY_PRINT = true;

  // isset id assignments
  private static final int __BEGIN_ISSET_ID = 0;
  private static final int __END_ISSET_ID = 1;
  private static final int __STATUS_ISSET_ID = 2;
  private BitSet __isset_bit_vector = new BitSet(3);

  public static final Map<Integer, FieldMetaData> metaDataMap;
  static {
    Map<Integer, FieldMetaData> tmpMetaDataMap = new HashMap<Integer, FieldMetaData>();
    tmpMetaDataMap.put(KEY, new FieldMetaData("key", TFieldRequirementType.DEFAULT, 
        new StructMetaData(TType.STRUCT, Key.class)));
    tmpMetaDataMap.put(BEGIN, new FieldMetaData("begin", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I64)));
    tmpMetaDataMap.put(END, new FieldMetaData("end", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I64)));
    tmpMetaDataMap.put(STATUS, new FieldMetaData("status", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    metaDataMap = Collections.unmodifiableMap(tmpMetaDataMap);
  }

  static {
    FieldMetaData.addStructMetaDataMap(DeleteSeriesDataStatus.class, metaDataMap);
  }

  public DeleteSeriesDataStatus() {
  }

  public DeleteSeriesDataStatus(
    Key key,
    long begin,
    long end,
    int status)
  {
    this();
    this.key = key;
    this.begin = begin;
    setBeginIsSet(true);
    this.end = end;
    setEndIsSet(true);
    this.status = status;
    setStatusIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public DeleteSeriesDataStatus(DeleteSeriesDataStatus other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetKey()) {
      this.key = TBaseHelper.deepCopy(other.key);
    }
    this.begin = TBaseHelper.deepCopy(other.begin);
    this.end = TBaseHelper.deepCopy(other.end);
    this.status = TBaseHelper.deepCopy(other.status);
  }

  public DeleteSeriesDataStatus deepCopy() {
    return new DeleteSeriesDataStatus(this);
  }

  @Deprecated
  public DeleteSeriesDataStatus clone() {
    return new DeleteSeriesDataStatus(this);
  }

  public Key getKey() {
    return this.key;
  }

  public DeleteSeriesDataStatus setKey(Key key) {
    this.key = key;
    return this;
  }

  public void unsetKey() {
    this.key = null;
  }

  // Returns true if field key is set (has been assigned a value) and false otherwise
  public boolean isSetKey() {
    return this.key != null;
  }

  public void setKeyIsSet(boolean value) {
    if (!value) {
      this.key = null;
    }
  }

  public long  getBegin() {
    return this.begin;
  }

  public DeleteSeriesDataStatus setBegin(long begin) {
    this.begin = begin;
    setBeginIsSet(true);
    return this;
  }

  public void unsetBegin() {
    __isset_bit_vector.clear(__BEGIN_ISSET_ID);
  }

  // Returns true if field begin is set (has been assigned a value) and false otherwise
  public boolean isSetBegin() {
    return __isset_bit_vector.get(__BEGIN_ISSET_ID);
  }

  public void setBeginIsSet(boolean value) {
    __isset_bit_vector.set(__BEGIN_ISSET_ID, value);
  }

  public long  getEnd() {
    return this.end;
  }

  public DeleteSeriesDataStatus setEnd(long end) {
    this.end = end;
    setEndIsSet(true);
    return this;
  }

  public void unsetEnd() {
    __isset_bit_vector.clear(__END_ISSET_ID);
  }

  // Returns true if field end is set (has been assigned a value) and false otherwise
  public boolean isSetEnd() {
    return __isset_bit_vector.get(__END_ISSET_ID);
  }

  public void setEndIsSet(boolean value) {
    __isset_bit_vector.set(__END_ISSET_ID, value);
  }

  /**
   * 
   * @see DeleteStatusCode
   */
  public int  getStatus() {
    return this.status;
  }

  /**
   * 
   * @see DeleteStatusCode
   */
  public DeleteSeriesDataStatus setStatus(int status) {
    this.status = status;
    setStatusIsSet(true);
    return this;
  }

  public void unsetStatus() {
    __isset_bit_vector.clear(__STATUS_ISSET_ID);
  }

  // Returns true if field status is set (has been assigned a value) and false otherwise
  public boolean isSetStatus() {
    return __isset_bit_vector.get(__STATUS_ISSET_ID);
  }

  public void setStatusIsSet(boolean value) {
    __isset_bit_vector.set(__STATUS_ISSET_ID, value);
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case KEY:
      if (value == null) {
        unsetKey();
      } else {
        setKey((Key)value);
      }
      break;

    case BEGIN:
      if (value == null) {
        unsetBegin();
      } else {
        setBegin((Long)value);
      }
      break;

    case END:
      if (value == null) {
        unsetEnd();
      } else {
        setEnd((Long)value);
      }
      break;

    case STATUS:
      if (value == null) {
        unsetStatus();
      } else {
        setStatus((Integer)value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case KEY:
      return getKey();

    case BEGIN:
      return new Long(getBegin());

    case END:
      return new Long(getEnd());

    case STATUS:
      return getStatus();

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case KEY:
      return isSetKey();
    case BEGIN:
      return isSetBegin();
    case END:
      return isSetEnd();
    case STATUS:
      return isSetStatus();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof DeleteSeriesDataStatus)
      return this.equals((DeleteSeriesDataStatus)that);
    return false;
  }

  public boolean equals(DeleteSeriesDataStatus that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_key = true && this.isSetKey();
    boolean that_present_key = true && that.isSetKey();
    if (this_present_key || that_present_key) {
      if (!(this_present_key && that_present_key))
        return false;
      if (!TBaseHelper.equalsNobinary(this.key, that.key))
        return false;
    }

    boolean this_present_begin = true;
    boolean that_present_begin = true;
    if (this_present_begin || that_present_begin) {
      if (!(this_present_begin && that_present_begin))
        return false;
      if (!TBaseHelper.equalsNobinary(this.begin, that.begin))
        return false;
    }

    boolean this_present_end = true;
    boolean that_present_end = true;
    if (this_present_end || that_present_end) {
      if (!(this_present_end && that_present_end))
        return false;
      if (!TBaseHelper.equalsNobinary(this.end, that.end))
        return false;
    }

    boolean this_present_status = true;
    boolean that_present_status = true;
    if (this_present_status || that_present_status) {
      if (!(this_present_status && that_present_status))
        return false;
      if (!TBaseHelper.equalsNobinary(this.status, that.status))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(DeleteSeriesDataStatus other) {
    if (other == null) {
      // See java.lang.Comparable docs
      throw new NullPointerException();
    }

    if (other == this) {
      return 0;
    }
    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetKey()).compareTo(other.isSetKey());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(key, other.key);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetBegin()).compareTo(other.isSetBegin());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(begin, other.begin);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetEnd()).compareTo(other.isSetEnd());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(end, other.end);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetStatus()).compareTo(other.isSetStatus());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(status, other.status);
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
        case KEY:
          if (field.type == TType.STRUCT) {
            this.key = new Key();
            this.key.read(iprot);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case BEGIN:
          if (field.type == TType.I64) {
            this.begin = iprot.readI64();
            setBeginIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case END:
          if (field.type == TType.I64) {
            this.end = iprot.readI64();
            setEndIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STATUS:
          if (field.type == TType.I32) {
            this.status = iprot.readI32();
            setStatusIsSet(true);
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
    if (this.key != null) {
      oprot.writeFieldBegin(KEY_FIELD_DESC);
      this.key.write(oprot);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(BEGIN_FIELD_DESC);
    oprot.writeI64(this.begin);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(END_FIELD_DESC);
    oprot.writeI64(this.end);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(STATUS_FIELD_DESC);
    oprot.writeI32(this.status);
    oprot.writeFieldEnd();
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
    StringBuilder sb = new StringBuilder("DeleteSeriesDataStatus");
    sb.append(space);
    sb.append("(");
    sb.append(newLine);
    boolean first = true;

    sb.append(indentStr);
    sb.append("key");
    sb.append(space);
    sb.append(":").append(space);
    if (this. getKey() == null) {
      sb.append("null");
    } else {
      sb.append(TBaseHelper.toString(this. getKey(), indent + 1, prettyPrint));
    }
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("begin");
    sb.append(space);
    sb.append(":").append(space);
    sb.append(TBaseHelper.toString(this. getBegin(), indent + 1, prettyPrint));
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("end");
    sb.append(space);
    sb.append(":").append(space);
    sb.append(TBaseHelper.toString(this. getEnd(), indent + 1, prettyPrint));
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("status");
    sb.append(space);
    sb.append(":").append(space);
    String status_name = DeleteStatusCode.VALUES_TO_NAMES.get(this. getStatus());
    if (status_name != null) {
      sb.append(status_name);
      sb.append(" (");
    }
    sb.append(this. getStatus());
    if (status_name != null) {
      sb.append(")");
    }
    first = false;
    sb.append(newLine + TBaseHelper.reduceIndent(indentStr));
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check that fields of type enum have valid values
    if (isSetStatus() && !DeleteStatusCode.VALID_VALUES.contains(status)){
      throw new TProtocolException("The field 'status' has been assigned the invalid value " + status);
    }
  }

}


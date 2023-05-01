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
public class TimeValueCategoryTuple implements TBase, java.io.Serializable, Cloneable, Comparable<TimeValueCategoryTuple> {
  private static final TStruct STRUCT_DESC = new TStruct("TimeValueCategoryTuple");
  private static final TField UNIX_TIME_FIELD_DESC = new TField("unixTime", TType.I64, (short)1);
  private static final TField VALUE_FIELD_DESC = new TField("value", TType.DOUBLE, (short)2);
  private static final TField CATEGORY_ID_FIELD_DESC = new TField("categoryId", TType.I32, (short)3);

  public long unixTime;
  public double value;
  public int categoryId;
  public static final int UNIXTIME = 1;
  public static final int VALUE = 2;
  public static final int CATEGORYID = 3;
  public static boolean DEFAULT_PRETTY_PRINT = true;

  // isset id assignments
  private static final int __UNIXTIME_ISSET_ID = 0;
  private static final int __VALUE_ISSET_ID = 1;
  private static final int __CATEGORYID_ISSET_ID = 2;
  private BitSet __isset_bit_vector = new BitSet(3);

  public static final Map<Integer, FieldMetaData> metaDataMap;
  static {
    Map<Integer, FieldMetaData> tmpMetaDataMap = new HashMap<Integer, FieldMetaData>();
    tmpMetaDataMap.put(UNIXTIME, new FieldMetaData("unixTime", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I64)));
    tmpMetaDataMap.put(VALUE, new FieldMetaData("value", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.DOUBLE)));
    tmpMetaDataMap.put(CATEGORYID, new FieldMetaData("categoryId", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    metaDataMap = Collections.unmodifiableMap(tmpMetaDataMap);
  }

  static {
    FieldMetaData.addStructMetaDataMap(TimeValueCategoryTuple.class, metaDataMap);
  }

  public TimeValueCategoryTuple() {
  }

  public TimeValueCategoryTuple(
    long unixTime,
    double value,
    int categoryId)
  {
    this();
    this.unixTime = unixTime;
    setUnixTimeIsSet(true);
    this.value = value;
    setValueIsSet(true);
    this.categoryId = categoryId;
    setCategoryIdIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TimeValueCategoryTuple(TimeValueCategoryTuple other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    this.unixTime = TBaseHelper.deepCopy(other.unixTime);
    this.value = TBaseHelper.deepCopy(other.value);
    this.categoryId = TBaseHelper.deepCopy(other.categoryId);
  }

  public TimeValueCategoryTuple deepCopy() {
    return new TimeValueCategoryTuple(this);
  }

  @Deprecated
  public TimeValueCategoryTuple clone() {
    return new TimeValueCategoryTuple(this);
  }

  public long  getUnixTime() {
    return this.unixTime;
  }

  public TimeValueCategoryTuple setUnixTime(long unixTime) {
    this.unixTime = unixTime;
    setUnixTimeIsSet(true);
    return this;
  }

  public void unsetUnixTime() {
    __isset_bit_vector.clear(__UNIXTIME_ISSET_ID);
  }

  // Returns true if field unixTime is set (has been assigned a value) and false otherwise
  public boolean isSetUnixTime() {
    return __isset_bit_vector.get(__UNIXTIME_ISSET_ID);
  }

  public void setUnixTimeIsSet(boolean value) {
    __isset_bit_vector.set(__UNIXTIME_ISSET_ID, value);
  }

  public double  getValue() {
    return this.value;
  }

  public TimeValueCategoryTuple setValue(double value) {
    this.value = value;
    setValueIsSet(true);
    return this;
  }

  public void unsetValue() {
    __isset_bit_vector.clear(__VALUE_ISSET_ID);
  }

  // Returns true if field value is set (has been assigned a value) and false otherwise
  public boolean isSetValue() {
    return __isset_bit_vector.get(__VALUE_ISSET_ID);
  }

  public void setValueIsSet(boolean value) {
    __isset_bit_vector.set(__VALUE_ISSET_ID, value);
  }

  public int  getCategoryId() {
    return this.categoryId;
  }

  public TimeValueCategoryTuple setCategoryId(int categoryId) {
    this.categoryId = categoryId;
    setCategoryIdIsSet(true);
    return this;
  }

  public void unsetCategoryId() {
    __isset_bit_vector.clear(__CATEGORYID_ISSET_ID);
  }

  // Returns true if field categoryId is set (has been assigned a value) and false otherwise
  public boolean isSetCategoryId() {
    return __isset_bit_vector.get(__CATEGORYID_ISSET_ID);
  }

  public void setCategoryIdIsSet(boolean value) {
    __isset_bit_vector.set(__CATEGORYID_ISSET_ID, value);
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case UNIXTIME:
      if (value == null) {
        unsetUnixTime();
      } else {
        setUnixTime((Long)value);
      }
      break;

    case VALUE:
      if (value == null) {
        unsetValue();
      } else {
        setValue((Double)value);
      }
      break;

    case CATEGORYID:
      if (value == null) {
        unsetCategoryId();
      } else {
        setCategoryId((Integer)value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case UNIXTIME:
      return new Long(getUnixTime());

    case VALUE:
      return new Double(getValue());

    case CATEGORYID:
      return new Integer(getCategoryId());

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case UNIXTIME:
      return isSetUnixTime();
    case VALUE:
      return isSetValue();
    case CATEGORYID:
      return isSetCategoryId();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TimeValueCategoryTuple)
      return this.equals((TimeValueCategoryTuple)that);
    return false;
  }

  public boolean equals(TimeValueCategoryTuple that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_unixTime = true;
    boolean that_present_unixTime = true;
    if (this_present_unixTime || that_present_unixTime) {
      if (!(this_present_unixTime && that_present_unixTime))
        return false;
      if (!TBaseHelper.equalsNobinary(this.unixTime, that.unixTime))
        return false;
    }

    boolean this_present_value = true;
    boolean that_present_value = true;
    if (this_present_value || that_present_value) {
      if (!(this_present_value && that_present_value))
        return false;
      if (!TBaseHelper.equalsNobinary(this.value, that.value))
        return false;
    }

    boolean this_present_categoryId = true;
    boolean that_present_categoryId = true;
    if (this_present_categoryId || that_present_categoryId) {
      if (!(this_present_categoryId && that_present_categoryId))
        return false;
      if (!TBaseHelper.equalsNobinary(this.categoryId, that.categoryId))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(TimeValueCategoryTuple other) {
    if (other == null) {
      // See java.lang.Comparable docs
      throw new NullPointerException();
    }

    if (other == this) {
      return 0;
    }
    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetUnixTime()).compareTo(other.isSetUnixTime());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(unixTime, other.unixTime);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetValue()).compareTo(other.isSetValue());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(value, other.value);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetCategoryId()).compareTo(other.isSetCategoryId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(categoryId, other.categoryId);
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
        case UNIXTIME:
          if (field.type == TType.I64) {
            this.unixTime = iprot.readI64();
            setUnixTimeIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case VALUE:
          if (field.type == TType.DOUBLE) {
            this.value = iprot.readDouble();
            setValueIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case CATEGORYID:
          if (field.type == TType.I32) {
            this.categoryId = iprot.readI32();
            setCategoryIdIsSet(true);
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
    oprot.writeFieldBegin(UNIX_TIME_FIELD_DESC);
    oprot.writeI64(this.unixTime);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(VALUE_FIELD_DESC);
    oprot.writeDouble(this.value);
    oprot.writeFieldEnd();
    oprot.writeFieldBegin(CATEGORY_ID_FIELD_DESC);
    oprot.writeI32(this.categoryId);
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
    StringBuilder sb = new StringBuilder("TimeValueCategoryTuple");
    sb.append(space);
    sb.append("(");
    sb.append(newLine);
    boolean first = true;

    sb.append(indentStr);
    sb.append("unixTime");
    sb.append(space);
    sb.append(":").append(space);
    sb.append(TBaseHelper.toString(this. getUnixTime(), indent + 1, prettyPrint));
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("value");
    sb.append(space);
    sb.append(":").append(space);
    sb.append(TBaseHelper.toString(this. getValue(), indent + 1, prettyPrint));
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("categoryId");
    sb.append(space);
    sb.append(":").append(space);
    sb.append(TBaseHelper.toString(this. getCategoryId(), indent + 1, prettyPrint));
    first = false;
    sb.append(newLine + TBaseHelper.reduceIndent(indentStr));
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check that fields of type enum have valid values
  }

}


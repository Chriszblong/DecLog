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
public class QueryRange implements TBase, java.io.Serializable, Cloneable, Comparable<QueryRange> {
  private static final TStruct STRUCT_DESC = new TStruct("QueryRange");
  private static final TField FROM_FIELD_DESC = new TField("from", TType.STRING, (short)1);
  private static final TField TO_FIELD_DESC = new TField("to", TType.STRING, (short)2);
  private static final TField RAW_FIELD_DESC = new TField("raw", TType.STRUCT, (short)3);

  public String from;
  public String to;
  public QueryRawRange raw;
  public static final int FROM = 1;
  public static final int TO = 2;
  public static final int RAW = 3;
  public static boolean DEFAULT_PRETTY_PRINT = true;

  // isset id assignments

  public static final Map<Integer, FieldMetaData> metaDataMap;
  static {
    Map<Integer, FieldMetaData> tmpMetaDataMap = new HashMap<Integer, FieldMetaData>();
    tmpMetaDataMap.put(FROM, new FieldMetaData("from", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    tmpMetaDataMap.put(TO, new FieldMetaData("to", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.STRING)));
    tmpMetaDataMap.put(RAW, new FieldMetaData("raw", TFieldRequirementType.DEFAULT, 
        new StructMetaData(TType.STRUCT, QueryRawRange.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMetaDataMap);
  }

  static {
    FieldMetaData.addStructMetaDataMap(QueryRange.class, metaDataMap);
  }

  public QueryRange() {
  }

  public QueryRange(
    String from,
    String to,
    QueryRawRange raw)
  {
    this();
    this.from = from;
    this.to = to;
    this.raw = raw;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public QueryRange(QueryRange other) {
    if (other.isSetFrom()) {
      this.from = TBaseHelper.deepCopy(other.from);
    }
    if (other.isSetTo()) {
      this.to = TBaseHelper.deepCopy(other.to);
    }
    if (other.isSetRaw()) {
      this.raw = TBaseHelper.deepCopy(other.raw);
    }
  }

  public QueryRange deepCopy() {
    return new QueryRange(this);
  }

  @Deprecated
  public QueryRange clone() {
    return new QueryRange(this);
  }

  public String  getFrom() {
    return this.from;
  }

  public QueryRange setFrom(String from) {
    this.from = from;
    return this;
  }

  public void unsetFrom() {
    this.from = null;
  }

  // Returns true if field from is set (has been assigned a value) and false otherwise
  public boolean isSetFrom() {
    return this.from != null;
  }

  public void setFromIsSet(boolean value) {
    if (!value) {
      this.from = null;
    }
  }

  public String  getTo() {
    return this.to;
  }

  public QueryRange setTo(String to) {
    this.to = to;
    return this;
  }

  public void unsetTo() {
    this.to = null;
  }

  // Returns true if field to is set (has been assigned a value) and false otherwise
  public boolean isSetTo() {
    return this.to != null;
  }

  public void setToIsSet(boolean value) {
    if (!value) {
      this.to = null;
    }
  }

  public QueryRawRange getRaw() {
    return this.raw;
  }

  public QueryRange setRaw(QueryRawRange raw) {
    this.raw = raw;
    return this;
  }

  public void unsetRaw() {
    this.raw = null;
  }

  // Returns true if field raw is set (has been assigned a value) and false otherwise
  public boolean isSetRaw() {
    return this.raw != null;
  }

  public void setRawIsSet(boolean value) {
    if (!value) {
      this.raw = null;
    }
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case FROM:
      if (value == null) {
        unsetFrom();
      } else {
        setFrom((String)value);
      }
      break;

    case TO:
      if (value == null) {
        unsetTo();
      } else {
        setTo((String)value);
      }
      break;

    case RAW:
      if (value == null) {
        unsetRaw();
      } else {
        setRaw((QueryRawRange)value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case FROM:
      return getFrom();

    case TO:
      return getTo();

    case RAW:
      return getRaw();

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case FROM:
      return isSetFrom();
    case TO:
      return isSetTo();
    case RAW:
      return isSetRaw();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof QueryRange)
      return this.equals((QueryRange)that);
    return false;
  }

  public boolean equals(QueryRange that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_from = true && this.isSetFrom();
    boolean that_present_from = true && that.isSetFrom();
    if (this_present_from || that_present_from) {
      if (!(this_present_from && that_present_from))
        return false;
      if (!TBaseHelper.equalsNobinary(this.from, that.from))
        return false;
    }

    boolean this_present_to = true && this.isSetTo();
    boolean that_present_to = true && that.isSetTo();
    if (this_present_to || that_present_to) {
      if (!(this_present_to && that_present_to))
        return false;
      if (!TBaseHelper.equalsNobinary(this.to, that.to))
        return false;
    }

    boolean this_present_raw = true && this.isSetRaw();
    boolean that_present_raw = true && that.isSetRaw();
    if (this_present_raw || that_present_raw) {
      if (!(this_present_raw && that_present_raw))
        return false;
      if (!TBaseHelper.equalsNobinary(this.raw, that.raw))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(QueryRange other) {
    if (other == null) {
      // See java.lang.Comparable docs
      throw new NullPointerException();
    }

    if (other == this) {
      return 0;
    }
    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetFrom()).compareTo(other.isSetFrom());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(from, other.from);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetTo()).compareTo(other.isSetTo());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(to, other.to);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetRaw()).compareTo(other.isSetRaw());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(raw, other.raw);
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
        case FROM:
          if (field.type == TType.STRING) {
            this.from = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case TO:
          if (field.type == TType.STRING) {
            this.to = iprot.readString();
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case RAW:
          if (field.type == TType.STRUCT) {
            this.raw = new QueryRawRange();
            this.raw.read(iprot);
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
    if (this.from != null) {
      oprot.writeFieldBegin(FROM_FIELD_DESC);
      oprot.writeString(this.from);
      oprot.writeFieldEnd();
    }
    if (this.to != null) {
      oprot.writeFieldBegin(TO_FIELD_DESC);
      oprot.writeString(this.to);
      oprot.writeFieldEnd();
    }
    if (this.raw != null) {
      oprot.writeFieldBegin(RAW_FIELD_DESC);
      this.raw.write(oprot);
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
    StringBuilder sb = new StringBuilder("QueryRange");
    sb.append(space);
    sb.append("(");
    sb.append(newLine);
    boolean first = true;

    sb.append(indentStr);
    sb.append("from");
    sb.append(space);
    sb.append(":").append(space);
    if (this. getFrom() == null) {
      sb.append("null");
    } else {
      sb.append(TBaseHelper.toString(this. getFrom(), indent + 1, prettyPrint));
    }
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("to");
    sb.append(space);
    sb.append(":").append(space);
    if (this. getTo() == null) {
      sb.append("null");
    } else {
      sb.append(TBaseHelper.toString(this. getTo(), indent + 1, prettyPrint));
    }
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("raw");
    sb.append(space);
    sb.append(":").append(space);
    if (this. getRaw() == null) {
      sb.append("null");
    } else {
      sb.append(TBaseHelper.toString(this. getRaw(), indent + 1, prettyPrint));
    }
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


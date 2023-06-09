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
public class PutDataRequest implements TBase, java.io.Serializable, Cloneable, Comparable<PutDataRequest> {
  private static final TStruct STRUCT_DESC = new TStruct("PutDataRequest");
  private static final TField DATA_FIELD_DESC = new TField("data", TType.LIST, (short)1);

  public List<DataPoint> data;
  public static final int DATA = 1;
  public static boolean DEFAULT_PRETTY_PRINT = true;

  // isset id assignments

  public static final Map<Integer, FieldMetaData> metaDataMap;
  static {
    Map<Integer, FieldMetaData> tmpMetaDataMap = new HashMap<Integer, FieldMetaData>();
    tmpMetaDataMap.put(DATA, new FieldMetaData("data", TFieldRequirementType.DEFAULT, 
        new ListMetaData(TType.LIST, 
            new StructMetaData(TType.STRUCT, DataPoint.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMetaDataMap);
  }

  static {
    FieldMetaData.addStructMetaDataMap(PutDataRequest.class, metaDataMap);
  }

  public PutDataRequest() {
  }

  public PutDataRequest(
    List<DataPoint> data)
  {
    this();
    this.data = data;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public PutDataRequest(PutDataRequest other) {
    if (other.isSetData()) {
      this.data = TBaseHelper.deepCopy(other.data);
    }
  }

  public PutDataRequest deepCopy() {
    return new PutDataRequest(this);
  }

  @Deprecated
  public PutDataRequest clone() {
    return new PutDataRequest(this);
  }

  public List<DataPoint>  getData() {
    return this.data;
  }

  public PutDataRequest setData(List<DataPoint> data) {
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

  @SuppressWarnings("unchecked")
  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case DATA:
      if (value == null) {
        unsetData();
      } else {
        setData((List<DataPoint>)value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case DATA:
      return getData();

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
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
    if (that instanceof PutDataRequest)
      return this.equals((PutDataRequest)that);
    return false;
  }

  public boolean equals(PutDataRequest that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_data = true && this.isSetData();
    boolean that_present_data = true && that.isSetData();
    if (this_present_data || that_present_data) {
      if (!(this_present_data && that_present_data))
        return false;
      if (!TBaseHelper.equalsNobinary(this.data, that.data))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(PutDataRequest other) {
    if (other == null) {
      // See java.lang.Comparable docs
      throw new NullPointerException();
    }

    if (other == this) {
      return 0;
    }
    int lastComparison = 0;

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
        case DATA:
          if (field.type == TType.LIST) {
            {
              TList _list12 = iprot.readListBegin();
              this.data = new ArrayList<DataPoint>(Math.max(0, _list12.size));
              for (int _i13 = 0; 
                   (_list12.size < 0) ? iprot.peekList() : (_i13 < _list12.size); 
                   ++_i13)
              {
                DataPoint _elem14;
                _elem14 = new DataPoint();
                _elem14.read(iprot);
                this.data.add(_elem14);
              }
              iprot.readListEnd();
            }
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
    if (this.data != null) {
      oprot.writeFieldBegin(DATA_FIELD_DESC);
      {
        oprot.writeListBegin(new TList(TType.STRUCT, this.data.size()));
        for (DataPoint _iter15 : this.data)        {
          _iter15.write(oprot);
        }
        oprot.writeListEnd();
      }
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
    StringBuilder sb = new StringBuilder("PutDataRequest");
    sb.append(space);
    sb.append("(");
    sb.append(newLine);
    boolean first = true;

    sb.append(indentStr);
    sb.append("data");
    sb.append(space);
    sb.append(":").append(space);
    if (this. getData() == null) {
      sb.append("null");
    } else {
      sb.append(TBaseHelper.toString(this. getData(), indent + 1, prettyPrint));
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


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
public class GetDataResult implements TBase, java.io.Serializable, Cloneable, Comparable<GetDataResult> {
  private static final TStruct STRUCT_DESC = new TStruct("GetDataResult");
  private static final TField RESULTS_FIELD_DESC = new TField("results", TType.LIST, (short)1);

  public List<TimeSeriesData> results;
  public static final int RESULTS = 1;
  public static boolean DEFAULT_PRETTY_PRINT = true;

  // isset id assignments

  public static final Map<Integer, FieldMetaData> metaDataMap;
  static {
    Map<Integer, FieldMetaData> tmpMetaDataMap = new HashMap<Integer, FieldMetaData>();
    tmpMetaDataMap.put(RESULTS, new FieldMetaData("results", TFieldRequirementType.DEFAULT, 
        new ListMetaData(TType.LIST, 
            new StructMetaData(TType.STRUCT, TimeSeriesData.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMetaDataMap);
  }

  static {
    FieldMetaData.addStructMetaDataMap(GetDataResult.class, metaDataMap);
  }

  public GetDataResult() {
  }

  public GetDataResult(
    List<TimeSeriesData> results)
  {
    this();
    this.results = results;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public GetDataResult(GetDataResult other) {
    if (other.isSetResults()) {
      this.results = TBaseHelper.deepCopy(other.results);
    }
  }

  public GetDataResult deepCopy() {
    return new GetDataResult(this);
  }

  @Deprecated
  public GetDataResult clone() {
    return new GetDataResult(this);
  }

  public List<TimeSeriesData>  getResults() {
    return this.results;
  }

  public GetDataResult setResults(List<TimeSeriesData> results) {
    this.results = results;
    return this;
  }

  public void unsetResults() {
    this.results = null;
  }

  // Returns true if field results is set (has been assigned a value) and false otherwise
  public boolean isSetResults() {
    return this.results != null;
  }

  public void setResultsIsSet(boolean value) {
    if (!value) {
      this.results = null;
    }
  }

  @SuppressWarnings("unchecked")
  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case RESULTS:
      if (value == null) {
        unsetResults();
      } else {
        setResults((List<TimeSeriesData>)value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case RESULTS:
      return getResults();

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case RESULTS:
      return isSetResults();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof GetDataResult)
      return this.equals((GetDataResult)that);
    return false;
  }

  public boolean equals(GetDataResult that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_results = true && this.isSetResults();
    boolean that_present_results = true && that.isSetResults();
    if (this_present_results || that_present_results) {
      if (!(this_present_results && that_present_results))
        return false;
      if (!TBaseHelper.equalsNobinary(this.results, that.results))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(GetDataResult other) {
    if (other == null) {
      // See java.lang.Comparable docs
      throw new NullPointerException();
    }

    if (other == this) {
      return 0;
    }
    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetResults()).compareTo(other.isSetResults());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(results, other.results);
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
        case RESULTS:
          if (field.type == TType.LIST) {
            {
              TList _list8 = iprot.readListBegin();
              this.results = new ArrayList<TimeSeriesData>(Math.max(0, _list8.size));
              for (int _i9 = 0; 
                   (_list8.size < 0) ? iprot.peekList() : (_i9 < _list8.size); 
                   ++_i9)
              {
                TimeSeriesData _elem10;
                _elem10 = new TimeSeriesData();
                _elem10.read(iprot);
                this.results.add(_elem10);
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
    if (this.results != null) {
      oprot.writeFieldBegin(RESULTS_FIELD_DESC);
      {
        oprot.writeListBegin(new TList(TType.STRUCT, this.results.size()));
        for (TimeSeriesData _iter11 : this.results)        {
          _iter11.write(oprot);
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
    StringBuilder sb = new StringBuilder("GetDataResult");
    sb.append(space);
    sb.append("(");
    sb.append(newLine);
    boolean first = true;

    sb.append(indentStr);
    sb.append("results");
    sb.append(space);
    sb.append(":").append(space);
    if (this. getResults() == null) {
      sb.append("null");
    } else {
      sb.append(TBaseHelper.toString(this. getResults(), indent + 1, prettyPrint));
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


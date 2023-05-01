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
public class UpdateDataPoint implements TBase, java.io.Serializable, Cloneable, Comparable<UpdateDataPoint> {
  private static final TStruct STRUCT_DESC = new TStruct("UpdateDataPoint");
  private static final TField KEY_FIELD_DESC = new TField("key", TType.STRUCT, (short)1);
  private static final TField NUM_OF_UPDATED_POINTS_FIELD_DESC = new TField("numOfUpdatedPoints", TType.I32, (short)2);
  private static final TField STATUS_FIELD_DESC = new TField("status", TType.LIST, (short)3);
  private static final TField POINTS_FIELD_DESC = new TField("points", TType.LIST, (short)4);

  public Key key;
  public int numOfUpdatedPoints;
  public List<Integer> status;
  public List<TimeValueCategoryTuple> points;
  public static final int KEY = 1;
  public static final int NUMOFUPDATEDPOINTS = 2;
  public static final int STATUS = 3;
  public static final int POINTS = 4;
  public static boolean DEFAULT_PRETTY_PRINT = true;

  // isset id assignments
  private static final int __NUMOFUPDATEDPOINTS_ISSET_ID = 0;
  private BitSet __isset_bit_vector = new BitSet(1);

  public static final Map<Integer, FieldMetaData> metaDataMap;
  static {
    Map<Integer, FieldMetaData> tmpMetaDataMap = new HashMap<Integer, FieldMetaData>();
    tmpMetaDataMap.put(KEY, new FieldMetaData("key", TFieldRequirementType.DEFAULT, 
        new StructMetaData(TType.STRUCT, Key.class)));
    tmpMetaDataMap.put(NUMOFUPDATEDPOINTS, new FieldMetaData("numOfUpdatedPoints", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    tmpMetaDataMap.put(STATUS, new FieldMetaData("status", TFieldRequirementType.DEFAULT, 
        new ListMetaData(TType.LIST, 
            new FieldValueMetaData(TType.I32))));
    tmpMetaDataMap.put(POINTS, new FieldMetaData("points", TFieldRequirementType.DEFAULT, 
        new ListMetaData(TType.LIST, 
            new StructMetaData(TType.STRUCT, TimeValueCategoryTuple.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMetaDataMap);
  }

  static {
    FieldMetaData.addStructMetaDataMap(UpdateDataPoint.class, metaDataMap);
  }

  public UpdateDataPoint() {
  }

  public UpdateDataPoint(
    Key key,
    int numOfUpdatedPoints,
    List<Integer> status,
    List<TimeValueCategoryTuple> points)
  {
    this();
    this.key = key;
    this.numOfUpdatedPoints = numOfUpdatedPoints;
    setNumOfUpdatedPointsIsSet(true);
    this.status = status;
    this.points = points;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public UpdateDataPoint(UpdateDataPoint other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetKey()) {
      this.key = TBaseHelper.deepCopy(other.key);
    }
    this.numOfUpdatedPoints = TBaseHelper.deepCopy(other.numOfUpdatedPoints);
    if (other.isSetStatus()) {
      this.status = TBaseHelper.deepCopy(other.status);
    }
    if (other.isSetPoints()) {
      this.points = TBaseHelper.deepCopy(other.points);
    }
  }

  public UpdateDataPoint deepCopy() {
    return new UpdateDataPoint(this);
  }

  @Deprecated
  public UpdateDataPoint clone() {
    return new UpdateDataPoint(this);
  }

  public Key  getKey() {
    return this.key;
  }

  public UpdateDataPoint setKey(Key key) {
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

  public int  getNumOfUpdatedPoints() {
    return this.numOfUpdatedPoints;
  }

  public UpdateDataPoint setNumOfUpdatedPoints(int numOfUpdatedPoints) {
    this.numOfUpdatedPoints = numOfUpdatedPoints;
    setNumOfUpdatedPointsIsSet(true);
    return this;
  }

  public void unsetNumOfUpdatedPoints() {
    __isset_bit_vector.clear(__NUMOFUPDATEDPOINTS_ISSET_ID);
  }

  // Returns true if field numOfUpdatedPoints is set (has been assigned a value) and false otherwise
  public boolean isSetNumOfUpdatedPoints() {
    return __isset_bit_vector.get(__NUMOFUPDATEDPOINTS_ISSET_ID);
  }

  public void setNumOfUpdatedPointsIsSet(boolean value) {
    __isset_bit_vector.set(__NUMOFUPDATEDPOINTS_ISSET_ID, value);
  }

  public List<Integer>  getStatus() {
    return this.status;
  }

  public UpdateDataPoint setStatus(List<Integer> status) {
    this.status = status;
    return this;
  }

  public void unsetStatus() {
    this.status = null;
  }

  // Returns true if field status is set (has been assigned a value) and false otherwise
  public boolean isSetStatus() {
    return this.status != null;
  }

  public void setStatusIsSet(boolean value) {
    if (!value) {
      this.status = null;
    }
  }

  public List<TimeValueCategoryTuple>  getPoints() {
    return this.points;
  }

  public UpdateDataPoint setPoints(List<TimeValueCategoryTuple> points) {
    this.points = points;
    return this;
  }

  public void unsetPoints() {
    this.points = null;
  }

  // Returns true if field points is set (has been assigned a value) and false otherwise
  public boolean isSetPoints() {
    return this.points != null;
  }

  public void setPointsIsSet(boolean value) {
    if (!value) {
      this.points = null;
    }
  }

  @SuppressWarnings("unchecked")
  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case KEY:
      if (value == null) {
        unsetKey();
      } else {
        setKey((Key)value);
      }
      break;

    case NUMOFUPDATEDPOINTS:
      if (value == null) {
        unsetNumOfUpdatedPoints();
      } else {
        setNumOfUpdatedPoints((Integer)value);
      }
      break;

    case STATUS:
      if (value == null) {
        unsetStatus();
      } else {
        setStatus((List<Integer>)value);
      }
      break;

    case POINTS:
      if (value == null) {
        unsetPoints();
      } else {
        setPoints((List<TimeValueCategoryTuple>)value);
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

    case NUMOFUPDATEDPOINTS:
      return new Integer(getNumOfUpdatedPoints());

    case STATUS:
      return getStatus();

    case POINTS:
      return getPoints();

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case KEY:
      return isSetKey();
    case NUMOFUPDATEDPOINTS:
      return isSetNumOfUpdatedPoints();
    case STATUS:
      return isSetStatus();
    case POINTS:
      return isSetPoints();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof UpdateDataPoint)
      return this.equals((UpdateDataPoint)that);
    return false;
  }

  public boolean equals(UpdateDataPoint that) {
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

    boolean this_present_numOfUpdatedPoints = true;
    boolean that_present_numOfUpdatedPoints = true;
    if (this_present_numOfUpdatedPoints || that_present_numOfUpdatedPoints) {
      if (!(this_present_numOfUpdatedPoints && that_present_numOfUpdatedPoints))
        return false;
      if (!TBaseHelper.equalsNobinary(this.numOfUpdatedPoints, that.numOfUpdatedPoints))
        return false;
    }

    boolean this_present_status = true && this.isSetStatus();
    boolean that_present_status = true && that.isSetStatus();
    if (this_present_status || that_present_status) {
      if (!(this_present_status && that_present_status))
        return false;
      if (!TBaseHelper.equalsNobinary(this.status, that.status))
        return false;
    }

    boolean this_present_points = true && this.isSetPoints();
    boolean that_present_points = true && that.isSetPoints();
    if (this_present_points || that_present_points) {
      if (!(this_present_points && that_present_points))
        return false;
      if (!TBaseHelper.equalsNobinary(this.points, that.points))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(UpdateDataPoint other) {
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
    lastComparison = Boolean.valueOf(isSetNumOfUpdatedPoints()).compareTo(other.isSetNumOfUpdatedPoints());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(numOfUpdatedPoints, other.numOfUpdatedPoints);
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
    lastComparison = Boolean.valueOf(isSetPoints()).compareTo(other.isSetPoints());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(points, other.points);
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
        case NUMOFUPDATEDPOINTS:
          if (field.type == TType.I32) {
            this.numOfUpdatedPoints = iprot.readI32();
            setNumOfUpdatedPointsIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case STATUS:
          if (field.type == TType.LIST) {
            {
              TList _list52 = iprot.readListBegin();
              this.status = new ArrayList<Integer>(Math.max(0, _list52.size));
              for (int _i53 = 0; 
                   (_list52.size < 0) ? iprot.peekList() : (_i53 < _list52.size); 
                   ++_i53)
              {
                int _elem54;
                _elem54 = iprot.readI32();
                this.status.add(_elem54);
              }
              iprot.readListEnd();
            }
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case POINTS:
          if (field.type == TType.LIST) {
            {
              TList _list55 = iprot.readListBegin();
              this.points = new ArrayList<TimeValueCategoryTuple>(Math.max(0, _list55.size));
              for (int _i56 = 0; 
                   (_list55.size < 0) ? iprot.peekList() : (_i56 < _list55.size); 
                   ++_i56)
              {
                TimeValueCategoryTuple _elem57;
                _elem57 = new TimeValueCategoryTuple();
                _elem57.read(iprot);
                this.points.add(_elem57);
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
    if (this.key != null) {
      oprot.writeFieldBegin(KEY_FIELD_DESC);
      this.key.write(oprot);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(NUM_OF_UPDATED_POINTS_FIELD_DESC);
    oprot.writeI32(this.numOfUpdatedPoints);
    oprot.writeFieldEnd();
    if (this.status != null) {
      oprot.writeFieldBegin(STATUS_FIELD_DESC);
      {
        oprot.writeListBegin(new TList(TType.I32, this.status.size()));
        for (int _iter58 : this.status)        {
          oprot.writeI32(_iter58);
        }
        oprot.writeListEnd();
      }
      oprot.writeFieldEnd();
    }
    if (this.points != null) {
      oprot.writeFieldBegin(POINTS_FIELD_DESC);
      {
        oprot.writeListBegin(new TList(TType.STRUCT, this.points.size()));
        for (TimeValueCategoryTuple _iter59 : this.points)        {
          _iter59.write(oprot);
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
    StringBuilder sb = new StringBuilder("UpdateDataPoint");
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
    sb.append("numOfUpdatedPoints");
    sb.append(space);
    sb.append(":").append(space);
    sb.append(TBaseHelper.toString(this. getNumOfUpdatedPoints(), indent + 1, prettyPrint));
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("status");
    sb.append(space);
    sb.append(":").append(space);
    if (this. getStatus() == null) {
      sb.append("null");
    } else {
      sb.append(TBaseHelper.toString(this. getStatus(), indent + 1, prettyPrint));
    }
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("points");
    sb.append(space);
    sb.append(":").append(space);
    if (this. getPoints() == null) {
      sb.append("null");
    } else {
      sb.append(TBaseHelper.toString(this. getPoints(), indent + 1, prettyPrint));
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


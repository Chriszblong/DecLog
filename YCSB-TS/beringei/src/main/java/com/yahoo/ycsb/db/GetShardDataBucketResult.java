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
public class GetShardDataBucketResult implements TBase, java.io.Serializable, Cloneable, Comparable<GetShardDataBucketResult> {
  private static final TStruct STRUCT_DESC = new TStruct("GetShardDataBucketResult");
  private static final TField STATUS_FIELD_DESC = new TField("status", TType.I32, (short)1);
  private static final TField KEYS_FIELD_DESC = new TField("keys", TType.LIST, (short)2);
  private static final TField DATA_FIELD_DESC = new TField("data", TType.LIST, (short)3);
  private static final TField RECENT_READ_FIELD_DESC = new TField("recentRead", TType.LIST, (short)4);
  private static final TField MORE_ENTRIES_FIELD_DESC = new TField("moreEntries", TType.BOOL, (short)5);

  /**
   * 
   * @see StatusCode
   */
  public int status;
  public List<String> keys;
  public List<List<TimeSeriesBlock>> data;
  public List<Boolean> recentRead;
  public boolean moreEntries;
  public static final int STATUS = 1;
  public static final int KEYS = 2;
  public static final int DATA = 3;
  public static final int RECENTREAD = 4;
  public static final int MOREENTRIES = 5;
  public static boolean DEFAULT_PRETTY_PRINT = true;

  // isset id assignments
  private static final int __STATUS_ISSET_ID = 0;
  private static final int __MOREENTRIES_ISSET_ID = 1;
  private BitSet __isset_bit_vector = new BitSet(2);

  public static final Map<Integer, FieldMetaData> metaDataMap;
  static {
    Map<Integer, FieldMetaData> tmpMetaDataMap = new HashMap<Integer, FieldMetaData>();
    tmpMetaDataMap.put(STATUS, new FieldMetaData("status", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.I32)));
    tmpMetaDataMap.put(KEYS, new FieldMetaData("keys", TFieldRequirementType.DEFAULT, 
        new ListMetaData(TType.LIST, 
            new FieldValueMetaData(TType.STRING))));
    tmpMetaDataMap.put(DATA, new FieldMetaData("data", TFieldRequirementType.DEFAULT, 
        new ListMetaData(TType.LIST, 
            new ListMetaData(TType.LIST, 
                new StructMetaData(TType.STRUCT, TimeSeriesBlock.class)))));
    tmpMetaDataMap.put(RECENTREAD, new FieldMetaData("recentRead", TFieldRequirementType.DEFAULT, 
        new ListMetaData(TType.LIST, 
            new FieldValueMetaData(TType.BOOL))));
    tmpMetaDataMap.put(MOREENTRIES, new FieldMetaData("moreEntries", TFieldRequirementType.DEFAULT, 
        new FieldValueMetaData(TType.BOOL)));
    metaDataMap = Collections.unmodifiableMap(tmpMetaDataMap);
  }

  static {
    FieldMetaData.addStructMetaDataMap(GetShardDataBucketResult.class, metaDataMap);
  }

  public GetShardDataBucketResult() {
  }

  public GetShardDataBucketResult(
    int status,
    List<String> keys,
    List<List<TimeSeriesBlock>> data,
    List<Boolean> recentRead,
    boolean moreEntries)
  {
    this();
    this.status = status;
    setStatusIsSet(true);
    this.keys = keys;
    this.data = data;
    this.recentRead = recentRead;
    this.moreEntries = moreEntries;
    setMoreEntriesIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public GetShardDataBucketResult(GetShardDataBucketResult other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    this.status = TBaseHelper.deepCopy(other.status);
    if (other.isSetKeys()) {
      this.keys = TBaseHelper.deepCopy(other.keys);
    }
    if (other.isSetData()) {
      this.data = TBaseHelper.deepCopy(other.data);
    }
    if (other.isSetRecentRead()) {
      this.recentRead = TBaseHelper.deepCopy(other.recentRead);
    }
    this.moreEntries = TBaseHelper.deepCopy(other.moreEntries);
  }

  public GetShardDataBucketResult deepCopy() {
    return new GetShardDataBucketResult(this);
  }

  @Deprecated
  public GetShardDataBucketResult clone() {
    return new GetShardDataBucketResult(this);
  }

  /**
   * 
   * @see StatusCode
   */
  public int  getStatus() {
    return this.status;
  }

  /**
   * 
   * @see StatusCode
   */
  public GetShardDataBucketResult setStatus(int status) {
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

  public List<String>  getKeys() {
    return this.keys;
  }

  public GetShardDataBucketResult setKeys(List<String> keys) {
    this.keys = keys;
    return this;
  }

  public void unsetKeys() {
    this.keys = null;
  }

  // Returns true if field keys is set (has been assigned a value) and false otherwise
  public boolean isSetKeys() {
    return this.keys != null;
  }

  public void setKeysIsSet(boolean value) {
    if (!value) {
      this.keys = null;
    }
  }

  public List<List<TimeSeriesBlock>>  getData() {
    return this.data;
  }

  public GetShardDataBucketResult setData(List<List<TimeSeriesBlock>> data) {
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

  public List<Boolean>  getRecentRead() {
    return this.recentRead;
  }

  public GetShardDataBucketResult setRecentRead(List<Boolean> recentRead) {
    this.recentRead = recentRead;
    return this;
  }

  public void unsetRecentRead() {
    this.recentRead = null;
  }

  // Returns true if field recentRead is set (has been assigned a value) and false otherwise
  public boolean isSetRecentRead() {
    return this.recentRead != null;
  }

  public void setRecentReadIsSet(boolean value) {
    if (!value) {
      this.recentRead = null;
    }
  }

  public boolean  isMoreEntries() {
    return this.moreEntries;
  }

  public GetShardDataBucketResult setMoreEntries(boolean moreEntries) {
    this.moreEntries = moreEntries;
    setMoreEntriesIsSet(true);
    return this;
  }

  public void unsetMoreEntries() {
    __isset_bit_vector.clear(__MOREENTRIES_ISSET_ID);
  }

  // Returns true if field moreEntries is set (has been assigned a value) and false otherwise
  public boolean isSetMoreEntries() {
    return __isset_bit_vector.get(__MOREENTRIES_ISSET_ID);
  }

  public void setMoreEntriesIsSet(boolean value) {
    __isset_bit_vector.set(__MOREENTRIES_ISSET_ID, value);
  }

  @SuppressWarnings("unchecked")
  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case STATUS:
      if (value == null) {
        unsetStatus();
      } else {
        setStatus((Integer)value);
      }
      break;

    case KEYS:
      if (value == null) {
        unsetKeys();
      } else {
        setKeys((List<String>)value);
      }
      break;

    case DATA:
      if (value == null) {
        unsetData();
      } else {
        setData((List<List<TimeSeriesBlock>>)value);
      }
      break;

    case RECENTREAD:
      if (value == null) {
        unsetRecentRead();
      } else {
        setRecentRead((List<Boolean>)value);
      }
      break;

    case MOREENTRIES:
      if (value == null) {
        unsetMoreEntries();
      } else {
        setMoreEntries((Boolean)value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case STATUS:
      return getStatus();

    case KEYS:
      return getKeys();

    case DATA:
      return getData();

    case RECENTREAD:
      return getRecentRead();

    case MOREENTRIES:
      return new Boolean(isMoreEntries());

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  // Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise
  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case STATUS:
      return isSetStatus();
    case KEYS:
      return isSetKeys();
    case DATA:
      return isSetData();
    case RECENTREAD:
      return isSetRecentRead();
    case MOREENTRIES:
      return isSetMoreEntries();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof GetShardDataBucketResult)
      return this.equals((GetShardDataBucketResult)that);
    return false;
  }

  public boolean equals(GetShardDataBucketResult that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_status = true;
    boolean that_present_status = true;
    if (this_present_status || that_present_status) {
      if (!(this_present_status && that_present_status))
        return false;
      if (!TBaseHelper.equalsNobinary(this.status, that.status))
        return false;
    }

    boolean this_present_keys = true && this.isSetKeys();
    boolean that_present_keys = true && that.isSetKeys();
    if (this_present_keys || that_present_keys) {
      if (!(this_present_keys && that_present_keys))
        return false;
      if (!TBaseHelper.equalsNobinary(this.keys, that.keys))
        return false;
    }

    boolean this_present_data = true && this.isSetData();
    boolean that_present_data = true && that.isSetData();
    if (this_present_data || that_present_data) {
      if (!(this_present_data && that_present_data))
        return false;
      if (!TBaseHelper.equalsNobinary(this.data, that.data))
        return false;
    }

    boolean this_present_recentRead = true && this.isSetRecentRead();
    boolean that_present_recentRead = true && that.isSetRecentRead();
    if (this_present_recentRead || that_present_recentRead) {
      if (!(this_present_recentRead && that_present_recentRead))
        return false;
      if (!TBaseHelper.equalsNobinary(this.recentRead, that.recentRead))
        return false;
    }

    boolean this_present_moreEntries = true;
    boolean that_present_moreEntries = true;
    if (this_present_moreEntries || that_present_moreEntries) {
      if (!(this_present_moreEntries && that_present_moreEntries))
        return false;
      if (!TBaseHelper.equalsNobinary(this.moreEntries, that.moreEntries))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(GetShardDataBucketResult other) {
    if (other == null) {
      // See java.lang.Comparable docs
      throw new NullPointerException();
    }

    if (other == this) {
      return 0;
    }
    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetStatus()).compareTo(other.isSetStatus());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(status, other.status);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetKeys()).compareTo(other.isSetKeys());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(keys, other.keys);
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
    lastComparison = Boolean.valueOf(isSetRecentRead()).compareTo(other.isSetRecentRead());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(recentRead, other.recentRead);
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = Boolean.valueOf(isSetMoreEntries()).compareTo(other.isSetMoreEntries());
    if (lastComparison != 0) {
      return lastComparison;
    }
    lastComparison = TBaseHelper.compareTo(moreEntries, other.moreEntries);
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
        case STATUS:
          if (field.type == TType.I32) {
            this.status = iprot.readI32();
            setStatusIsSet(true);
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case KEYS:
          if (field.type == TType.LIST) {
            {
              TList _list20 = iprot.readListBegin();
              this.keys = new ArrayList<String>(Math.max(0, _list20.size));
              for (int _i21 = 0; 
                   (_list20.size < 0) ? iprot.peekList() : (_i21 < _list20.size); 
                   ++_i21)
              {
                String _elem22;
                _elem22 = iprot.readString();
                this.keys.add(_elem22);
              }
              iprot.readListEnd();
            }
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case DATA:
          if (field.type == TType.LIST) {
            {
              TList _list23 = iprot.readListBegin();
              this.data = new ArrayList<List<TimeSeriesBlock>>(Math.max(0, _list23.size));
              for (int _i24 = 0; 
                   (_list23.size < 0) ? iprot.peekList() : (_i24 < _list23.size); 
                   ++_i24)
              {
                List<TimeSeriesBlock> _elem25;
                {
                  TList _list26 = iprot.readListBegin();
                  _elem25 = new ArrayList<TimeSeriesBlock>(Math.max(0, _list26.size));
                  for (int _i27 = 0; 
                       (_list26.size < 0) ? iprot.peekList() : (_i27 < _list26.size); 
                       ++_i27)
                  {
                    TimeSeriesBlock _elem28;
                    _elem28 = new TimeSeriesBlock();
                    _elem28.read(iprot);
                    _elem25.add(_elem28);
                  }
                  iprot.readListEnd();
                }
                this.data.add(_elem25);
              }
              iprot.readListEnd();
            }
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case RECENTREAD:
          if (field.type == TType.LIST) {
            {
              TList _list29 = iprot.readListBegin();
              this.recentRead = new ArrayList<Boolean>(Math.max(0, _list29.size));
              for (int _i30 = 0; 
                   (_list29.size < 0) ? iprot.peekList() : (_i30 < _list29.size); 
                   ++_i30)
              {
                boolean _elem31;
                _elem31 = iprot.readBool();
                this.recentRead.add(_elem31);
              }
              iprot.readListEnd();
            }
          } else { 
            TProtocolUtil.skip(iprot, field.type);
          }
          break;
        case MOREENTRIES:
          if (field.type == TType.BOOL) {
            this.moreEntries = iprot.readBool();
            setMoreEntriesIsSet(true);
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
    oprot.writeFieldBegin(STATUS_FIELD_DESC);
    oprot.writeI32(this.status);
    oprot.writeFieldEnd();
    if (this.keys != null) {
      oprot.writeFieldBegin(KEYS_FIELD_DESC);
      {
        oprot.writeListBegin(new TList(TType.STRING, this.keys.size()));
        for (String _iter32 : this.keys)        {
          oprot.writeString(_iter32);
        }
        oprot.writeListEnd();
      }
      oprot.writeFieldEnd();
    }
    if (this.data != null) {
      oprot.writeFieldBegin(DATA_FIELD_DESC);
      {
        oprot.writeListBegin(new TList(TType.LIST, this.data.size()));
        for (List<TimeSeriesBlock> _iter33 : this.data)        {
          {
            oprot.writeListBegin(new TList(TType.STRUCT, _iter33.size()));
            for (TimeSeriesBlock _iter34 : _iter33)            {
              _iter34.write(oprot);
            }
            oprot.writeListEnd();
          }
        }
        oprot.writeListEnd();
      }
      oprot.writeFieldEnd();
    }
    if (this.recentRead != null) {
      oprot.writeFieldBegin(RECENT_READ_FIELD_DESC);
      {
        oprot.writeListBegin(new TList(TType.BOOL, this.recentRead.size()));
        for (boolean _iter35 : this.recentRead)        {
          oprot.writeBool(_iter35);
        }
        oprot.writeListEnd();
      }
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(MORE_ENTRIES_FIELD_DESC);
    oprot.writeBool(this.moreEntries);
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
    StringBuilder sb = new StringBuilder("GetShardDataBucketResult");
    sb.append(space);
    sb.append("(");
    sb.append(newLine);
    boolean first = true;

    sb.append(indentStr);
    sb.append("status");
    sb.append(space);
    sb.append(":").append(space);
    String status_name = StatusCode.VALUES_TO_NAMES.get(this. getStatus());
    if (status_name != null) {
      sb.append(status_name);
      sb.append(" (");
    }
    sb.append(this. getStatus());
    if (status_name != null) {
      sb.append(")");
    }
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("keys");
    sb.append(space);
    sb.append(":").append(space);
    if (this. getKeys() == null) {
      sb.append("null");
    } else {
      sb.append(TBaseHelper.toString(this. getKeys(), indent + 1, prettyPrint));
    }
    first = false;
    if (!first) sb.append("," + newLine);
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
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("recentRead");
    sb.append(space);
    sb.append(":").append(space);
    if (this. getRecentRead() == null) {
      sb.append("null");
    } else {
      sb.append(TBaseHelper.toString(this. getRecentRead(), indent + 1, prettyPrint));
    }
    first = false;
    if (!first) sb.append("," + newLine);
    sb.append(indentStr);
    sb.append("moreEntries");
    sb.append(space);
    sb.append(":").append(space);
    sb.append(TBaseHelper.toString(this. isMoreEntries(), indent + 1, prettyPrint));
    first = false;
    sb.append(newLine + TBaseHelper.reduceIndent(indentStr));
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
    // check for required fields
    // check that fields of type enum have valid values
    if (isSetStatus() && !StatusCode.VALID_VALUES.contains(status)){
      throw new TProtocolException("The field 'status' has been assigned the invalid value " + status);
    }
  }

}


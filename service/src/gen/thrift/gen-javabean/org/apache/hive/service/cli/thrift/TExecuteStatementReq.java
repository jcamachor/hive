/**
 * Autogenerated by Thrift Compiler (0.9.2)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hive.service.cli.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import javax.annotation.Generated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked"})
@Generated(value = "Autogenerated by Thrift Compiler (0.9.2)", date = "2015-8-3")
public class TExecuteStatementReq implements org.apache.thrift.TBase<TExecuteStatementReq, TExecuteStatementReq._Fields>, java.io.Serializable, Cloneable, Comparable<TExecuteStatementReq> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TExecuteStatementReq");

  private static final org.apache.thrift.protocol.TField SESSION_HANDLE_FIELD_DESC = new org.apache.thrift.protocol.TField("sessionHandle", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField STATEMENT_FIELD_DESC = new org.apache.thrift.protocol.TField("statement", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField CONF_OVERLAY_FIELD_DESC = new org.apache.thrift.protocol.TField("confOverlay", org.apache.thrift.protocol.TType.MAP, (short)3);
  private static final org.apache.thrift.protocol.TField RUN_ASYNC_FIELD_DESC = new org.apache.thrift.protocol.TField("runAsync", org.apache.thrift.protocol.TType.BOOL, (short)4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TExecuteStatementReqStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TExecuteStatementReqTupleSchemeFactory());
  }

  private TSessionHandle sessionHandle; // required
  private String statement; // required
  private Map<String,String> confOverlay; // optional
  private boolean runAsync; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    SESSION_HANDLE((short)1, "sessionHandle"),
    STATEMENT((short)2, "statement"),
    CONF_OVERLAY((short)3, "confOverlay"),
    RUN_ASYNC((short)4, "runAsync");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // SESSION_HANDLE
          return SESSION_HANDLE;
        case 2: // STATEMENT
          return STATEMENT;
        case 3: // CONF_OVERLAY
          return CONF_OVERLAY;
        case 4: // RUN_ASYNC
          return RUN_ASYNC;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __RUNASYNC_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private static final _Fields optionals[] = {_Fields.CONF_OVERLAY,_Fields.RUN_ASYNC};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.SESSION_HANDLE, new org.apache.thrift.meta_data.FieldMetaData("sessionHandle", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, TSessionHandle.class)));
    tmpMap.put(_Fields.STATEMENT, new org.apache.thrift.meta_data.FieldMetaData("statement", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CONF_OVERLAY, new org.apache.thrift.meta_data.FieldMetaData("confOverlay", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING), 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.RUN_ASYNC, new org.apache.thrift.meta_data.FieldMetaData("runAsync", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TExecuteStatementReq.class, metaDataMap);
  }

  public TExecuteStatementReq() {
    this.runAsync = false;

  }

  public TExecuteStatementReq(
    TSessionHandle sessionHandle,
    String statement)
  {
    this();
    this.sessionHandle = sessionHandle;
    this.statement = statement;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TExecuteStatementReq(TExecuteStatementReq other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetSessionHandle()) {
      this.sessionHandle = new TSessionHandle(other.sessionHandle);
    }
    if (other.isSetStatement()) {
      this.statement = other.statement;
    }
    if (other.isSetConfOverlay()) {
      Map<String,String> __this__confOverlay = new HashMap<String,String>(other.confOverlay);
      this.confOverlay = __this__confOverlay;
    }
    this.runAsync = other.runAsync;
  }

  public TExecuteStatementReq deepCopy() {
    return new TExecuteStatementReq(this);
  }

  @Override
  public void clear() {
    this.sessionHandle = null;
    this.statement = null;
    this.confOverlay = null;
    this.runAsync = false;

  }

  public TSessionHandle getSessionHandle() {
    return this.sessionHandle;
  }

  public void setSessionHandle(TSessionHandle sessionHandle) {
    this.sessionHandle = sessionHandle;
  }

  public void unsetSessionHandle() {
    this.sessionHandle = null;
  }

  /** Returns true if field sessionHandle is set (has been assigned a value) and false otherwise */
  public boolean isSetSessionHandle() {
    return this.sessionHandle != null;
  }

  public void setSessionHandleIsSet(boolean value) {
    if (!value) {
      this.sessionHandle = null;
    }
  }

  public String getStatement() {
    return this.statement;
  }

  public void setStatement(String statement) {
    this.statement = statement;
  }

  public void unsetStatement() {
    this.statement = null;
  }

  /** Returns true if field statement is set (has been assigned a value) and false otherwise */
  public boolean isSetStatement() {
    return this.statement != null;
  }

  public void setStatementIsSet(boolean value) {
    if (!value) {
      this.statement = null;
    }
  }

  public int getConfOverlaySize() {
    return (this.confOverlay == null) ? 0 : this.confOverlay.size();
  }

  public void putToConfOverlay(String key, String val) {
    if (this.confOverlay == null) {
      this.confOverlay = new HashMap<String,String>();
    }
    this.confOverlay.put(key, val);
  }

  public Map<String,String> getConfOverlay() {
    return this.confOverlay;
  }

  public void setConfOverlay(Map<String,String> confOverlay) {
    this.confOverlay = confOverlay;
  }

  public void unsetConfOverlay() {
    this.confOverlay = null;
  }

  /** Returns true if field confOverlay is set (has been assigned a value) and false otherwise */
  public boolean isSetConfOverlay() {
    return this.confOverlay != null;
  }

  public void setConfOverlayIsSet(boolean value) {
    if (!value) {
      this.confOverlay = null;
    }
  }

  public boolean isRunAsync() {
    return this.runAsync;
  }

  public void setRunAsync(boolean runAsync) {
    this.runAsync = runAsync;
    setRunAsyncIsSet(true);
  }

  public void unsetRunAsync() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __RUNASYNC_ISSET_ID);
  }

  /** Returns true if field runAsync is set (has been assigned a value) and false otherwise */
  public boolean isSetRunAsync() {
    return EncodingUtils.testBit(__isset_bitfield, __RUNASYNC_ISSET_ID);
  }

  public void setRunAsyncIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __RUNASYNC_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case SESSION_HANDLE:
      if (value == null) {
        unsetSessionHandle();
      } else {
        setSessionHandle((TSessionHandle)value);
      }
      break;

    case STATEMENT:
      if (value == null) {
        unsetStatement();
      } else {
        setStatement((String)value);
      }
      break;

    case CONF_OVERLAY:
      if (value == null) {
        unsetConfOverlay();
      } else {
        setConfOverlay((Map<String,String>)value);
      }
      break;

    case RUN_ASYNC:
      if (value == null) {
        unsetRunAsync();
      } else {
        setRunAsync((Boolean)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case SESSION_HANDLE:
      return getSessionHandle();

    case STATEMENT:
      return getStatement();

    case CONF_OVERLAY:
      return getConfOverlay();

    case RUN_ASYNC:
      return Boolean.valueOf(isRunAsync());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case SESSION_HANDLE:
      return isSetSessionHandle();
    case STATEMENT:
      return isSetStatement();
    case CONF_OVERLAY:
      return isSetConfOverlay();
    case RUN_ASYNC:
      return isSetRunAsync();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TExecuteStatementReq)
      return this.equals((TExecuteStatementReq)that);
    return false;
  }

  public boolean equals(TExecuteStatementReq that) {
    if (that == null)
      return false;

    boolean this_present_sessionHandle = true && this.isSetSessionHandle();
    boolean that_present_sessionHandle = true && that.isSetSessionHandle();
    if (this_present_sessionHandle || that_present_sessionHandle) {
      if (!(this_present_sessionHandle && that_present_sessionHandle))
        return false;
      if (!this.sessionHandle.equals(that.sessionHandle))
        return false;
    }

    boolean this_present_statement = true && this.isSetStatement();
    boolean that_present_statement = true && that.isSetStatement();
    if (this_present_statement || that_present_statement) {
      if (!(this_present_statement && that_present_statement))
        return false;
      if (!this.statement.equals(that.statement))
        return false;
    }

    boolean this_present_confOverlay = true && this.isSetConfOverlay();
    boolean that_present_confOverlay = true && that.isSetConfOverlay();
    if (this_present_confOverlay || that_present_confOverlay) {
      if (!(this_present_confOverlay && that_present_confOverlay))
        return false;
      if (!this.confOverlay.equals(that.confOverlay))
        return false;
    }

    boolean this_present_runAsync = true && this.isSetRunAsync();
    boolean that_present_runAsync = true && that.isSetRunAsync();
    if (this_present_runAsync || that_present_runAsync) {
      if (!(this_present_runAsync && that_present_runAsync))
        return false;
      if (this.runAsync != that.runAsync)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    List<Object> list = new ArrayList<Object>();

    boolean present_sessionHandle = true && (isSetSessionHandle());
    list.add(present_sessionHandle);
    if (present_sessionHandle)
      list.add(sessionHandle);

    boolean present_statement = true && (isSetStatement());
    list.add(present_statement);
    if (present_statement)
      list.add(statement);

    boolean present_confOverlay = true && (isSetConfOverlay());
    list.add(present_confOverlay);
    if (present_confOverlay)
      list.add(confOverlay);

    boolean present_runAsync = true && (isSetRunAsync());
    list.add(present_runAsync);
    if (present_runAsync)
      list.add(runAsync);

    return list.hashCode();
  }

  @Override
  public int compareTo(TExecuteStatementReq other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetSessionHandle()).compareTo(other.isSetSessionHandle());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSessionHandle()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.sessionHandle, other.sessionHandle);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStatement()).compareTo(other.isSetStatement());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStatement()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.statement, other.statement);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetConfOverlay()).compareTo(other.isSetConfOverlay());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetConfOverlay()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.confOverlay, other.confOverlay);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetRunAsync()).compareTo(other.isSetRunAsync());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRunAsync()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.runAsync, other.runAsync);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TExecuteStatementReq(");
    boolean first = true;

    sb.append("sessionHandle:");
    if (this.sessionHandle == null) {
      sb.append("null");
    } else {
      sb.append(this.sessionHandle);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("statement:");
    if (this.statement == null) {
      sb.append("null");
    } else {
      sb.append(this.statement);
    }
    first = false;
    if (isSetConfOverlay()) {
      if (!first) sb.append(", ");
      sb.append("confOverlay:");
      if (this.confOverlay == null) {
        sb.append("null");
      } else {
        sb.append(this.confOverlay);
      }
      first = false;
    }
    if (isSetRunAsync()) {
      if (!first) sb.append(", ");
      sb.append("runAsync:");
      sb.append(this.runAsync);
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetSessionHandle()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'sessionHandle' is unset! Struct:" + toString());
    }

    if (!isSetStatement()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'statement' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
    if (sessionHandle != null) {
      sessionHandle.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TExecuteStatementReqStandardSchemeFactory implements SchemeFactory {
    public TExecuteStatementReqStandardScheme getScheme() {
      return new TExecuteStatementReqStandardScheme();
    }
  }

  private static class TExecuteStatementReqStandardScheme extends StandardScheme<TExecuteStatementReq> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TExecuteStatementReq struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // SESSION_HANDLE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.sessionHandle = new TSessionHandle();
              struct.sessionHandle.read(iprot);
              struct.setSessionHandleIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // STATEMENT
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.statement = iprot.readString();
              struct.setStatementIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // CONF_OVERLAY
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map162 = iprot.readMapBegin();
                struct.confOverlay = new HashMap<String,String>(2*_map162.size);
                String _key163;
                String _val164;
                for (int _i165 = 0; _i165 < _map162.size; ++_i165)
                {
                  _key163 = iprot.readString();
                  _val164 = iprot.readString();
                  struct.confOverlay.put(_key163, _val164);
                }
                iprot.readMapEnd();
              }
              struct.setConfOverlayIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // RUN_ASYNC
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.runAsync = iprot.readBool();
              struct.setRunAsyncIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TExecuteStatementReq struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.sessionHandle != null) {
        oprot.writeFieldBegin(SESSION_HANDLE_FIELD_DESC);
        struct.sessionHandle.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.statement != null) {
        oprot.writeFieldBegin(STATEMENT_FIELD_DESC);
        oprot.writeString(struct.statement);
        oprot.writeFieldEnd();
      }
      if (struct.confOverlay != null) {
        if (struct.isSetConfOverlay()) {
          oprot.writeFieldBegin(CONF_OVERLAY_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, struct.confOverlay.size()));
            for (Map.Entry<String, String> _iter166 : struct.confOverlay.entrySet())
            {
              oprot.writeString(_iter166.getKey());
              oprot.writeString(_iter166.getValue());
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.isSetRunAsync()) {
        oprot.writeFieldBegin(RUN_ASYNC_FIELD_DESC);
        oprot.writeBool(struct.runAsync);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TExecuteStatementReqTupleSchemeFactory implements SchemeFactory {
    public TExecuteStatementReqTupleScheme getScheme() {
      return new TExecuteStatementReqTupleScheme();
    }
  }

  private static class TExecuteStatementReqTupleScheme extends TupleScheme<TExecuteStatementReq> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TExecuteStatementReq struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      struct.sessionHandle.write(oprot);
      oprot.writeString(struct.statement);
      BitSet optionals = new BitSet();
      if (struct.isSetConfOverlay()) {
        optionals.set(0);
      }
      if (struct.isSetRunAsync()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetConfOverlay()) {
        {
          oprot.writeI32(struct.confOverlay.size());
          for (Map.Entry<String, String> _iter167 : struct.confOverlay.entrySet())
          {
            oprot.writeString(_iter167.getKey());
            oprot.writeString(_iter167.getValue());
          }
        }
      }
      if (struct.isSetRunAsync()) {
        oprot.writeBool(struct.runAsync);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TExecuteStatementReq struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.sessionHandle = new TSessionHandle();
      struct.sessionHandle.read(iprot);
      struct.setSessionHandleIsSet(true);
      struct.statement = iprot.readString();
      struct.setStatementIsSet(true);
      BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TMap _map168 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.STRING, org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.confOverlay = new HashMap<String,String>(2*_map168.size);
          String _key169;
          String _val170;
          for (int _i171 = 0; _i171 < _map168.size; ++_i171)
          {
            _key169 = iprot.readString();
            _val170 = iprot.readString();
            struct.confOverlay.put(_key169, _val170);
          }
        }
        struct.setConfOverlayIsSet(true);
      }
      if (incoming.get(1)) {
        struct.runAsync = iprot.readBool();
        struct.setRunAsyncIsSet(true);
      }
    }
  }

}

